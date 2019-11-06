/* Copyright (c) 2012-2016 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "BindTransport.h"
#include "Server.h"
#include "ShortMacros.h"
#include "WorkerManager.h"
#include "MigrationTargetManager.h"
#include "MigrationBackupManager.h"
#include "RocksteadyMigrationManager.h"
#include "GeminiMigrationManager.h"
#include "AuxiliaryManager.h"

namespace RAMCloud {
/**
 * Constructor for Server: binds a configuration to a Server, but doesn't
 * start anything up yet.
 *
 * \param context
 *      Overall information about the RAMCloud server.  Note: if caller
 *      has not provided a serverList here, then we will.  This method
 *      will store information about the master and backup services in
 *      this context.
 * \param config
 *      Specifies which services and their configuration details for
 *      when the Server is run.
 */
Server::Server(Context* context, const ServerConfig* config)
    : context(context)
    , config(*config)
    , backupReadSpeed()
    , serverId()
    , serverList(NULL)
    , failureDetector()
    , master()
    , backup()
    , adminService()
    , enlistTimer()
    , auxDispatchThread()
{
    context->coordinatorSession->setLocation(
            config->coordinatorLocator.c_str(), config->clusterName.c_str());
    context->workerManager = new WorkerManager(context, config->maxCores-1);
    context->migrationTargetManager = new MigrationTargetManager(context);
    context->migrationBackupManager = new MigrationBackupManager(
        context, config->localLocator, config->segmentSize);
    context->rocksteadyMigrationManager =
        new RocksteadyMigrationManager(context, config->localLocator);
    auxDispatchThread.construct(auxDispatchMain, context, config);
}

/**
 * Destructor for Server.
 */
Server::~Server()
{
    delete serverList;
}

/**
 * Create services according to #config, enlist with the coordinator and
 * then return. This method should almost exclusively be used by MockCluster
 * and is only useful for unit testing.  Production code should always use
 * run() instead.
 *
 * \param bindTransport
 *      The BindTransport to register on to listen for rpcs during unit
 *      testing.
 */
void
Server::startForTesting(BindTransport& bindTransport)
{
    ServerId formerServerId = createAndRegisterServices();
    bindTransport.registerServer(context, config.localLocator);
    enlist(formerServerId);
}

/**
 * Create services according to #config and enlist with the coordinator.
 * Either call this method or startForTesting(), not both.  Loops
 * forever calling Dispatch::poll() to serve requests.
 */
void
Server::run()
{
    LOG(NOTICE, "Starting services");
    ServerId formerServerId = createAndRegisterServices();
    LOG(NOTICE, "Services started");

    // Only pin down memory _after_ users of LargeBlockOfMemory have
    // obtained their allocations (since LBOM probes are much slower if
    // the memory needs to be pinned during mmap).
    LOG(NOTICE, "Pinning memory");
    pinAllMemory();
    LOG(NOTICE, "Memory pinned");

    // The following statement suppresses a "long gap" message that would
    // otherwise be generated by the next call to dispatch.poll (the
    // warning is benign, and is caused by the time to benchmark secondary
    // storage above.
    Dispatch& dispatch = *context->dispatch;
    dispatch.currentTime = Cycles::rdtsc();

    // Enlist only once all expensive initialization has been done.
    // In particular, we must not enlist until we have mmapped all of the
    // log and hash table memory and have registered any regions with the NIC
    // (if appropriate). These large virtual memory operations can block
    // the entire process for seconds at a time, so we must not be expected
    // to handle RPCs until we're confident such hiccups won't occur.
    // Even so, some of the work that has to be done after enlistment takes
    // significant amounts of time, so execute the enlistment in a worker
    // thread. That way, this thread can enter the dispatcher and start
    // servicing requests.
    enlistTimer.construct(this, formerServerId);

    dispatch.run();
}

// - private -

/**
 * Create each of the services which are marked as active in config.services,
 * configure them according to #config, and register them.
 *
 * \return
 *      If this server is rejoining a cluster its former server id is returned,
 *      otherwise an invalid server is returned.  "Rejoining" means the backup
 *      service on this server may have segment replicas stored that were
 *      created by masters in the cluster.  In this case, the coordinator must
 *      be told of the former server id under which these replicas were
 *      created to ensure correct garbage collection of the stored replicas.
 */
ServerId
Server::createAndRegisterServices()
{
    ServerId formerServerId;

    if (config.services.has(WireFormat::COORDINATOR_SERVICE)) {
        DIE("Server class is not capable of running the CoordinatorService "
            "(yet).");
    }

    // If a serverList was already provided in the context, then use it;
    // otherwise, create a new one.
    if (context->serverList == NULL) {
        serverList = new ServerList(context);
    }

    if (config.services.has(WireFormat::MASTER_SERVICE)) {
        LOG(NOTICE, "Master is using %u backups", config.master.numReplicas);
        master.construct(context, &config);
    }

    if (config.services.has(WireFormat::BACKUP_SERVICE)) {
        LOG(NOTICE, "Starting backup service");
        backup.construct(context, &config);
        formerServerId = backup->getFormerServerId();
        backupReadSpeed = backup->getReadSpeed();
        LOG(NOTICE, "Backup service started");
    }

    if (config.services.has(WireFormat::ADMIN_SERVICE)) {
        adminService.construct(context,
                               static_cast<ServerList*>(context->serverList),
                               &config);
    }

    return formerServerId;
}

/**
 * Enlist the Server with the coordinator, notify all services of the ServerId
 * assigned by the coordinator, and start the failure detector if it is enabled
 * in #config.
 *
 * \param replacingId
 *      If this server has found replicas on storage written by a now-defunct
 *      server then the backup must report the server id that formerly owned
 *      those replicas upon enlistment. This is used to ensure that all
 *      servers in the cluster know of the crash of the old server which
 *      created the replicas before a new server enters the cluster
 *      attempting to reuse those replicas.  This property is used as part
 *      of the backup's replica garbage collection routines.
 */
void
Server::enlist(ServerId replacingId)
{
    // Enlist with the coordinator just before dedicating this thread
    // to rpc dispatch. This reduces the window of being unavailable to
    // service rpcs after enlisting with the coordinator (which can
    // lead to session open timeouts).
    LOG(NOTICE, "Enlisting with cooordinator");
    serverId = CoordinatorClient::enlistServer(context,
                                               config.preferredIndex,
                                               replacingId,
                                               config.services,
                                               config.localLocator,
                                               backupReadSpeed);
    LOG(NOTICE, "Enlisted; serverId %s", serverId.toString().c_str());

    // Finish AdminService initialization first, so that the getServerId
    // RPC will work; otherwise, no one can open connections to us.
    if (adminService)
        adminService->setServerId(serverId);
    if (master)
        master->setServerId(serverId);
    if (backup)
        backup->setServerId(serverId);

    if (config.detectFailures) {
        failureDetector.construct(context, serverId);
        failureDetector->start();
    }
}

void Server::auxDispatchMain(Context *context, const ServerConfig* config)
{
    context->auxDispatch = new Dispatch(true);
    context->auxManager = new AuxiliaryManager(context);
    context->geminiMigrationManager =
        new GeminiMigrationManager(context, config->localLocator);
    context->auxDispatch->run();
}

} // namespace RAMCloud