#include "MigrationTargetManager.h"
#include "MasterService.h"
#include "WorkerManager.h"

namespace RAMCloud {

MigrationTargetManager::MigrationTargetManager(Context *context) :
    Dispatch::Poller(context->dispatch, "MigrationTargetManager"),
    context(context), migrations(), migrationsInProgress(),
    lock("MigrationTargetManager"), finishNotifier(new RealFinishNotifier()),
    disableMigrationRecover(false), polling(false)
{
}

void
MigrationTargetManager::startMigration(uint64_t migrationId, Buffer *payload)
{
    Migration *migration = new Migration(
        this, payload, finishNotifier->clone());
    migrations[migrationId] = migration;
    migrationsInProgress.push_back(migration);
    RAMCLOUD_LOG(NOTICE, "Start migration %lu in Target", migrationId);
}

bool MigrationTargetManager::isLocked(uint64_t migrationId, Key &key)
{
    SpinLock::Guard guard(lock);
    auto migration = migrations.find(migrationId);
    if (migration == migrations.end())
        throw FatalError(HERE, "Migration not found");
    return migration->second->rangeList.isLocked(key.getHash());
}

void MigrationTargetManager::update(uint64_t migrationId,
                                    vector<WireFormat::MigrationIsLocked::Range> &ranges)
{

    SpinLock::Guard guard(lock);
    auto migration = migrations.find(migrationId);
    if (migration == migrations.end())
        throw FatalError(HERE, "Migration not found");
    for (auto range : ranges)
        migration->second->rangeList.push(range);

}

MigrationTargetManager::Migration *
MigrationTargetManager::getMigration(uint64_t migrationId)
{
    std::unordered_map<uint64_t, Migration *>::iterator migrationPair =
        migrations.find(migrationId);
    if (migrationPair == migrations.end())
        return NULL;
    return migrationPair->second;
}

int MigrationTargetManager::poll()
{

    int workPerformed = 0;
    if (polling)
        return workPerformed;
    polling = true;
    for (auto migration = migrationsInProgress.begin();
         migration != migrationsInProgress.end();) {
        Migration *currentMigration = *migration;


        workPerformed += currentMigration->poll();

        if (currentMigration->phase == Migration::COMPLETED) {
            migration = migrationsInProgress.erase(migration);
//            migrations.erase(currentMigration->migrationId);
            delete currentMigration;
        } else {
            migration++;
        }
    }

    polling = false;
    return workPerformed == 0 ? 0 : 1;
}

bool MigrationTargetManager::anyMigration()
{
    return !migrationsInProgress.empty();
}

MigrationTargetManager::Migration::Migration(
    MigrationTargetManager *manager, Buffer *payload,
    FinishNotifier *finishNotifier)
    : context(manager->context), localLocator(), migrationId(), replicas(),
      rangeList(), tableId(), firstKeyHash(), lastKeyHash(), sourceServerId(),
      targetServerId(), tabletManager(), objectManager(), safeVersion(),
      phase(SETUP),
      freePullBuffers(), freeReplayBuffers(), freePullRpcs(), busyPullRpcs(),
      freeReplayRpcs(), busyReplayRpcs(), freeSideLogs(), sideLogCommitRpc(),
      totalReplayedBytes(0), numReplicas(), numCompletedReplicas(0),
      migrationStartTS(), migrationEndTS(), migratedMegaBytes(),
      sideLogCommitStartTS(), sideLogCommitEndTS(),
      finishNotifier(finishNotifier), finishNotified(false),
      tombstoneProtector(), timestamp(0), lastTime(0), inputBandwidth(0),
      outputBandwidth(0), lastReplayedBytes(0), bandwidthSamples(), print(false)
{
    WireFormat::MigrationTargetStart::Request *reqHdr =
        payload->getOffset<WireFormat::MigrationTargetStart::Request>(0);

    migrationId = reqHdr->migrationId;
    tableId = reqHdr->tableId;
    firstKeyHash = reqHdr->firstKeyHash;
    lastKeyHash = reqHdr->lastKeyHash;
    sourceServerId = ServerId(reqHdr->sourceServerId);
    targetServerId = ServerId(reqHdr->targetServerId);
    numReplicas = reqHdr->numReplicas;

    MasterService *masterService = context->getMasterService();
    tabletManager = &(masterService->tabletManager);
    objectManager = &(masterService->objectManager);
    safeVersion = reqHdr->safeVersion;
    tabletManager->raiseSafeVersion(safeVersion);
    objectManager->raiseSafeVersion(safeVersion);
    localLocator = masterService->config->localLocator;

    uint32_t offset = sizeof32(WireFormat::MigrationTargetStart::Request);
    for (uint32_t i = 0; i < numReplicas; ++i) {
        const WireFormat::MigrationTargetStart::Replica *replicaLocation =
            payload->getOffset<WireFormat::MigrationTargetStart::Replica>(
                offset);
        offset += sizeof32(WireFormat::MigrationTargetStart::Replica);
        replicas.push_back(
            new Replica(replicaLocation->backupId, replicaLocation->segmentId));
    }

    RAMCLOUD_LOG(WARNING, "replicas number: %u", numReplicas);
    //TODO: handle log head
    for (int i = 0; i < 3; i++) {
        RAMCLOUD_CLOG(WARNING, "skip segment %lu", replicas.front()->segmentId);
        replicas.pop_front();
        numReplicas--;
    }

    for (uint32_t i = 0; i < PIPELINE_DEPTH; i++) {
        freePullBuffers.push_back(&(rpcBuffers[i]));
    }

    for (uint32_t i = 0; i < MAX_PARALLEL_PULL_RPCS; i++) {
        freePullRpcs.push_back(&(pullRpcs[i]));
    }

    // To begin with, all replay rpcs are free.
    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        freeReplayRpcs.push_back(&(replayRpcs[i]));
    }

    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        sideLogs[i].construct(objectManager->getLog(), manager);
    }

    // To begin with, all sidelogs are free.
    for (uint32_t i = 0; i < MAX_PARALLEL_REPLAY_RPCS; i++) {
        freeSideLogs.push_back(&(sideLogs[i]));
    }

    migrationStartTS = Cycles::rdtsc();
}

int MigrationTargetManager::Migration::poll()
{
    uint64_t stop = Cycles::rdtsc();
    if (Cycles::toMicroseconds(stop - lastTime) > 100000) {
        uint64_t currentInput = metrics->transport.receive.byteCount;
        uint64_t currentOutput = metrics->transport.transmit.byteCount;

        timestamp++;

        bandwidthSamples.emplace_back(
            static_cast<double>(currentInput -
                                inputBandwidth) / 1024 / 102,
            static_cast<double>(currentOutput -
                                outputBandwidth) / 1024 / 102,
            static_cast<double>(totalReplayedBytes -
                                lastReplayedBytes) / 1024 /
            102
        );

        inputBandwidth = currentInput;
        outputBandwidth = currentOutput;
        lastReplayedBytes = totalReplayedBytes;
        lastTime = stop;
    }
    if (!print && migrationEndTS != 0 &&
        Cycles::toSeconds(stop - migrationEndTS) > 2) {
        print = true;
        int i = 0;
        for (BandwidthSample &sample: bandwidthSamples) {
            i++;
            RAMCLOUD_LOG(NOTICE, "%d, %lf, %lf, %lf",
                         i, sample.inputBandwidth,
                         sample.migrationBandwidth,
                         sample.outputBandwidth);
        }
    }

    switch (phase) {
        case SETUP :
            return prepare();

        case MIGRATING_DATA :
            return pullAndReplay_main();
        case SIDE_LOG_COMMIT :
            return sideLogCommit();

        case COMPLETED :
            return 0;

        default :
            return 0;
    }
}


uint64_t MigrationTargetManager::Migration::getSafeVersion()
{
    return safeVersion;
}

bool MigrationTargetManager::Migration::isFinished()
{
    return phase > MIGRATING_DATA;
}

int MigrationTargetManager::Migration::prepare()
{
    bool added = tabletManager->addTablet(
        tableId,
        firstKeyHash,
        lastKeyHash,
        TabletManager::MIGRATION_TARGET);
    if (!added) {
        throw Exception(HERE,
                        format("Cannot recover tablet that overlaps "
                               "an already existing one (tablet to recover: %lu "
                               "range [0x%lx,0x%lx], current tablet map: %s)",
                               tableId,
                               firstKeyHash, lastKeyHash,
                               tabletManager->toString().c_str()));
    }
    tabletManager->migrateTablet(
        tableId, firstKeyHash, lastKeyHash, migrationId, sourceServerId.getId(),
        targetServerId.getId(), TabletManager::MIGRATION_TARGET);

    phase = MIGRATING_DATA;
    if (!tombstoneProtector)
        tombstoneProtector.construct(objectManager);

    lastTime = Cycles::rdtsc();
    inputBandwidth = metrics->transport.receive.byteCount;
    outputBandwidth = metrics->transport.transmit.byteCount;
    lastReplayedBytes = totalReplayedBytes;

    RAMCLOUD_LOG(NOTICE, "Migration %lu start serving at Target",
                 migrationId);
    return 1;
}

int MigrationTargetManager::Migration::pullAndReplay_main()
{
    int workDone = 0;

    workDone += pullAndReplay_reapPullRpcs();

    workDone += pullAndReplay_reapReplayRpcs();

    if (numCompletedReplicas == numReplicas) {
        migrationEndTS = Cycles::rdtsc();
        sideLogCommitStartTS = migrationEndTS;

        double migrationSeconds = Cycles::toSeconds(migrationEndTS -
                                                    migrationStartTS);

        migratedMegaBytes =
            static_cast<double>(totalReplayedBytes) / (1024. * 1024.);

        RAMCLOUD_LOG(WARNING,
                     "Migration has completed. Changing state to state to"
                     " SIDE_LOG_COMMIT (Tablet[0x%lx, 0x%lx] in table %lu)."
                     " Moving %.2f MB of data over took %.2f seconds (%.2f MB/s)",
                     firstKeyHash, lastKeyHash, tableId, migratedMegaBytes,
                     migrationSeconds, migratedMegaBytes / migrationSeconds);

        bool changed = tabletManager->changeState(
            tableId, firstKeyHash, lastKeyHash,
            TabletManager::MIGRATION_TARGET, TabletManager::NORMAL);
        if (!changed) {
            throw FatalError(
                HERE, format("Could not change recovering "
                             "tablet's state to NORMAL (%lu range [%lu,%lu])",
                             tableId, firstKeyHash, lastKeyHash));
        }
        phase = SIDE_LOG_COMMIT;

        return workDone;

    }

    workDone += pullAndReplay_sendPullRpcs();

    workDone += pullAndReplay_sendReplayRpcs();
    return workDone;
}

//__inline __attribute__((always_inline))
int MigrationTargetManager::Migration::pullAndReplay_reapPullRpcs()
{
    int workDone = 0;
    size_t numBusyPullRpcs = busyPullRpcs.size();

    for (size_t i = 0; i < numBusyPullRpcs; i++) {
        Tub<PullRpc> *pullRpc = busyPullRpcs.front();
        Replica *replica = (*pullRpc)->replica;
        if ((*pullRpc)->isReady()) {
            try {
                bool done = (*pullRpc)->wait();

                replica->done = done;
                freeReplayBuffers.emplace_back((*pullRpc)->responseBuffer,
                                               replica);
                workDone++;
            } catch (SegmentRecoveryFailedException &e) {
                RAMCLOUD_LOG(ERROR, "skip a failed segment:%lu",
                             replica->segmentId);
                replica->done = true;
                numCompletedReplicas++;
                Tub<Buffer> *responseBuffer = (*pullRpc)->responseBuffer;
                (*responseBuffer).destroy();
                freePullBuffers.push_back(responseBuffer);
                workDone++;
            }

            (*pullRpc).destroy();

            freePullRpcs.push_back(pullRpc);

        } else {
            busyPullRpcs.push_back(pullRpc);
        }
        busyPullRpcs.pop_front();
    }
    return 0;
}

//__inline __attribute__((always_inline))
int MigrationTargetManager::Migration::pullAndReplay_reapReplayRpcs()
{
    int workDone = 0;

    size_t numBusyReplayRpcs = busyReplayRpcs.size();

    for (size_t i = 0; i < numBusyReplayRpcs; i++) {
        Tub<ReplayRpc> *replayRpc = busyReplayRpcs.front();

        if ((*replayRpc)->isReady()) {
            Tub<Buffer> *responseBuffer = (*replayRpc)->responseBuffer;
            Tub<SideLog> *sideLog = (*replayRpc)->sideLog;
            Replica *replica = (*replayRpc)->replica;
            uint32_t numReplayedBytes = (*responseBuffer)->size();
            totalReplayedBytes += numReplayedBytes;
            (*responseBuffer).destroy();
            freePullBuffers.push_back(responseBuffer);
            freeSideLogs.push_back(sideLog);

            if ((*replayRpc)->done) {
                numCompletedReplicas++;
                if (numCompletedReplicas == numReplicas ||
                    numCompletedReplicas % 10 == 0) {
                    double time = Cycles::toSeconds(
                        Cycles::rdtsc() - migrationStartTS);
                    RAMCLOUD_LOG(DEBUG, "replay progress: %u/%u, time: %.3lf",
                                 numCompletedReplicas, numReplicas, time);
                }
            } else {
                replica->seqId += 1;
                replicas.push_front(replica);
            }

            (*replayRpc).destroy();
            freeReplayRpcs.push_back(replayRpc);
            workDone++;
        } else {
            busyReplayRpcs.push_back(replayRpc);
        }
        busyReplayRpcs.pop_front();
    }
    return 0;
}

//__inline __attribute__((always_inline))
int MigrationTargetManager::Migration::pullAndReplay_sendPullRpcs()
{
    int workDone = 0;
    while (!freePullRpcs.empty() && !freePullBuffers.empty() &&
           !replicas.empty()) {
        Replica *replica = replicas.front();
        Tub<PullRpc> *pullRpc = freePullRpcs.front();

        Tub<Buffer> *responseBuffer = freePullBuffers.front();
        responseBuffer->construct();

        pullRpc->construct(context, migrationId, replica,
                           sourceServerId, responseBuffer);
        replicas.pop_front();

        freePullBuffers.pop_front();
        freePullRpcs.pop_front();
        busyPullRpcs.push_back(pullRpc);
        workDone++;
    }

    return workDone;
}

//__inline __attribute__((always_inline))
int MigrationTargetManager::Migration::pullAndReplay_sendReplayRpcs()
{

    int workDone = 0;

    while (!freeReplayRpcs.empty() && !freeReplayBuffers.empty()) {
        Tub<ReplayRpc> *replayRpc = freeReplayRpcs.front();
        Tub<SideLog> *sideLog = freeSideLogs.front();
        auto bufferAndReplica = freeReplayBuffers.front();
        Tub<Buffer> *responseBuffer = bufferAndReplica.first;
        Replica *replica = bufferAndReplica.second;

        auto respHdr = (*responseBuffer)->
            getOffset<WireFormat::MigrationGetData::Response>(0);

        SegmentCertificate certificate =
            respHdr->certificate;
        (*responseBuffer)->truncateFront(
            sizeof32(WireFormat::MigrationGetData::Response));

        (*replayRpc).construct(replica, responseBuffer, sideLog,
                               localLocator, respHdr->done, certificate);
        context->workerManager->handleRpc(replayRpc->get());

        freeReplayBuffers.pop_front();
        freeSideLogs.pop_front();
        busyReplayRpcs.push_back(replayRpc);
        freeReplayRpcs.pop_front();
        workDone++;
    }
    return workDone;
}

int MigrationTargetManager::Migration::sideLogCommit()
{

    int workDone = 0;

    if (!finishNotified) {
        if (!finishNotifier->isSent()) {
            finishNotifier->notify(this);
        } else if (finishNotifier->isReady()) {
            finishNotifier->wait();
            finishNotified = true;
            RAMCLOUD_LOG(WARNING, "migration %lu finished", migrationId);
            return 1;
        }
    }


    if (sideLogCommitRpc) {
        if (sideLogCommitRpc->isReady()) {
            sideLogCommitRpc.destroy();
            workDone++;
        }
    }

    if (!sideLogCommitRpc) {
        if (freeSideLogs.empty()) {
            sideLogCommitEndTS = Cycles::rdtsc();
            if (tombstoneProtector)
                tombstoneProtector.destroy();
            if (finishNotified) {
                phase = COMPLETED;
                RAMCLOUD_LOG(WARNING, "SideLog commit finish, notifying");
            }
        } else {
            sideLogCommitRpc.construct(freeSideLogs.front(), localLocator);
            context->workerManager->handleRpc(sideLogCommitRpc.get());
            freeSideLogs.pop_front();
        }
        workDone++;
    }
    return workDone;
}

MigrationTargetManager::Replica::Replica(
    uint64_t backupId, uint64_t segmentId)
    : backupId(backupId), segmentId(segmentId), seqId(0), done(false)
{
}

MigrationTargetManager::RealFinishNotifier::RealFinishNotifier()
    : coordinatorRpc(), sourceRpc()
{
}

MigrationTargetManager::RealFinishNotifier::~RealFinishNotifier()
{
}

void MigrationTargetManager::RealFinishNotifier::notify(Migration *migration)
{
    if (!coordinatorRpc)
        coordinatorRpc.construct(migration->context, migration->migrationId,
                                 migration->targetServerId, true);
    if (!sourceRpc)
        sourceRpc.construct(migration->context, migration->sourceServerId,
                            migration->migrationId, true);
}

bool MigrationTargetManager::RealFinishNotifier::isSent()
{
    return coordinatorRpc && sourceRpc;
}

bool MigrationTargetManager::RealFinishNotifier::isReady()
{
    if (coordinatorRpc && sourceRpc)
        return coordinatorRpc->isReady() && sourceRpc->isReady();
    else
        return false;
}

bool MigrationTargetManager::RealFinishNotifier::wait()
{
    if (coordinatorRpc && sourceRpc) {
        sourceRpc->wait();
        return coordinatorRpc->wait();
    }
    return false;

}

MigrationTargetManager::FinishNotifier *
MigrationTargetManager::RealFinishNotifier::clone()
{
    return new RealFinishNotifier();
}


}
