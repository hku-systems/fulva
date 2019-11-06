#include "MigrationManager.h"
#include "ShortMacros.h"

namespace RAMCloud {

namespace MigrationManagerInternal {

class MaybeStartMigrationTask : public Task {
  PUBLIC:

    explicit MaybeStartMigrationTask(MigrationManager &migrationManager)
        : Task(migrationManager.taskQueue),
          mgr(migrationManager)
    {}

    void performTask()
    {
        std::vector<Migration *> alreadyActive;
        while (!mgr.waitingMigrations.empty() &&
               mgr.activeMigrations.size() < mgr.maxActiveMigrations) {
            Migration *migration = mgr.waitingMigrations.front();

            // Do not allow two recoveries for the same crashed master
            // at the same time. This can happen if one recovery fails
            // and schedules another. The second may get started before
            // the first finishes without this check.
            bool alreadyMigration = false;
            for (const auto &other: mgr.activeMigrations) {
                if (*migration == *other.second) {
                    alreadyMigration = true;
                    break;
                }
            }

            if (alreadyMigration) {
                alreadyActive.push_back(migration);
                mgr.waitingMigrations.pop();
            } else {
                migration->schedule();
                mgr.activeMigrations[migration->getMigrationId()] = migration;
                mgr.waitingMigrations.pop();
                RAMCLOUD_LOG(NOTICE,
                             "Starting recovery of server %s (now %lu active "
                             "recoveries)",
                             migration->targetServerId.toString().c_str(),
                             mgr.activeMigrations.size());
            }
        }
        for (auto *migration: alreadyActive)
            mgr.waitingMigrations.push(migration);
        if (mgr.waitingMigrations.size() > 0)
            RAMCLOUD_LOG(NOTICE,
                         "%lu recoveries blocked waiting for other recoveries",
                         mgr.waitingMigrations.size());
        delete this;
    }

  PRIVATE:
    MigrationManager &mgr;
};

class EnqueueMigrationTask : public Task {
  PUBLIC:

    EnqueueMigrationTask(MigrationManager &migrationManager,
                         Migration *migration)
        : Task(migrationManager.taskQueue),
          mgr(migrationManager),
          migration(migration)
    {}

    void performTask()
    {
        mgr.waitingMigrations.push(migration);
        (new MaybeStartMigrationTask(mgr))->schedule();
        delete this;
    }

  PRIVATE:
    MigrationManager &mgr;
    Migration *migration;

    DISALLOW_COPY_AND_ASSIGN(EnqueueMigrationTask);
};


class MigrationFinishedTask : public Task {
  PUBLIC:

    MigrationFinishedTask(MigrationManager &migrationManager,
                          uint64_t migrationId,
                          ServerId targetId,
                          bool successful)
        : Task(migrationManager.taskQueue), mgr(migrationManager),
          migrationId(migrationId), recoveryMasterId(targetId),
          successful(successful), mutex(),
          taskPerformed(false), performed(),
          cancelRecoveryOnRecoveryMaster(false)
    {}

    void performTask()
    {
        Lock _(mutex);
        auto it = mgr.activeMigrations.find(migrationId);
        if (it == mgr.activeMigrations.end()) {
            RAMCLOUD_LOG(ERROR, "Recovery master reported completing recovery "
                                "%lu but there is no ongoing recovery with that id; "
                                "this should only happen after coordinator rollover; "
                                "asking recovery master to abort this recovery",
                         migrationId);
            taskPerformed = true;
            cancelRecoveryOnRecoveryMaster = true;
            performed.notify_all();
            return;
        }

        Migration *migration = it->second;
        migration->migrationFinished(successful);
        taskPerformed = true;
        performed.notify_all();
    }

    bool wait()
    {
        Lock lock(mutex);
        while (!taskPerformed)
            performed.wait(lock);
        bool shouldAbort = this->cancelRecoveryOnRecoveryMaster;
        return shouldAbort;
    }

  PRIVATE:
    MigrationManager &mgr;
    uint64_t migrationId;
    ServerId recoveryMasterId;
    bool successful;

    std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;

    bool taskPerformed;

    std::condition_variable performed;
    bool cancelRecoveryOnRecoveryMaster;
};

class ApplyTrackerChangesTask : public Task {
  PUBLIC:

    /**
     * Create a task which when run will apply all enqueued changes to
     * #tracker and notifies any recoveries which have lost recovery masters.
     * This brings #mgr.tracker into sync with #mgr.serverList. Because this
     * task is run by #mgr.taskQueue is it serialized with other tasks.
     */
    explicit ApplyTrackerChangesTask(MigrationManager &mgr)
        : Task(mgr.taskQueue), mgr(mgr)
    {
    }

    void performTask()
    {
        ServerDetails server;
        ServerChangeEvent event;
        while (mgr.tracker.getChange(server, event)) {
            if (event == SERVER_CRASHED || event == SERVER_REMOVED) {
                mgr.tracker[server.serverId] = NULL;
            }
        }
        delete this;
    }

    MigrationManager &mgr;
};
}

using namespace MigrationManagerInternal; // NOLINT

MigrationManager::MigrationManager(Context *context, TableManager &tableManager,
                                   RuntimeOptions *runtimeOptions)
    : context(context), tableManager(tableManager),
      runtimeOptions(runtimeOptions), thread(), waitingMigrations(),
      activeMigrations(), maxActiveMigrations(1u), taskQueue(),
      tracker(context, this), doNotStartMigrations(false),
      startMigrationsEvenIfNoThread(false), skipRescheduleDelay(false),
      mutex(), migrationNumber(0)
{

}

MigrationManager::~MigrationManager()
{

}

void MigrationManager::main()
try
{
    taskQueue.performTasksUntilHalt();
} catch (const std::exception &e) {
    RAMCLOUD_LOG(ERROR, "Fatal error in MigrationManager: %s", e.what());
    throw;
} catch (...) {
    RAMCLOUD_LOG(ERROR, "Unknown fatal error in MigrationManager.");
    throw;
}

uint64_t MigrationManager::startMigration(
    ServerId sourceServerId,
    ServerId targetServerId,
    uint64_t tableId,
    uint64_t firstKeyHash,
    uint64_t lastKeyHash,
    const ProtoBuf::MasterRecoveryInfo &masterRecoveryInfo,
    bool skipMaster)
{
    uint64_t migrationId;
    if (!thread && !startMigrationsEvenIfNoThread) {
        // Recovery has not yet been officially enabled, so don't do
        // anything (when the start method is invoked, it will
        // automatically start recovery of all servers in the crashed state).
        TEST_LOG("Migration requested for %s",
                 targetServerId.toString().c_str());
        return 0;
    }
    RAMCLOUD_LOG(NOTICE, "Scheduling migration from %s to %s for "
                         "tablet(id:%lu, first:%lx, last:%lx)",
                 sourceServerId.toString().c_str(),
                 targetServerId.toString().c_str(),
                 tableId, firstKeyHash, lastKeyHash);
    if (doNotStartMigrations) {
        TEST_LOG("migration targetServerId: %s",
                 targetServerId.toString().c_str());
        return 0;
    }
    {
        std::unique_lock<std::mutex> _(mutex);
        migrationNumber++;
        migrationId = migrationNumber;
        Migration *migration = new Migration(context, taskQueue,
                                             migrationId,
                                             &tableManager,
                                             &tracker,
                                             this, sourceServerId,
                                             targetServerId, tableId,
                                             firstKeyHash, lastKeyHash,
                                             masterRecoveryInfo);
        migration->skipMaster = skipMaster;
        (new MigrationManagerInternal::EnqueueMigrationTask(
            *this, migration))->schedule();
    }
    return migrationId;
}


bool MigrationManager::migrationMasterFinished(uint64_t recoveryId,
                                               ServerId recoveryMasterId,
                                               bool successful)
{
    TEST_LOG("Recovered tablets");
    MigrationFinishedTask task(*this, recoveryId, recoveryMasterId, successful);
    task.schedule();
    bool shouldAbort = task.wait();
    if (shouldAbort)
        RAMCLOUD_LOG (NOTICE, "Asking recovery master to abort its recovery");
    else
        RAMCLOUD_LOG (NOTICE, "Notifying recovery master ok to serve tablets");
    return shouldAbort;
}

void MigrationManager::start()
{
    if (!thread)
        thread.construct(&MigrationManager::main, this);
}

void MigrationManager::halt()
{
    taskQueue.halt();
    if (thread)
        thread->join();
    thread.destroy();
}

void MigrationManager::trackerChangesEnqueued()
{
    (new MigrationManagerInternal::ApplyTrackerChangesTask(*this))->schedule();
}

void MigrationManager::migrationFinished(Migration *migration)
{
    RAMCLOUD_LOG(NOTICE,
                 "Migration %lu completed for master %s (now %lu active "
                 "migrations)",
                 migration->getMigrationId(),
                 migration->targetServerId.toString().c_str(),
                 activeMigrations.size() - 1);
    if (migration->wasCompletelySuccessful()) {
        // Remove recovered server from the server list and broadcast
        (new MigrationManagerInternal::MaybeStartMigrationTask(*this))
            ->schedule();
    } else {
        RAMCLOUD_LOG(NOTICE,
                     "Recovery of server %s failed to recover some "
                     "tablets, rescheduling another recovery",
                     migration->targetServerId.toString().c_str());

        if (!skipRescheduleDelay) {
            usleep(2000000);
        }

    }

    activeMigrations.erase(migration->getMigrationId());
    delete migration;
}

bool MigrationManager::isFinished(uint64_t migrationId)
{
    return activeMigrations.find(migrationId) == activeMigrations.end();
}

}
