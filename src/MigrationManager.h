#ifndef RAMCLOUD_MIGRATIONMANAGER_H
#define RAMCLOUD_MIGRATIONMANAGER_H


#include "TaskQueue.h"
#include "Migration.h"
#include "RuntimeOptions.h"
#include "CoordinatorServerList.h"

namespace RAMCloud {
namespace MigrationManagerInternal {

class ApplyTrackerChangesTask;

class MaybeStartMigrationTask;

class EnqueueMigrationTask;

class MigrationFinishedTask;
}

class MigrationManager
    : public Migration::Owner, public ServerTracker<Migration>::Callback {
  PUBLIC:

    MigrationManager(Context *context,
                     TableManager &tableManager,
                     RuntimeOptions *runtimeOptions);

    ~MigrationManager();

    uint64_t startMigration(
        ServerId sourceServerId, ServerId targetServerId,
        uint64_t tableId, uint64_t firstKeyHash,
        uint64_t lastKeyHash,
        const ProtoBuf::MasterRecoveryInfo &masterRecoveryInfo,
        bool skipMaster);

    bool migrationMasterFinished(uint64_t recoveryId,
                                 ServerId recoveryMasterId,
                                 bool successful);

    void start();

    void halt();

    virtual void trackerChangesEnqueued();

    virtual void migrationFinished(Migration *migration);

    bool isFinished(uint64_t migrationId);

  PRIVATE:

    void main();

    Context *context;

    TableManager &tableManager;

    RuntimeOptions *runtimeOptions;

    Tub<std::thread> thread;

    std::queue<Migration *> waitingMigrations;

    typedef std::unordered_map<uint64_t, Migration *> MigrationMap;

    MigrationMap activeMigrations;

    uint32_t maxActiveMigrations;

    TaskQueue taskQueue;

    MigrationTracker tracker;

    bool doNotStartMigrations;

    bool startMigrationsEvenIfNoThread;

    bool skipRescheduleDelay;

    std::mutex mutex;
    uint64_t migrationNumber;

    friend class MigrationManagerInternal::ApplyTrackerChangesTask;

    friend class MigrationManagerInternal::MaybeStartMigrationTask;

    friend class MigrationManagerInternal::EnqueueMigrationTask;

    friend class MigrationManagerInternal::MigrationFinishedTask;

    DISALLOW_COPY_AND_ASSIGN(MigrationManager);
};
}


#endif //RAMCLOUD_MIGRATIONMANAGER_H
