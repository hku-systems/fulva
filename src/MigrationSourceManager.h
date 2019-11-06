#ifndef RAMCLOUD_MIGRATIONSOURCEMANAGER_H
#define RAMCLOUD_MIGRATIONSOURCEMANAGER_H

#include <queue>
#include <unordered_map>

#include "Key.h"
#include "SpinLock.h"
#include "WorkerTimer.h"
#include "TransactionManager.h"
#include "RangeList.h"


namespace RAMCloud {


class MigrationSourceManager : public WorkerTimer {
  PRIVATE:

    enum ItemType {
        LOCK, UNLOCK
    };

    struct Item {
        ItemType type;
        uint64_t migrationId;
        uint64_t keyHash;
        uint64_t timestamp;
    };

    class Migration {
      PUBLIC:

        Migration(MigrationSourceManager *manager,
                  uint64_t migrationId,
                  uint64_t tableId,
                  uint64_t firstKeyHash,
                  uint64_t lastKeyHash,
                  uint64_t sourceId,
                  uint64_t targetId);

        void lock(uint64_t keyHash);

        void unlock(uint64_t keyHash);

        void start();

        void finish();

      PRIVATE:
        MigrationSourceManager *manager;
        RangeList rangeList;

        uint64_t migrationId;
        uint64_t tableId;
        uint64_t firstKeyHash;
        uint64_t lastKeyHash;
        uint64_t sourceId;
        uint64_t targetId;
        uint64_t timestamp;
        uint64_t startEpoch;
        bool active;
        friend MigrationSourceManager;
        DISALLOW_COPY_AND_ASSIGN(Migration);
    };

    struct StartNotifier {
        virtual void notify(uint64_t migrationId) = 0;

        virtual ~StartNotifier()
        {}
    };

    struct RealStartNotifier : public StartNotifier {

        void notify(uint64_t migrationId);

        virtual ~RealStartNotifier()
        {}
    };

    MasterService *masterService;
    TransactionManager *transactionManager;
    std::unique_ptr<std::queue<Item>[]> operatingBuffer;
    uint8_t operatingIndex;
    SpinLock bufferLock;
    std::mutex listLock;
    std::unordered_map<uint64_t, Migration *> migrations;
    std::list<Migration *> prepQueue, activeQueue;
    uint64_t epoch;


  PUBLIC:

    MigrationSourceManager(MasterService *masterService);

    ~MigrationSourceManager();

    virtual void handleTimerEvent();

    void startMigration(uint64_t migrationId, uint64_t tableId,
                        uint64_t firstKeyHash, uint64_t lastKeyHash,
                        uint64_t sourceId, uint64_t targetId);

    void finishMigration(uint64_t migrationId);

    Migration *getMigration(uint64_t migrationId);

    void lock(uint64_t migrationId, Key &key, uint64_t timestamp);

    void unlock(uint64_t migrationId, Key &key, uint64_t timestamp);

    Status isLocked(uint64_t migrationId, Key &key, bool *isLocked,
                    vector<WireFormat::MigrationIsLocked::Range> &ranges);

    std::unique_ptr<StartNotifier> startNotifier;
  PRIVATE:

    DISALLOW_COPY_AND_ASSIGN(MigrationSourceManager);
};

}

#endif //RAMCLOUD_MIGRATIONSOURCEMANAGER_H
