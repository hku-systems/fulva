/* Copyright (c) 2010-2015 Stanford University
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


#ifndef RAMCLOUD_MIGRATION_H
#define RAMCLOUD_MIGRATION_H

#include "TaskQueue.h"
#include "ServerId.h"
#include "TableManager.h"
#include "ServerTracker.h"
#include "LogDigest.h"
#include "TableStats.h"
#include "MigrationPartition.pb.h"

namespace RAMCloud {

class Migration;

typedef ServerTracker<Migration> MigrationTracker;

namespace MigrationInternal {
class BackupStartTask {
  PUBLIC:

    BackupStartTask(Migration *migration,
                    ServerId backupId);

    bool isDone() const
    { return done; }

    bool isReady()
    { return testingCallback || (rpc && rpc->isReady()); }

    void send();

    void filterOutInvalidReplicas();

    void wait();

    const ServerId backupId;
    MigrationStartReadingRpc::Result result;

  PRIVATE:
    Migration *migration;
    Tub<MigrationStartReadingRpc> rpc;
    bool done;

  PUBLIC:

    struct TestingCallback {
        virtual void backupStartTaskSend(
            MigrationStartReadingRpc::Result &result)
        {}

        virtual ~TestingCallback()
        {}
    };

    TestingCallback *testingCallback;
    DISALLOW_COPY_AND_ASSIGN(BackupStartTask);
};

class BackupStartPartitionTask {
  PUBLIC:

    BackupStartPartitionTask(Migration *migration, ServerId backupServerId);

    bool isReady()
    { return rpc && rpc->isReady(); }

    bool isDone() const
    { return done; }

    void send();

    void wait();


  PRIVATE:
    bool done;
    Tub<MigrationStartPartitioningRpc> rpc;
    const ServerId backupServerId;
    const Migration *migration;

    DISALLOW_COPY_AND_ASSIGN(BackupStartPartitionTask);
};

bool verifyLogComplete(Tub<BackupStartTask> tasks[],
                       size_t taskCount,
                       const LogDigest &digest);

Tub<std::tuple<uint64_t, LogDigest, TableStats::Digest *>>
findLogDigest(Tub<BackupStartTask> tasks[], size_t taskCount);

vector<WireFormat::MigrationTargetStart::Replica> buildReplicaMap(
    Tub<BackupStartTask> tasks[], size_t taskCount,
    MigrationTracker *tracker);

struct MasterStartTask;

struct MasterStartTaskTestingCallback {
    virtual void masterStartTaskSend(
        uint64_t migrationId,
        ServerId targetServerId,
        const WireFormat::MigrationTargetStart::Replica replicaMap[],
        size_t replicaMapSize)
    {}

    virtual ~MasterStartTaskTestingCallback()
    {}
};

struct BackupEndTask;

struct BackupEndTaskTestingCallback {
    virtual void backupEndTaskSend(ServerId backup, uint64_t migrationId)
    {}

    virtual ~BackupEndTaskTestingCallback()
    {}
};
}

class Migration : public Task {
  public:
    struct Owner {
        virtual void migrationFinished(Migration *migration)
        {}

        virtual ~Owner()
        {}
    };

    Migration(Context *context, TaskQueue &taskQueue, uint64_t migrationId,
              TableManager *tableManager, MigrationTracker *tracker,
              Owner *owner, ServerId sourceServerId,
              ServerId targetServerId, uint64_t tableId, uint64_t firstKeyHash,
              uint64_t lastKeyHash,
              const ProtoBuf::MasterRecoveryInfo masterRecoveryInfo);

    /// Shared RAMCloud information.
    Context *context;

    const ServerId sourceServerId;
    /// The id of the target which is being migrated to.
    const ServerId targetServerId;
    uint64_t tableId;
    uint64_t firstKeyHash;
    uint64_t lastKeyHash;

    const ProtoBuf::MasterRecoveryInfo masterRecoveryInfo;

    virtual void performTask();

    void migrationFinished(bool successful);

    bool operator==(Migration &that) const;

    uint64_t getMigrationId() const;

    bool wasCompletelySuccessful() const;

    /// Defines max number of bytes a tablet partition should accommodate.
    static const uint64_t PARTITION_MAX_BYTES = 500 * 1024 * 1024;
    /// Defines the max number of records a tablet partition should accommodate.
    static const uint64_t PARTITION_MAX_RECORDS = 2000000;
  PRIVATE:

    void splitTablets(vector<Tablet> *tablets,
                      TableStats::Estimator *estimator);

    void partitionTablets(vector<Tablet> tablets,
                          TableStats::Estimator *estimator);

    ProtoBuf::MigrationPartition dataToMigration;

    /**
     * Coordinator's authoritative information about tablets and their mapping
     * to servers. Used during to find out which tablets need to be recovered
     * for the crashed master.
     */
    TableManager *tableManager;

    MigrationTracker *tracker;
    /**
     * Owning object, if any, which gets called back on certain events.
     * See #Owner.
     */
    Owner *owner;

    uint64_t migrationId;

    enum Status {
        START_RECOVERY_ON_BACKUPS,
        START_RECOVERY_MASTERS,
        WAIT_FOR_RECOVERY_MASTERS,
        ALL_RECOVERY_MASTERS_FINISHED,
        DONE,
    };

    Status status;

    int migrateSuccessful;

    void startBackups();

    void startMasters();

    void broadcastMigrationComplete();

    uint32_t numPartitions;

  PUBLIC:
    MigrationInternal::BackupStartTask::TestingCallback *
        testingBackupStartTaskSendCallback;
    MigrationInternal::MasterStartTaskTestingCallback *
        testingMasterStartTaskSendCallback;
    MigrationInternal::BackupEndTaskTestingCallback *
        testingBackupEndTaskSendCallback;
    uint32_t testingFailRecoveryMasters;

    bool skipMaster;

    uint64_t backupStartTime;

    friend class MigrationInternal::BackupStartTask;

    friend class MigrationInternal::BackupStartPartitionTask;

    friend class MigrationInternal::MasterStartTask;
    DISALLOW_COPY_AND_ASSIGN(Migration);
};
};


#endif //RAMCLOUD_MIGRATION_H
