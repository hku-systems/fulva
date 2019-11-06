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

#include <unordered_set>

#include "Migration.h"
#include "ParallelRun.h"
#include "ShortMacros.h"
#include "MasterClient.h"

namespace RAMCloud {

Migration::Migration(Context *context, TaskQueue &taskQueue,
                     uint64_t migrationId,
                     TableManager *tableManager, MigrationTracker *tracker,
                     Owner *owner, ServerId sourceServerId,
                     ServerId targetServerId, uint64_t tableId,
                     uint64_t firstKeyHash, uint64_t lastKeyHash,
                     const ProtoBuf::MasterRecoveryInfo masterRecoveryInfo)
    : Task(taskQueue),
      context(context),
      sourceServerId(sourceServerId),
      targetServerId(targetServerId),
      tableId(tableId),
      firstKeyHash(firstKeyHash),
      lastKeyHash(lastKeyHash),
      masterRecoveryInfo(masterRecoveryInfo),
      dataToMigration(),
      tableManager(tableManager),
      tracker(tracker),
      owner(owner),
      migrationId(migrationId),
      status(START_RECOVERY_ON_BACKUPS),
      migrateSuccessful(0),
      numPartitions(),
      testingBackupStartTaskSendCallback(),
      testingMasterStartTaskSendCallback(),
      testingBackupEndTaskSendCallback(),
      testingFailRecoveryMasters(),
      skipMaster(false),
      backupStartTime(0)
{
}

void Migration::performTask()
{
    if (status == DONE) {
        // We get here only if there were no tablets in the crashed master
        // when the constructor for this Recovery object was invoked.
        RAMCLOUD_LOG(NOTICE, "No tablets in crashed server %s, removing it from"
                             " coordinator server list",
                     sourceServerId.toString().c_str());
        if (owner) {
            owner->migrationFinished(this);
        }
        return;
    }
    switch (status) {
        case START_RECOVERY_ON_BACKUPS:
            RAMCLOUD_LOG(NOTICE, "Starting recovery %lu for crashed server %s",
                         migrationId, sourceServerId.toString().c_str());
            startBackups();
            break;
        case START_RECOVERY_MASTERS:
            startMasters();
            break;
        case WAIT_FOR_RECOVERY_MASTERS:
            // Calls to recoveryMasterFinished drive
            // recovery from WAIT_FOR_RECOVERY_MASTERS to
            // ALL_RECOVERY_MASTERS_FINISHED.
            assert(false);
            break;
        case ALL_RECOVERY_MASTERS_FINISHED:
            // Tell all of the backups that they can reclaim recovery state.
            // This is the "right" place to do this (don't want to do it in
            // recoveryMasterFinished, because that's in the middle of handling
            // an RPC). However, this code breaks recovery metrics since they
            // use this broadcast as a signal to stop their recovery timers,
            // and by the time code is executed here, the client could already
            // fetched the metrics (resulting in divide-by-zero errors). This
            // #ifdef can be changed to move the notification for benchmarking.
            // A better solution would be to make the backups stop their timer
            // when metrics are fetched.
#define BCAST_INLINE 0
#if !BCAST_INLINE
            broadcastMigrationComplete();
#endif
            status = DONE;
            if (owner)
                owner->migrationFinished(this);
            break;
        case DONE:
        default:
            assert(false);
    }
}

void Migration::migrationFinished(bool successful)
{
    if (successful) {
        migrateSuccessful = 1;
        status = ALL_RECOVERY_MASTERS_FINISHED;
#if BCAST_INLINE
        broadcastMigrationComplete();
#endif
        schedule();
    } else {
        migrateSuccessful = -1;
    }
}

bool Migration::operator==(Migration &that) const
{
    return this->tableId == that.tableId &&
           this->firstKeyHash == that.firstKeyHash &&
           this->lastKeyHash == that.lastKeyHash &&
           this->targetServerId == that.targetServerId;
}


uint64_t Migration::getMigrationId() const
{
    return migrationId;
}

bool Migration::wasCompletelySuccessful() const
{
    return migrateSuccessful == 1;
}

namespace MigrationInternal {
BackupStartTask::BackupStartTask(Migration *migration, ServerId backupId)
    : backupId(backupId), result(), migration(migration), rpc(), done(),
      testingCallback(migration ?
                      migration->testingBackupStartTaskSendCallback :
                      NULL)
{

}

void BackupStartTask::send()
{
    RAMCLOUD_LOG(DEBUG, "Starting startReadingData on backup %s",
                 backupId.toString().c_str());
    if (!testingCallback) {
        rpc.construct(migration->context, backupId, migration->migrationId,
                      migration->sourceServerId, migration->targetServerId,
                      migration->tableId, migration->firstKeyHash,
                      migration->lastKeyHash);
    } else {
        testingCallback->backupStartTaskSend(result);
    }
}

void BackupStartTask::filterOutInvalidReplicas()
{
    uint64_t minOpenSegmentId =
        migration->masterRecoveryInfo.min_open_segment_id();
    uint64_t minOpenSegmentEpoch =
        migration->masterRecoveryInfo.min_open_segment_epoch();

    vector<MigrationStartReadingRpc::Replica> newReplicas;
    newReplicas.reserve(result.replicas.size());
    uint32_t newPrimaryReplicaCount = 0;

    for (size_t i = 0; i < result.replicas.size(); ++i) {
        auto &replica = result.replicas[i];
        if (!replica.closed) {
            if (replica.segmentId < minOpenSegmentId ||
                (replica.segmentId == minOpenSegmentId &&
                 replica.segmentEpoch < minOpenSegmentEpoch)) {
                RAMCLOUD_LOG(DEBUG,
                             "Removing replica for segmentId %lu from replica "
                             "list for backup %s because it was open and had "
                             "<id, epoch> <%lu ,%lu> which is less than the required "
                             "<id, epoch> <%lu ,%lu> for the recovering master",
                             replica.segmentId, backupId.toString().c_str(),
                             replica.segmentId, replica.segmentEpoch,
                             minOpenSegmentId, minOpenSegmentEpoch);
                continue;
            }
        }
        if (i < result.primaryReplicaCount)
            ++newPrimaryReplicaCount;
        newReplicas.emplace_back(replica);
    }
    std::swap(result.replicas, newReplicas);
    std::swap(result.primaryReplicaCount, newPrimaryReplicaCount);

    if (result.logDigestSegmentId < minOpenSegmentId ||
        (result.logDigestSegmentId == minOpenSegmentId &&
         result.logDigestSegmentEpoch < minOpenSegmentEpoch)) {
        RAMCLOUD_LOG(DEBUG,
                     "Backup %s returned a log digest for segment id/epoch "
                     "<%lu, %lu> but the minimum <id, epoch> for this master is "
                     "<%lu, %lu> so discarding it", backupId.toString().c_str(),
                     result.logDigestSegmentId, result.logDigestSegmentEpoch,
                     minOpenSegmentId, minOpenSegmentEpoch);
        result.logDigestBytes = 0;
        result.logDigestBuffer.reset();
        result.logDigestSegmentId = -1;
        result.logDigestSegmentEpoch = -1;
    }
}

void BackupStartTask::wait()
{
    try {
        if (!testingCallback)
            result = rpc->wait();
    } catch (const ServerNotUpException &e) {
        RAMCLOUD_LOG(WARNING,
                     "Couldn't contact %s; server no longer in server list",
                     backupId.toString().c_str());
        // Leave empty result as if the backup has no replicas.
    } catch (const ClientException &e) {
        RAMCLOUD_LOG(WARNING, "startReadingData failed on %s, failure was: %s",
                     backupId.toString().c_str(), e.str().c_str());
        // Leave empty result as if the backup has no replicas.
    }
    rpc.destroy();

    filterOutInvalidReplicas();

    done = true;
    RAMCLOUD_LOG(DEBUG, "Backup %s has %lu segment replicas",
                 backupId.toString().c_str(), result.replicas.size());
}

bool verifyLogComplete(Tub<BackupStartTask> *tasks, size_t taskCount,
                       const LogDigest &digest)
{
    std::unordered_set<uint64_t> replicaSet;
    for (size_t i = 0; i < taskCount; ++i) {
        for (auto replica: tasks[i]->result.replicas)
            replicaSet.insert(replica.segmentId);
    }

    uint32_t missing = 0;
    for (uint32_t i = 0; i < digest.size(); i++) {
        uint64_t id = digest[i];
        if (!contains(replicaSet, id)) {
            if (missing == 0) {
                // Only log the first missing segment; logging them all creates
                // too much log data.
                RAMCLOUD_LOG(NOTICE,
                             "Segment %lu listed in the log digest but not "
                             "found among available backups", id);
            }
            missing++;
        }
    }

    if (missing) {
        RAMCLOUD_LOG(NOTICE,
                     "%u segments in the digest but not available from backups",
                     missing);
    }

    return !missing;
}

BackupStartPartitionTask::BackupStartPartitionTask(Migration *migration,
                                                   ServerId backupServerId)
    : done(), rpc(), backupServerId(backupServerId), migration(migration)
{

}

void BackupStartPartitionTask::send()
{
    RAMCLOUD_LOG(DEBUG, "Sending StartPartitioning: %s",
                 backupServerId.toString().c_str());
    rpc.construct(migration->context, backupServerId, migration->migrationId,
                  migration->sourceServerId, &(migration->dataToMigration));
}

void BackupStartPartitionTask::wait()
{
    try {
        rpc->wait();
        RAMCLOUD_LOG(DEBUG, "Backup %s started partitioning replicas",
                     backupServerId.toString().c_str());
    } catch (const ServerNotUpException &e) {
        RAMCLOUD_LOG(WARNING,
                     "Couldn't contact %s; server no longer in server list",
                     backupServerId.toString().c_str());
    } catch (const ClientException &e) {
        RAMCLOUD_LOG(WARNING, "startPartition failed on %s, failure was: %s",
                     backupServerId.toString().c_str(), e.str().c_str());
    }
    rpc.destroy();

    done = true;
}

Tub<std::tuple<uint64_t, LogDigest, TableStats::Digest *>>
findLogDigest(Tub<BackupStartTask> *tasks, size_t taskCount)
{
    uint64_t headId = ~0ul;
    void *headBuffer = NULL;
    uint32_t headBufferLength = 0;
    TableStats::Digest *tableStatsBuffer = NULL;

    for (size_t i = 0; i < taskCount; ++i) {
        const auto &result = tasks[i]->result;
        if (!result.logDigestBuffer)
            continue;
        if (result.logDigestSegmentId < headId) {
            headBuffer = result.logDigestBuffer.get();
            headBufferLength = result.logDigestBytes;
            headId = result.logDigestSegmentId;
            if (result.tableStatsBytes >= sizeof(TableStats::Digest)) {
                // The buffer is returned as a pointer into an object inside
                // "tasks".  This task object should live throughout the scope
                // of this method's caller.
                tableStatsBuffer = reinterpret_cast<TableStats::Digest *>
                (result.tableStatsBuffer.get());
            }
        }
    }

    if (headId == ~(0lu))
        return {};
    return {std::make_tuple(headId,
                            LogDigest(headBuffer, headBufferLength),
                            tableStatsBuffer)};
}

/// Used in buildReplicaMap().
struct ReplicaAndLoadTime {
    WireFormat::MigrationTargetStart::Replica replica;
    uint64_t expectedLoadTimeMs;

    bool operator<(const ReplicaAndLoadTime &r) const
    {
        return expectedLoadTimeMs < r.expectedLoadTimeMs;
    }
};

vector<WireFormat::MigrationTargetStart::Replica>
buildReplicaMap(Tub<BackupStartTask> *tasks, size_t taskCount,
                MigrationTracker *tracker)
{
    vector<ReplicaAndLoadTime> replicasToSort;
    for (uint32_t taskIndex = 0; taskIndex < taskCount; taskIndex++) {
        const auto &task = tasks[taskIndex];
        const auto backupId = task->backupId;
        const uint64_t speed = (*tracker).getServerDetails(backupId)->
            expectedReadMBytesPerSec;

        RAMCLOUD_LOG(DEBUG, "Adding %lu segment replicas from %s "
                            "with bench speed of %lu",
                     task->result.replicas.size(),
                     backupId.toString().c_str(), speed);

        for (size_t i = 0; i < task->result.replicas.size(); ++i) {
            uint64_t expectedLoadTimeMs;
            if (i < task->result.primaryReplicaCount) {
                // for primaries just estimate when they'll load
                expectedLoadTimeMs = (i + 1) * 8 * 1000 / (speed ?: 1);
            } else {
                // for secondaries estimate when they'll load
                // but add a huge bias so secondaries don't overlap
                // with primaries but are still interleaved
                expectedLoadTimeMs =
                    ((i + 1) - task->result.primaryReplicaCount) *
                    8 * 1000 / (speed ?: 1);
                expectedLoadTimeMs += 1000000;
            }

            const auto &replica = task->result.replicas[i];
            ReplicaAndLoadTime r{{backupId.getId(), replica.segmentId},
                                 expectedLoadTimeMs};
            replicasToSort.push_back(r);

        }
    }

    std::sort(replicasToSort.begin(), replicasToSort.end());
    vector<WireFormat::MigrationTargetStart::Replica> replicaMap;
    for (const auto &sortedReplica: replicasToSort) {
        RAMCLOUD_LOG(DEBUG, "Load segment %lu replica from backup %s "
                            "with expected load time of %lu ms",
                     sortedReplica.replica.segmentId,
                     ServerId(
                         sortedReplica.replica.backupId).toString().c_str(),
                     sortedReplica.expectedLoadTimeMs);
        replicaMap.push_back(sortedReplica.replica);
    }
    return replicaMap;
}

struct MasterStartTask {
    MasterStartTask(
        Migration &recovery,
        ServerId serverId)
        : migration(recovery), serverId(serverId),
          rpc(), done(false),
          testingCallback(recovery.testingMasterStartTaskSendCallback)
    {}

    bool isReady()
    { return testingCallback || (rpc && rpc->isReady()); }

    bool isDone()
    { return done; }

    void send()
    {
        RAMCLOUD_LOG(NOTICE, "Starting migration %lu on master %s",
                     migration.migrationId, serverId.toString().c_str());

        (*migration.tracker)[serverId] = &migration;
        if (!testingCallback) {

            rpc.construct(migration.context,
                          serverId,
                          migration.migrationId,
                          migration.sourceServerId,
                          migration.targetServerId,
                          migration.tableId,
                          migration.firstKeyHash,
                          migration.lastKeyHash);
            if (migration.testingFailRecoveryMasters > 0) {
                    LOG(NOTICE, "Told recovery master %s to kill itself",
                        serverId.toString().c_str());
                --migration.testingFailRecoveryMasters;
            }
        } else {
            testingCallback->masterStartTaskSend(migration.migrationId,
                                                 serverId,
                                                 NULL,
                                                 0);
        }
    }

    void wait()
    {
        try {
            if (!testingCallback)
                rpc->wait();
            done = true;
            return;
        } catch (const ServerNotUpException &e) {
                LOG(WARNING, "Couldn't contact server %s to start recovery: %s",
                    serverId.toString().c_str(), e.what());
        } catch (const ClientException &e) {
                LOG(WARNING, "Couldn't contact server %s to start recovery: %s",
                    serverId.toString().c_str(), e.what());
        }
        migration.migrationFinished(false);
        done = true;
    }

    /// The parent recovery object.
    Migration &migration;

    /// Id of master server to kick off this partition's recovery on.
    ServerId serverId;

    Tub<MigrationSourceStartRpc> rpc;
    bool done;
    MasterStartTaskTestingCallback *testingCallback;
    DISALLOW_COPY_AND_ASSIGN(MasterStartTask);
};

struct BackupEndTask {
    BackupEndTask(Migration &recovery,
                  ServerId serverId,
                  uint64_t migrationId)
        : recovery(recovery), serverId(serverId), migrationId(migrationId),
          rpc(), done(false),
          testingCallback(recovery.testingBackupEndTaskSendCallback)
    {}

    bool isReady()
    { return rpc && rpc->isReady(); }

    bool isDone()
    { return done; }

    void send()
    {
        if (testingCallback) {
            testingCallback->backupEndTaskSend(serverId, migrationId);
            done = true;
            return;
        }
        rpc.construct(recovery.context, serverId, migrationId);
    }

    void wait()
    {
        if (!rpc)
            return;
        try {
            rpc->wait();
        } catch (const ServerNotUpException &e) {
                LOG(DEBUG, "recoveryComplete failed on %s, ignoring; "
                           "server no longer in the servers list",
                    serverId.toString().c_str());
        } catch (const ClientException &e) {
                LOG(DEBUG, "recoveryComplete failed on %s, ignoring; "
                           "failure was: %s", serverId.toString().c_str(),
                    e.what());
        }
        done = true;
    }

    Migration &recovery;
    const ServerId serverId;
    uint64_t migrationId;
    Tub<MigrationCompleteRpc> rpc;
    bool done;
    BackupEndTaskTestingCallback *testingCallback;
    DISALLOW_COPY_AND_ASSIGN(BackupEndTask);
};
}

using namespace MigrationInternal; // NOLINT

void Migration::startBackups()
{

    RAMCLOUD_LOG(DEBUG, "Getting segment lists from backups and preparing "
                        "them for recovery");

    const uint32_t maxActiveBackupHosts = 10;

    std::vector<ServerId> backups =
        tracker->getServersWithService(WireFormat::BACKUP_SERVICE);

    auto backupStartTasks = std::unique_ptr<Tub<BackupStartTask>[]>(
        new Tub<BackupStartTask>[backups.size()]);
    uint32_t i = 0;
    for (ServerId backup: backups) {
        backupStartTasks[i].construct(this, backup);
        i++;
    }

    /* Broadcast 1: start reading replicas from disk and verify log integrity */
    parallelRun(backupStartTasks.get(), backups.size(), maxActiveBackupHosts);

    backupStartTime = Cycles::rdtsc();
    status = START_RECOVERY_MASTERS;
    schedule();
}

void Migration::startMasters()
{
    if (!skipMaster) {
        tableManager->markTabletsMigration(
            sourceServerId, targetServerId, tableId, firstKeyHash, lastKeyHash);
        CycleCounter<RawMetric> _(&metrics->coordinator.recoveryStartTicks);
        RAMCLOUD_LOG(NOTICE,
                     "Starting migration %lu for server %s with %u "
                     "partitions", migrationId,
                     sourceServerId.toString().c_str(),
                     numPartitions);


        auto migrationTasks =
            std::unique_ptr<Tub<MasterStartTask>[]>(
                new Tub<MasterStartTask>[2]);
        migrationTasks[0].construct(*this, sourceServerId);

        parallelRun(migrationTasks.get(), 1, 1);

    }
    if (status > WAIT_FOR_RECOVERY_MASTERS)
        return;
    status = WAIT_FOR_RECOVERY_MASTERS;
    RAMCLOUD_LOG(DEBUG, "Waiting for migration to complete");

}

void Migration::broadcastMigrationComplete()
{
    RAMCLOUD_LOG(DEBUG, "Broadcasting the end of migration %lu to backups",
                 migrationId);
    CycleCounter<RawMetric> ticks(&metrics->coordinator.recoveryCompleteTicks);
    std::vector<ServerId> backups =
        tracker->getServersWithService(WireFormat::BACKUP_SERVICE);
    Tub<BackupEndTask> tasks[backups.size()];
    size_t taskNum = 0;
    for (ServerId backup: backups)
        tasks[taskNum++].construct(*this, backup, migrationId);
    parallelRun(tasks, backups.size(), 10);
}

void Migration::splitTablets(vector<Tablet> *tablets,
                             TableStats::Estimator *estimator)
{

    if (estimator == NULL || !estimator->valid) {
        return;
    }

    size_t size = tablets->size();

    for (size_t i = 0; i < size; ++i) {
        Tablet *tablet = &tablets->at(i);

        if (tableManager->isIndexletTable(tablet->tableId))
            continue;

        TableStats::Estimator::Estimate stats = estimator->estimate(tablet);

        uint64_t startKeyHash = tablet->startKeyHash;
        uint64_t endKeyHash = tablet->endKeyHash;

        uint64_t byteTCount =
            (stats.byteCount + Migration::PARTITION_MAX_BYTES - 1)
            / Migration::PARTITION_MAX_BYTES;
        uint64_t recordTCount =
            (stats.recordCount + Migration::PARTITION_MAX_RECORDS - 1)
            / Migration::PARTITION_MAX_RECORDS;
        uint64_t tabletCount = std::max(byteTCount, recordTCount);

        uint64_t splits = 0;
        if (tabletCount > 0) {
            splits = std::min(endKeyHash - startKeyHash, tabletCount - 1);
        }

        tabletCount = splits + 1;

        if (tabletCount > 1) {
            uint64_t keyRange = (endKeyHash - startKeyHash) / tabletCount;
            uint64_t keyRangeBig = keyRange + 1;

            uint64_t bigCount = ((endKeyHash - startKeyHash) % tabletCount) + 1;

            tablet->endKeyHash = startKeyHash + (keyRangeBig - 1);
            startKeyHash += keyRangeBig;
            uint64_t tabletsComplete = 1;

            // Create all "big" tablets.
            for (; tabletsComplete < bigCount; tabletsComplete++) {
                Tablet temp = tablets->at(i);
                temp.startKeyHash = startKeyHash;
                temp.endKeyHash = startKeyHash + (keyRangeBig - 1);
                tableManager->splitMigrationTablet(temp.tableId, startKeyHash);
                tablets->push_back(temp);
                startKeyHash += keyRangeBig;
            }

            // Create all remaining tablets.
            for (; tabletsComplete < tabletCount; tabletsComplete++) {
                Tablet temp = tablets->at(i);
                temp.startKeyHash = startKeyHash;
                temp.endKeyHash = startKeyHash + (keyRange - 1);
                tableManager->splitMigrationTablet(temp.tableId, startKeyHash);
                tablets->push_back(temp);
                startKeyHash += keyRange;
            }
        }
    }
}

namespace {

struct MigrationPartition {
    uint64_t partitionId;    //< Id used to differentiate tablet partitions
    uint64_t byteCount;      //< Number of bytes assigned to this partition.
    uint64_t recordCount;    //< Number of records assigned to this partition.

    explicit MigrationPartition(uint64_t partitionId)
        : partitionId(partitionId), byteCount(0), recordCount(0)
    {}

    double usage()
    {
        double byte2 = (double(byteCount) * double(byteCount))
                       / (double(Migration::PARTITION_MAX_BYTES)
                          * double(Migration::PARTITION_MAX_BYTES));
        double record2 = (double(recordCount) * double(recordCount))
                         / (double(Migration::PARTITION_MAX_RECORDS)
                            * double(Migration::PARTITION_MAX_RECORDS));
        return sqrt(byte2 + record2) / sqrt(2);
    }

    bool fits(TableStats::Estimator::Estimate estimate)
    {
        if ((byteCount + estimate.byteCount) > Migration::PARTITION_MAX_BYTES)
            return false;
        if ((recordCount + estimate.recordCount) >
            Migration::PARTITION_MAX_RECORDS)
            return false;
        return true;
    }

    void add(TableStats::Estimator::Estimate estimate)
    {
        byteCount += estimate.byteCount;
        recordCount += estimate.recordCount;
    }
};

}

void Migration::partitionTablets(vector<Tablet> tablets,
                                 TableStats::Estimator *estimator)
{
    numPartitions = 0;

    if (estimator == NULL || !estimator->valid) {
        RAMCLOUD_LOG(ERROR,
                     "No valid TableStats Estimator; using naive partitioning.");
        for (auto &tablet: tablets) {
            ProtoBuf::Tablets::Tablet &entry = *dataToMigration.add_tablet();
            tablet.serialize(entry);
            entry.set_user_data(numPartitions++);
        }
        return;
    }

    splitTablets(&tablets, estimator);

    std::vector<MigrationPartition> openPartitions;

    for (Tablet &tablet: tablets) {
        bool done = false;
        size_t numOpenPartitions = openPartitions.size();

        for (size_t i = 0; i < std::min(numOpenPartitions, 5lu); i++) {
            size_t j = generateRandom() % numOpenPartitions;
            MigrationPartition &partition = openPartitions[j];
            if (partition.fits(estimator->estimate(&tablet))) {
                partition.add(estimator->estimate(&tablet));
                ProtoBuf::Tablets::Tablet &entry =
                    *dataToMigration.add_tablet();
                tablet.serialize(entry);
                entry.set_user_data(partition.partitionId);
                // If the partition is mostly full, remove the partition
                if (partition.usage() > 0.9) {
                    partition = openPartitions.back();
                    openPartitions.pop_back();
                }
                done = true;
                break;
            }
        }

        if (!done) {
            // This tablet did not fit in any of the open partitions we tried,
            // so make a new partition.
            MigrationPartition partition(numPartitions++);
            partition.add(estimator->estimate(&tablet));
            ProtoBuf::Tablets::Tablet &entry = *dataToMigration.add_tablet();
            tablet.serialize(entry);
            entry.set_user_data(partition.partitionId);
            // If the partition still has room for more tablets, add it to the
            // available set of partitions.
            if (partition.usage() <= 0.9) {
                openPartitions.push_back(partition);
            }
        }
    }
}

}
