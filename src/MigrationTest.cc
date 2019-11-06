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

#include "TestUtil.h"
#include "Migration.h"
#include "MockExternalStorage.h"

namespace RAMCloud {

using namespace MigrationInternal; // NOLINT

struct MigrationTest : public ::testing::Test {
    typedef MigrationStartReadingRpc::Replica Replica;
    Context context;
    TaskQueue taskQueue;
    MigrationTracker tracker;
    ServerList serverList;
    MockExternalStorage storage;
    CoordinatorUpdateManager manager;
    TableManager tableManager;
    ProtoBuf::MasterRecoveryInfo recoveryInfo;
    std::mutex mutex;

    MigrationTest()
        : context(),
          taskQueue(),
          tracker(&context),
          serverList(&context),
          storage(true),
          manager(&storage),
          tableManager(&context, &manager),
          recoveryInfo(),
          mutex()
    {
        Logger::get().setLogLevels(SILENT_LOG_LEVEL);
    }

    void
    addServersToTracker(size_t count, ServiceMask services)
    {
        for (uint32_t i = 1; i < count + 1; ++i) {
            string locator = format("mock:host=server%u", i);
            tracker.enqueueChange({{i, 0}, locator, services,
                                   100, ServerStatus::UP}, SERVER_ADDED);
        }
        ServerDetails _;
        ServerChangeEvent __;
        while (tracker.getChange(_, __));
    }

    typedef std::unique_lock<std::mutex> Lock;

  private:
    DISALLOW_COPY_AND_ASSIGN(MigrationTest);
};
namespace {
void
populateLogDigest(MigrationStartReadingRpc::Result &result,
                  uint64_t segmentId,
                  std::vector<uint64_t> segmentIds)
{
    // Doesn't matter for these tests.
    LogDigest digest;
        foreach (uint64_t id, segmentIds)digest.addSegmentId(id);

    Buffer buffer;
    digest.appendToBuffer(buffer);
    result.logDigestSegmentEpoch = 100;
    result.logDigestSegmentId = segmentId;
    result.logDigestBytes = buffer.size();
    result.logDigestBuffer =
        std::unique_ptr<char[]>(new char[result.logDigestBytes]);
    buffer.copy(0, buffer.size(), result.logDigestBuffer.get());
}
} // namespace


TEST_F(MigrationTest, splitTabletsNoEstimator)
{
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    uint64_t firstKeyHash = 0;
    uint64_t lastKeyHash = 9;
    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, firstKeyHash, lastKeyHash, sourceId, Tablet::NORMAL, {}});
    tableManager.testAddTablet(
        {tableId, lastKeyHash + 1, lastKeyHash + 10, sourceId, Tablet::NORMAL,
         {}});

    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, firstKeyHash, lastKeyHash,
                        recoveryInfo);

    auto tablets = tableManager.markTabletsMigration(
        sourceId, targetId, tableId,
        firstKeyHash, lastKeyHash);

    EXPECT_EQ(1u, tablets.size());

    migration->splitTablets(&tablets, NULL);

    Tablet *tablet = NULL;

    EXPECT_EQ(1u, tablets.size());

    EXPECT_EQ("{ 1: 0x0-0x9 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x0-0x9 }", tablet->debugString(1));
    }
    tablet = NULL;
}

TEST_F(MigrationTest, splitTabletsBadEstimator)
{
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> recovery;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    uint64_t firstKeyHash = 0;
    uint64_t lastKeyHash = 9;
    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, firstKeyHash, lastKeyHash, sourceId, Tablet::NORMAL, {}});

    recovery.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, firstKeyHash, lastKeyHash,
                       recoveryInfo);
    auto tablets = tableManager.markTabletsMigration(
        sourceId, targetId, tableId,
        firstKeyHash, lastKeyHash);


    TableStats::Estimator e(NULL);

    EXPECT_FALSE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    Tablet *tablet = NULL;

    EXPECT_EQ(1u, tablets.size());

    EXPECT_EQ("{ 1: 0x0-0x9 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 0u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x0-0x9 }", tablet->debugString(1));
    }
    tablet = NULL;
}

TEST_F(MigrationTest, splitTabletsMultiTablet)
{
    // Case where there is more than one tablet.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet({tableId, 0, 9, sourceId, Tablet::NORMAL, {}});
    tableManager.testAddTablet({tableId, 10, 29, sourceId, Tablet::NORMAL, {}});
    tableManager.testAddTablet({tableId, 30, 49, sourceId, Tablet::NORMAL, {}});
    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 29, recoveryInfo);
    auto tablets = tableManager.markTabletsMigration(sourceId, targetId,
                                                     tableId, 0, 29);

    char buffer[sizeof(TableStats::DigestHeader) +
                3 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 3;
    digest->header.otherBytesPerKeyHash = 0;
    digest->header.otherRecordsPerKeyHash = 0;
    digest->entries[0].tableId = 1;
    digest->entries[0].bytesPerKeyHash = Migration::PARTITION_MAX_BYTES / 10;
    digest->entries[0].recordsPerKeyHash =
        Migration::PARTITION_MAX_RECORDS / 10;

    digest->entries[1].tableId = 2;
    digest->entries[1].bytesPerKeyHash = Migration::PARTITION_MAX_BYTES / 10;
    digest->entries[1].recordsPerKeyHash =
        Migration::PARTITION_MAX_RECORDS / 10;

    digest->entries[2].tableId = 3;
    digest->entries[2].bytesPerKeyHash = Migration::PARTITION_MAX_BYTES / 10;
    digest->entries[2].recordsPerKeyHash =
        Migration::PARTITION_MAX_RECORDS / 10;

    TableStats::Estimator e(digest);

    EXPECT_TRUE(e.valid);

    EXPECT_EQ(2u, tablets.size());
    migration->splitTablets(&tablets, &e);
    EXPECT_EQ(3u, tablets.size());
}

TEST_F(MigrationTest, splitTabletsBasic)
{
    // Tests that a basic split of a single tablet will execute and update the
    // TableManager tablet information.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t keyHashCount = 5;
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, 1, keyHashCount, sourceId, Tablet::NORMAL, {}});
    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 1, keyHashCount,
                        recoveryInfo);

    auto tablets = tableManager.markTabletsMigration(sourceId, targetId,
                                                     tableId, 1, keyHashCount);

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (3.0 / double(keyHashCount))
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (3.0 / double(keyHashCount))
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    migration->splitTablets(&tablets, &e);

    Tablet *tablet = NULL;

    EXPECT_EQ(3u, tablets.size());

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x1-0x2 }", tablets[0].debugString(1));
    tablet = tableManager.testFindTablet(1u, 1u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x1-0x2 }", tablet->debugString(1));
    }
    tablet = NULL;

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x3-0x4 }", tablets[1].debugString(1));
    tablet = tableManager.testFindTablet(1u, 3u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x3-0x4 }", tablet->debugString(1));
    }
    tablet = NULL;

    // Check Tablet Contents
    EXPECT_EQ("{ 1: 0x5-0x5 }", tablets[2].debugString(1));
    tablet = tableManager.testFindTablet(1u, 5u);
    EXPECT_TRUE(tablet != NULL);
    if (tablet != NULL) {
        EXPECT_EQ("{ 1: 0x5-0x5 }", tablet->debugString(1));
    }
    tablet = NULL;
}

TEST_F(MigrationTest, splitTabletsByteDominated)
{
    // Ensure the tablet count can be determined by the byte count.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> recovery;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t keyHashCount = 1000;
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, 1, keyHashCount, sourceId, Tablet::NORMAL, {}});
    recovery.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, 1, keyHashCount,
                       recoveryInfo);
    auto tablets = tableManager.markTabletsMigration(sourceId, targetId,
                                                     tableId, 1, keyHashCount);

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (6.0 / double(keyHashCount))
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (2.0 / double(keyHashCount))
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(6u, tablets.size());
}

TEST_F(MigrationTest, splitTabletsRecordDominated)
{
    // Ensure the tablet count can be determined by the record count.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> recovery;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t keyHashCount = 1000;
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet(
        {tableId, 1, keyHashCount, {99, 0}, Tablet::NORMAL, {}});
    recovery.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                       sourceId, targetId, tableId, 1, keyHashCount,
                       recoveryInfo);
    auto tablets = tableManager.markTabletsMigration(sourceId, targetId,
                                                     tableId, 1, keyHashCount);

    char buffer[sizeof(TableStats::DigestHeader)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (2.0 / double(keyHashCount))
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (6.0 / double(keyHashCount))
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);
    EXPECT_TRUE(e.valid);

    EXPECT_EQ(1u, tablets.size());

    recovery->splitTablets(&tablets, &e);

    EXPECT_EQ(6u, tablets.size());
}

TEST_F(MigrationTest, partitionTabletsNoEstimator)
{
    Lock lock(mutex);     // To trick TableManager internal calls.
    uint64_t tableId = 123;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);

    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 30, recoveryInfo);
    auto tablets = tableManager.markTabletsMigration(sourceId, targetId,
                                                     tableId, 0, 30);
    migration->partitionTablets(tablets, NULL);
    EXPECT_EQ(0lu, migration->numPartitions);

    tableManager.testCreateTable("t", tableId);
    tableManager.testAddTablet(
        {tableId, 0, 9, sourceId, Tablet::RECOVERING, {}});
    tableManager.testAddTablet(
        {tableId, 20, 29, sourceId, Tablet::RECOVERING, {}});
    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 30, recoveryInfo);
    tablets = tableManager.markAllTabletsRecovering(ServerId(99));
    migration->partitionTablets(tablets, NULL);
    EXPECT_EQ(2lu, migration->numPartitions);

    tableManager.testAddTablet(
        {tableId, 10, 19, sourceId, Tablet::RECOVERING, {}});
    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 30, recoveryInfo);
    tablets = tableManager.markAllTabletsRecovering(ServerId(99));
    migration->partitionTablets(tablets, NULL);
    EXPECT_EQ(3lu, migration->numPartitions);
}

TEST_F(MigrationTest, partitionTabletsSplits)
{
    Lock lock(mutex);     // To trick TableManager internal calls.
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);

    tableManager.testCreateTable("t1", tableId);
    tableManager.testAddTablet({tableId, 0, 99, sourceId, Tablet::NORMAL, {}});
    tableManager.testAddTablet(
        {tableId, 100, 199, sourceId, Tablet::NORMAL, {}});

    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 199, recoveryInfo);
    auto tablets = tableManager.markTabletsMigration(sourceId, targetId,
                                                     tableId, 0, 199);

    char buffer[sizeof(TableStats::DigestHeader) +
                2 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 2;
    digest->header.otherBytesPerKeyHash = 0;
    digest->header.otherRecordsPerKeyHash = 0;
    digest->entries[0].tableId = 1;
    digest->entries[0].bytesPerKeyHash = (10.0 / 100)
                                         * Migration::PARTITION_MAX_BYTES;
    digest->entries[0].recordsPerKeyHash = (1.0 / 100)
                                           * Migration::PARTITION_MAX_RECORDS;
    digest->entries[1].tableId = 2;
    digest->entries[1].bytesPerKeyHash = (1.0 / 100)
                                         * Migration::PARTITION_MAX_BYTES;
    digest->entries[1].recordsPerKeyHash = (10.0 / 100)
                                           * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);

    migration->partitionTablets(tablets, &e);
    EXPECT_EQ(20lu, migration->numPartitions);
}

TEST_F(MigrationTest, partitionTabletsBasic)
{
    // This covers the following cases:
    //      (1) No partitions to choose from
    //      (2) Single partiton to choose from
    //      (3) Evicting a partition
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    for (uint64_t i = 0; i < 250; i++) {
        tableManager.testAddTablet(
            {tableId, i * 10, i * 10 + 9, sourceId, Tablet::NORMAL, {}});
    }
    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 2499, recoveryInfo);
    auto tablets = tableManager.markTabletsMigration(sourceId, targetId,
                                                     tableId, 0, 2499);

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (0.01 / 10)
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (0.01 / 10)
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);

    migration->partitionTablets(tablets, &e);
    EXPECT_EQ(3lu, migration->numPartitions);
}

TEST_F(MigrationTest, partitionTabletsAllPartitionsOpen)
{
    // Covers the addtional case where no partitions are large enough to be
    // evicted, there is more than one partition to choose from and none will
    // fit.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    for (uint64_t i = 0; i < 20; i++) {
        tableManager.testAddTablet(
            {tableId, i * 10, i * 10 + 9, sourceId, Tablet::NORMAL, {}});
    }
    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 199, recoveryInfo);
    auto tablets = tableManager.markTabletsMigration(sourceId, targetId,
                                                     tableId, 0, 199);

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (0.6 / 10)
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (0.6 / 10)
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);

    migration->partitionTablets(tablets, &e);
    EXPECT_EQ(20lu, migration->numPartitions);
}

bool
tabletCompare(const Tablet &a,
              const Tablet &b)
{
    if (a.tableId != b.tableId)
        return a.tableId < b.tableId;
    else
        return a.startKeyHash < b.startKeyHash;
}

TEST_F(MigrationTest, partitionTabletsMixed)
{
    // Covers the addtional case where partitions will contain one large tablet
    // and filled by many smaller ones.  Case covers selecting from multiple
    // partitons and having a tablet fit.
    Lock lock(mutex);     // To trick TableManager internal calls.
    Tub<Migration> migration;
    Migration::Owner *own = static_cast<Migration::Owner *>(NULL);
    uint64_t tableId = 1;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};

    tableManager.testCreateTable("t1", tableId);
    for (uint64_t i = 0; i < 6; i++) {
        tableManager.testAddTablet(
            {tableId, i * 600, i * 600 + 599, sourceId, Tablet::NORMAL, {}});
    }
    for (uint64_t i = 0; i < 180; i++) {
        tableManager.testAddTablet(
            {tableId, i * 10 + 3600, i * 10 + 3609,
             sourceId, Tablet::NORMAL, {}});
    }
    migration.construct(&context, taskQueue, 1, &tableManager, &tracker, own,
                        sourceId, targetId, tableId, 0, 5399, recoveryInfo);
    vector<Tablet> tablets =
        tableManager.markTabletsMigration(sourceId, targetId, tableId, 0, 5399);

    char buffer[sizeof(TableStats::DigestHeader) +
                0 * sizeof(TableStats::DigestEntry)];
    TableStats::Digest *digest = reinterpret_cast<TableStats::Digest *>(buffer);
    digest->header.entryCount = 0;
    digest->header.otherBytesPerKeyHash = (0.6 / 600)
                                          * Migration::PARTITION_MAX_BYTES;
    digest->header.otherRecordsPerKeyHash = (0.6 / 600)
                                            * Migration::PARTITION_MAX_RECORDS;

    TableStats::Estimator e(digest);

    // Tablets need to be sorted because the list returned from
    // markAllTabletsRecovering is built from unordered_map::iterator, which
    // means that the ordering can change from version to version of gcc.
    std::sort(tablets.begin(), tablets.end(), tabletCompare);

    migration->partitionTablets(tablets, &e);
    EXPECT_EQ(6lu, migration->numPartitions);
}


TEST_F(MigrationTest, startBackups)
{
    /**
     * Called by BackupStartTask instead of sending the startReadingData
     * RPC. The callback mocks out the result of the call for testing.
     * Each call into the callback corresponds to the send of the RPC
     * to an individual backup.
     */
    struct Cb : public BackupStartTask::TestingCallback {
        int callCount;

        Cb() : callCount()
        {}

        void backupStartTaskSend(MigrationStartReadingRpc::Result &result)
        {
            if (callCount == 0) {
                // Two segments on backup1, one that overlaps with backup2
                // Includes a segment digest
                result.replicas.push_back(Replica{88lu, 100lu, false});
                result.replicas.push_back(Replica{89lu, 100lu, true});
                populateLogDigest(result, 89, {88, 89});
                result.primaryReplicaCount = 1;
            } else if (callCount == 1) {
                // One segment on backup2
                result.replicas.push_back(Replica{88lu, 100lu, false});
                result.primaryReplicaCount = 1;
            } else if (callCount == 2) {
                // No segments on backup3
            }
            callCount++;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    uint64_t tableId = 123;
    ServerId sourceId = {99, 0};
    ServerId targetId = {100, 0};


    tableManager.testCreateTable("t", tableId);
    tableManager.testAddTablet({tableId, 10, 19, sourceId, Tablet::NORMAL, {}});
    Migration recovery(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                       sourceId, targetId, tableId, 10, 19, recoveryInfo);
    recovery.testingBackupStartTaskSendCallback = &callback;
    recovery.startBackups();
    EXPECT_EQ((vector<WireFormat::MigrationTargetStart::Replica>{
        {1, 88},
        {2, 88},
        {1, 89},
    }), recovery.replicaMap);
}

TEST_F(MigrationTest, startBackupsFailureContactingSomeBackup)
{
    Migration recovery(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                       ServerId(99), ServerId(100), 123, 0, 9, recoveryInfo);
    BackupStartTask task(&recovery, {2, 0});
    EXPECT_NO_THROW(task.send());
}

TEST_F(MigrationTest, startBackupsSecondariesEarlyInSomeList)
{
    // See buildReplicaMap test for info about how the callback is used.
    struct Cb : public BackupStartTask::TestingCallback {
        int callCount;

        Cb() : callCount()
        {}

        void backupStartTaskSend(MigrationStartReadingRpc::Result &result)
        {
            if (callCount == 0) {
                result.replicas.push_back(Replica{88lu, 100u, false});
                result.replicas.push_back(Replica{89lu, 100u, false});
                result.replicas.push_back(Replica{90lu, 100u, false});
                result.primaryReplicaCount = 3;
            } else if (callCount == 1) {
                result.replicas.push_back(Replica{88lu, 100u, false});
                result.replicas.push_back(Replica{91lu, 100u, true});
                populateLogDigest(result, 91, {88, 89, 90, 91});
                result.primaryReplicaCount = 1;
            } else if (callCount == 2) {
                result.replicas.push_back(Replica{91lu, 100u, true});
                result.primaryReplicaCount = 1;
            }
            callCount++;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(99), ServerId(100), 123, 10, 19, recoveryInfo);
    migration.testingBackupStartTaskSendCallback = &callback;
    migration.startBackups();
    ASSERT_EQ(6U, migration.replicaMap.size());
    // The secondary of segment 91 must be last in the list.
    EXPECT_EQ(91U, migration.replicaMap.at(5).segmentId);
}

TEST_F(MigrationTest, startBackups_noLogDigestFound)
{
    BackupStartTask::TestingCallback callback; // No-op callback.
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(99), ServerId(100), 123, 10, 19, recoveryInfo);
    migration.testingBackupStartTaskSendCallback = &callback;
    TestLog::Enable _("startBackups");
    migration.startBackups();
    EXPECT_EQ(
        "startBackups: Getting segment lists from backups and preparing "
        "them for recovery | "
        "startBackups: No log digest among replicas on available backups. "
        "Will retry recovery later.", TestLog::get());
}

TEST_F(MigrationTest, startBackups_someReplicasMissing)
{
    // See buildReplicaMap test for info about how the callback is used.
    struct Cb : public BackupStartTask::TestingCallback {
        void backupStartTaskSend(MigrationStartReadingRpc::Result &result)
        {
            populateLogDigest(result, 91, {91});
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::RECOVERING, {}});
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(99), ServerId(100), 123, 10, 19, recoveryInfo);
    migration.testingBackupStartTaskSendCallback = &callback;
    TestLog::Enable _("startBackups");
    migration.startBackups();
    EXPECT_EQ(
        "startBackups: Getting segment lists from backups and preparing "
        "them for recovery | "
        "startBackups: Segment 91 is the head of the log | "
        "startBackups: Some replicas from log digest not on available backups. "
        "Will retry recovery later.", TestLog::get());
}

TEST_F(MigrationTest, BackupStartTask_filterOutInvalidReplicas)
{
    recoveryInfo.set_min_open_segment_id(10);
    recoveryInfo.set_min_open_segment_epoch(1);
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(99), ServerId(100), 123, 10, 19, recoveryInfo);
    BackupStartTask task(&migration, {2, 0});
    auto &segments = task.result.replicas;
    segments = {
        {2,  1, true},
        {2,  0, false},
        {10, 1, false},
        {10, 0, true},
        {10, 0, false},
        {11, 1, true},
        {11, 0, false},
    };

    task.result.primaryReplicaCount = 2;
    uint32_t bytes = 4; // Doesn't really matter.
    task.result.logDigestBytes = bytes;
    task.result.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    task.result.logDigestSegmentId = 9;
    task.result.logDigestSegmentEpoch = 1;
    task.filterOutInvalidReplicas();
    ASSERT_EQ(5lu, segments.size());
    EXPECT_EQ((vector<MigrationStartReadingRpc::Replica>{
        {2,  1, true},
        {10, 1, false},
        {10, 0, true},
        {11, 1, true},
        {11, 0, false},
    }), segments);
    EXPECT_EQ(1lu, task.result.primaryReplicaCount);
    EXPECT_EQ(0u, task.result.logDigestBytes);
    EXPECT_EQ(static_cast<char *>(NULL), task.result.logDigestBuffer.get());
    EXPECT_EQ(~0lu, task.result.logDigestSegmentId);
    EXPECT_EQ(~0lu, task.result.logDigestSegmentEpoch);
}

TEST_F(MigrationTest, verifyLogComplete)
{
    LogDigest digest;
    digest.addSegmentId(10);
    digest.addSegmentId(11);
    digest.addSegmentId(12);


    Tub<BackupStartTask> tasks[1];
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(99), ServerId(100), 123, 10, 19, recoveryInfo);
    tasks[0].construct(&migration, ServerId(2, 0));
    auto &segments = tasks[0]->result.replicas;

    TestLog::Enable _;
    segments = {{10, 0, false},
                {12, 0, true}};
    EXPECT_FALSE(verifyLogComplete(tasks, 1, digest));
    EXPECT_EQ(
        "verifyLogComplete: Segment 11 listed in the log digest but "
        "not found among available backups | "
        "verifyLogComplete: 1 segments in the digest but not available "
        "from backups", TestLog::get());
    segments = {{10, 0, false},
                {11, 0, false},
                {12, 0, true}};
    EXPECT_TRUE(verifyLogComplete(tasks, 1, digest));
}

TEST_F(MigrationTest, findLogDigest)
{
    recoveryInfo.set_min_open_segment_id(10);
    recoveryInfo.set_min_open_segment_epoch(1);
    Tub<BackupStartTask> tasks[2];
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(99), ServerId(100), 123, 10, 19, recoveryInfo);
    tasks[0].construct(&migration, ServerId(2, 0));
    tasks[1].construct(&migration, ServerId(3, 0));

    // No log digest found.
    auto digest = findLogDigest(tasks, 2);
    EXPECT_FALSE(digest);

    auto &result0 = tasks[0]->result;
    auto &result1 = tasks[1]->result;

    // Two digests with different contents to differentiate them below.
    LogDigest result0Digest, result1Digest;
    result0Digest.addSegmentId(0);
    result1Digest.addSegmentId(1);

    Buffer result0Buffer, result1Buffer;
    result0Digest.appendToBuffer(result0Buffer);
    result1Digest.appendToBuffer(result1Buffer);

    uint32_t bytes = result0Buffer.size();

    result0.logDigestBytes = bytes;
    result0.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    result0.logDigestSegmentId = 10;
    result0Buffer.copy(0, bytes, result0.logDigestBuffer.get());

    result1.logDigestBytes = bytes;
    result1.logDigestBuffer = std::unique_ptr<char[]>(new char[bytes]);
    result1.logDigestSegmentId = 10;
    result1Buffer.copy(0, bytes, result1.logDigestBuffer.get());

    // Two log digests, same segment id, keeps earlier of two.
    digest = findLogDigest(tasks, 2);
    ASSERT_TRUE(digest);
    EXPECT_EQ(10lu, std::get<0>(*digest.get()));
    EXPECT_EQ(0u, std::get<1>(*digest.get())[0]);

    result1.logDigestSegmentId = 9;
    // Two log digests, later one has a lower segment id.
    digest = findLogDigest(tasks, 2);
    ASSERT_TRUE(digest);
    EXPECT_EQ(9lu, std::get<0>(*digest.get()));
    EXPECT_EQ(1u, std::get<1>(*digest.get())[0]);
}

TEST_F(MigrationTest, buildReplicaMap)
{
    Tub<BackupStartTask> tasks[2];
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(99), ServerId(100), 123, 10, 19, recoveryInfo);
    tasks[0].construct(&migration, ServerId(2, 0));
    auto *result = &tasks[0]->result;
    result->replicas.push_back(Replica{88lu, 100u, false});
    result->replicas.push_back(Replica{89lu, 100u, false});
    result->replicas.push_back(Replica{90lu, 100u, false});
    result->primaryReplicaCount = 3;

    tasks[1].construct(&migration, ServerId(3, 0));
    result = &tasks[1]->result;
    result->replicas.push_back(Replica{88lu, 100u, false});
    result->replicas.push_back(Replica{91lu, 100u, false});
    result->primaryReplicaCount = 1;

    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});

    auto replicaMap = buildReplicaMap(tasks, 2, &tracker);
    EXPECT_EQ((vector<WireFormat::MigrationTargetStart::Replica>{
        {2, 88},
        {3, 88},
        {2, 89},
        {2, 90},
        {3, 91},
    }), replicaMap);

    tracker.getServerDetails({3, 0})->expectedReadMBytesPerSec = 101;
    TestLog::Enable _;
    replicaMap = buildReplicaMap(tasks, 2, &tracker);
    EXPECT_EQ((vector<WireFormat::MigrationTargetStart::Replica>{
        {3, 88},
        {2, 88},
        {2, 89},
        {2, 90},
        {3, 91},
    }), replicaMap);
}

TEST_F(MigrationTest, buildReplicaMap_badReplicas)
{
    Tub<BackupStartTask> tasks[1];
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(99), ServerId(100), 123, 10, 19, recoveryInfo);
    tasks[0].construct(&migration, ServerId(2, 0));
    auto *result = &tasks[0]->result;
    result->replicas.push_back(Replica{92lu, 100u, true}); // beyond head
    result->primaryReplicaCount = 1;

    addServersToTracker(2, {WireFormat::BACKUP_SERVICE});

    auto replicaMap = buildReplicaMap(tasks, 1, &tracker);
    EXPECT_EQ((vector<WireFormat::MigrationTargetStart::Replica>()),
              replicaMap);
}

TEST_F(MigrationTest, startMaster)
{
    MockRandom _(1);
    struct Cb : public MasterStartTaskTestingCallback {
        int callCount;

        Cb() : callCount()
        {}

        void masterStartTaskSend(
            uint64_t migrationId, ServerId targetServerId,
            const WireFormat::MigrationTargetStart::Replica replicaMap[],
            size_t replicaMapSize)
        {
            if (callCount == 0) {
                EXPECT_EQ(1lu, migrationId);
                EXPECT_EQ(ServerId(1, 0), targetServerId);
            } else if (callCount == 1) {
                EXPECT_EQ(1lu, migrationId);
                EXPECT_EQ(ServerId(2, 0), targetServerId);
            } else {
                FAIL();
            }
            ++callCount;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(2, {WireFormat::MASTER_SERVICE});
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 0, 9, {99, 0}, Tablet::NORMAL, {}});
    tableManager.testAddTablet({123, 20, 29, {99, 0}, Tablet::NORMAL, {}});
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::NORMAL, {}});
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(1, 0), ServerId(2, 0), 123, 10, 19,
                        recoveryInfo);
    migration.partitionTablets(
        tableManager.markTabletsMigration({1, 0}, {2, 0}, 123, 10, 19), NULL);
    // Hack 'tablets' to get the first two tablets on the same server.
    migration.testingMasterStartTaskSendCallback = &callback;
    migration.startMasters();

    EXPECT_EQ(0, migration.migrateSuccessful);
}

TEST_F(MigrationTest, startRecoveryMasters_tooFewIdleMasters)
{
    MockRandom _(1);
    struct Cb : public MasterStartTaskTestingCallback {
        int callCount;

        Cb() : callCount()
        {}

        void masterStartTaskSend(
            uint64_t migrationId, ServerId targetServerId,
            const WireFormat::MigrationTargetStart::Replica replicaMap[],
            size_t replicaMapSize)
        {
            if (callCount == 0) {
                EXPECT_EQ(1lu, migrationId);
                EXPECT_EQ(ServerId(1, 0), targetServerId);
            } else {
                EXPECT_EQ(1lu, migrationId);
                EXPECT_EQ(ServerId(2, 0), targetServerId);
            }
            ++callCount;
        }
    } callback;
    Lock lock(mutex);     // To trick TableManager internal calls.
    addServersToTracker(2, {WireFormat::MASTER_SERVICE});
    tracker[ServerId(1, 0)] = reinterpret_cast<Migration *>(0x1);
    tableManager.testCreateTable("t", 123);
    tableManager.testAddTablet({123, 0, 9, {99, 0}, Tablet::NORMAL, {}});
    tableManager.testAddTablet({123, 20, 29, {99, 0}, Tablet::NORMAL, {}});
    tableManager.testAddTablet({123, 10, 19, {99, 0}, Tablet::NORMAL, {}});
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(1, 0), ServerId(2, 0), 123, 10, 19,
                        recoveryInfo);
    // Hack 'tablets' to get the first two tablets on the same server.
    migration.testingMasterStartTaskSendCallback = &callback;
    migration.startMasters();

    migration.migrationFinished(true);

    EXPECT_EQ(1, migration.migrateSuccessful);
    EXPECT_TRUE(migration.wasCompletelySuccessful());
}

TEST_F(MigrationTest, broadcastRecoveryComplete)
{
    addServersToTracker(3, {WireFormat::BACKUP_SERVICE});
    struct Cb : public BackupEndTaskTestingCallback {
        int callCount;

        Cb() : callCount()
        {}

        void backupEndTaskSend(ServerId backupId, uint64_t migrationId)
        {
            ++callCount;
        }
    } callback;
    Migration migration(&context, taskQueue, 1, &tableManager, &tracker, NULL,
                        ServerId(1, 0), ServerId(2, 0), 123, 10, 19,
                        recoveryInfo);
    migration.testingBackupEndTaskSendCallback = &callback;
    migration.broadcastMigrationComplete();
    EXPECT_EQ(3, callback.callCount);
}
}
