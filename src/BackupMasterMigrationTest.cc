
#include "TestUtil.h"
#include "StringUtil.h"
#include "BackupMasterMigration.h"
#include "InMemoryStorage.h"
#include "TabletsBuilder.h"

namespace RAMCloud {

struct BackupMasterMigrationTest : public ::testing::Test {
    TaskQueue taskQueue;
    uint32_t segmentSize;
    uint32_t readSpeed;
    uint32_t maxReplicasInMemory;
    InMemoryStorage storage;
    std::vector<BackupStorage::FrameRef> frames;
    Buffer source;
    ServerId sourceServerId;
    ServerId targetServerId;
    uint64_t tableId;
    uint64_t firstKeyHash;
    uint64_t lastKeyHash;

    Tub<BackupMasterMigration> migration;

    BackupMasterMigrationTest()
        : taskQueue(), segmentSize(1024), readSpeed(100),
          maxReplicasInMemory(4), storage(segmentSize, 6, 0), frames(),
          source(), sourceServerId(99, 0), targetServerId(100, 0),
          tableId(1), firstKeyHash(0lu), lastKeyHash(~0lu), migration()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
        ProtoBuf::Tablets tablets;
        TabletsBuilder{tablets}
            (1, 0lu, 10lu, TabletsBuilder::NORMAL, 0lu)  // partition 0
            (1, 11lu, ~0lu, TabletsBuilder::NORMAL, 1lu)  // partition 1
            (2, 0lu, ~0lu, TabletsBuilder::NORMAL, 0lu)  // partition 0
            (3, 0lu, ~0lu, TabletsBuilder::NORMAL, 0lu); // partition 0
        source.appendExternal("test", 5);
        migration.construct(taskQueue, 456lu, sourceServerId, targetServerId,
                            tableId, firstKeyHash, lastKeyHash,
                            segmentSize, readSpeed, maxReplicasInMemory);
    }

    void
    mockMetadata(uint64_t segmentId,
                 bool closed = true, bool primary = false,
                 bool screwItUp = false)
    {
        frames.emplace_back(storage.open(true, ServerId(), 0));
        SegmentCertificate certificate;
        uint32_t epoch = downCast<uint32_t>(segmentId) + 100;
        BackupReplicaMetadata metadata(certificate, sourceServerId.getId(),
                                       segmentId, 1024, epoch,
                                       closed, primary);
        if (screwItUp)
            metadata.checksum = 0;
        frames.back()->append(source, 0, 0, 0, &metadata, sizeof(metadata));
    }

    void
    setupReplicaBuffer(bool readyToEvict)
    {
        for (uint32_t i = 0; i < maxReplicasInMemory + 1; i++) {
            mockMetadata(i, true, true); // primary
        }
        mockMetadata(88); // secondary

        // Enqueue the primary replicas
        migration->start(frames, NULL, NULL);
        migration->setPartitionsAndSchedule();

        BackupMasterMigration::CyclicReplicaBuffer *replicaBuffer =
            &migration->replicaBuffer;

        // Buffer the primary replicas
        for (uint32_t i = 0; i < maxReplicasInMemory; i++) {
            replicaBuffer->bufferNext();
        }
        ASSERT_EQ(replicaBuffer->inMemoryReplicas.size(), maxReplicasInMemory);
        ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 1u);

        // Enqueue the secondary replica
        EXPECT_THROW(migration->getRecoverySegment(456, 88, NULL, NULL),
                     RetryException);
        ASSERT_EQ(replicaBuffer->highPriorityQueuedReplicas.size(), 1u);

        if (readyToEvict) {
            BackupMasterMigration::Replica *firstReplica =
                replicaBuffer->inMemoryReplicas[0];
            firstReplica->built = true;
            firstReplica->fetchCount = 1;
            firstReplica->lastAccessTime = 0;
            Cycles::mockTscValue =
                Cycles::fromSeconds(replicaBuffer->bufferReadTime / 2 + 1e-9);
        }
    }

    DISALLOW_COPY_AND_ASSIGN(BackupMasterMigrationTest);
};

namespace {
bool mockExtractDigest(uint64_t segmentId, Buffer *digestBuffer,
                       Buffer *tableStatsBuffer)
{
    if (segmentId == 92lu) {
        digestBuffer->reset();
        digestBuffer->appendExternal("digest", 7);
        tableStatsBuffer->reset();
        tableStatsBuffer->appendExternal("tableStats", 11);
        return true;
    }
    if (segmentId == 93lu) {
        digestBuffer->reset();
        digestBuffer->appendExternal("not digest", 11);
        tableStatsBuffer->reset();
        tableStatsBuffer->appendExternal("not tableStats", 15);
        return true;
    }
    return false;
}
}


TEST_F(BackupMasterMigrationTest, start)
{
    TestLog::Enable _;
    mockMetadata(88, true, true);
    mockMetadata(89);
    mockMetadata(90, true, false, true); // Screwed up metadata.
    mockMetadata(91, true, true);
    mockMetadata(93, false);
    mockMetadata(92, false);
    migration->testingExtractDigest = &mockExtractDigest;
    Buffer buffer;
    auto response = buffer.emplaceAppend<BackupMasterMigration::StartResponse>();
    migration->start(frames, &buffer, response);
    migration->setPartitionsAndSchedule();

    ASSERT_EQ(5u, response->replicaCount);
    EXPECT_EQ(2u, response->primaryReplicaCount);
    // Make sure we got the "lowest" log digest.
    EXPECT_EQ(7u, response->digestBytes);
    EXPECT_EQ(92lu, response->digestSegmentId);
    EXPECT_EQ(192u, response->digestSegmentEpoch);
    EXPECT_EQ(11u, response->tableStatsBytes);

    buffer.truncateFront(sizeof32(BackupMasterRecovery::StartResponse));
    // Verify returned segment ids and lengths.
    typedef WireFormat::BackupStartReadingData::Replica WireReplica;
    const WireReplica *replica = buffer.getStart<WireReplica>();
    EXPECT_TRUE(88lu == replica->segmentId || 91lu == replica->segmentId);
    EXPECT_TRUE(188lu == replica->segmentEpoch ||
                191lu == replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    replica = buffer.getStart<WireReplica>();
    EXPECT_TRUE(88lu == replica->segmentId || 91lu == replica->segmentId);
    EXPECT_TRUE(188lu == replica->segmentEpoch ||
                191lu == replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    replica = buffer.getStart<WireReplica>();
    EXPECT_EQ(89lu, replica->segmentId);
    EXPECT_EQ(189lu, replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    replica = buffer.getStart<WireReplica>();
    EXPECT_EQ(93lu, replica->segmentId);
    EXPECT_EQ(193lu, replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    replica = buffer.getStart<WireReplica>();
    EXPECT_EQ(92lu, replica->segmentId);
    EXPECT_EQ(192lu, replica->segmentEpoch);
    buffer.truncateFront(sizeof32(WireReplica));
    EXPECT_STREQ("digest", buffer.getStart<char>());

    // Primaries come first; random order.
    EXPECT_TRUE(migration->replicas[0].metadata->primary);
    EXPECT_TRUE(migration->replicas[1].metadata->primary);
    EXPECT_FALSE(migration->replicas[2].metadata->primary);
    EXPECT_FALSE(migration->replicas[3].metadata->primary);
    EXPECT_FALSE(migration->replicas[4].metadata->primary);

    // Primaries are in reverse order.
    EXPECT_EQ(91lu, migration->replicas[0].metadata->segmentId);
    EXPECT_EQ(88lu, migration->replicas[1].metadata->segmentId);

    // All secondaries are there.
    EXPECT_EQ(89lu, migration->replicas[2].metadata->segmentId);
    EXPECT_EQ(93lu, migration->replicas[3].metadata->segmentId);
    EXPECT_EQ(92lu, migration->replicas[4].metadata->segmentId);

    // The buffer has not started loading replicas into memory yet, so only
    // open secondaries will have loads requested because they were scanned
    // for a log digest.
    typedef InMemoryStorage::Frame *p;
    EXPECT_FALSE(p(migration->replicas[0].frame.get())->loadRequested);
    EXPECT_FALSE(p(migration->replicas[1].frame.get())->loadRequested);
    EXPECT_FALSE(p(migration->replicas[2].frame.get())->loadRequested);
    EXPECT_TRUE(p(migration->replicas[3].frame.get())->loadRequested);
    EXPECT_TRUE(p(migration->replicas[4].frame.get())->loadRequested);

    EXPECT_TRUE(migration->isScheduled());

    // Idempotence.
    buffer.reset();
    response = buffer.emplaceAppend<BackupMasterMigration::StartResponse>();
    migration->start(frames, &buffer, response);
    EXPECT_EQ(5u, response->replicaCount);
    EXPECT_EQ(2u, response->primaryReplicaCount);
    EXPECT_EQ(7u, response->digestBytes);
    EXPECT_EQ(92lu, response->digestSegmentId);
    EXPECT_EQ(192u, response->digestSegmentEpoch);
    EXPECT_EQ(11u, response->tableStatsBytes);
    EXPECT_STREQ("digest",
                 buffer.getOffset<char>(buffer.size()
                                        - 11 - 7));
    EXPECT_STREQ("tableStats",
                 buffer.getOffset<char>(buffer.size()
                                        - 11));
}

TEST_F(BackupMasterMigrationTest, getRecoverySegment)
{
    mockMetadata(88); // secondary
    mockMetadata(89, true, true); // primary
    migration->testingExtractDigest = &mockExtractDigest;
    migration->testingSkipBuild = true;
    migration->start(frames, NULL, NULL);
    migration->setPartitionsAndSchedule();

    EXPECT_THROW(migration->getRecoverySegment(456, 89, NULL, NULL),
                 RetryException);
    EXPECT_THROW(migration->getRecoverySegment(456, 88, NULL, NULL),
                 RetryException);

    taskQueue.performTask();
    Status status = migration->getRecoverySegment(456, 88, NULL, NULL);
    EXPECT_EQ(STATUS_OK, status);

    taskQueue.performTask();
    Buffer buffer;
    buffer.appendExternal("important", 10);
    ASSERT_TRUE(migration->replicas[1].migrationSegment->append(
        LOG_ENTRY_TYPE_OBJ, buffer));
    buffer.reset();
    SegmentCertificate certificate;
    memset(&certificate, 0xff, sizeof(certificate));
    status = migration->getRecoverySegment(456, 88, &buffer, &certificate);
    EXPECT_EQ(STATUS_OK, status);
    EXPECT_EQ(12lu, certificate.segmentLength);
    EXPECT_STREQ("important",
                 buffer.getOffset<char>(buffer.size() - 10));
}

TEST_F(BackupMasterMigrationTest, getRecoverySegment_exceptionDuringBuild)
{
    mockMetadata(89, true, true);
    migration->start(frames, NULL, NULL);
    migration->setPartitionsAndSchedule();
    taskQueue.performTask();
    EXPECT_THROW(migration->getRecoverySegment(456, 89, NULL, NULL),
                 SegmentRecoveryFailedException);
}

TEST_F(BackupMasterMigrationTest, getRecoverySegment_badArgs)
{
    mockMetadata(88, true, true);
    migration->testingExtractDigest = &mockExtractDigest;
    migration->testingSkipBuild = true;
    migration->start(frames, NULL, NULL);
    migration->setPartitionsAndSchedule();
    taskQueue.performTask();
    EXPECT_THROW(migration->getRecoverySegment(455, 88, NULL, NULL),
                 BackupBadSegmentIdException);
    migration->getRecoverySegment(456, 88, NULL, NULL);
    EXPECT_THROW(migration->getRecoverySegment(456, 89, NULL, NULL),
                 BackupBadSegmentIdException);
}

TEST_F(BackupMasterMigrationTest, free)
{
    std::unique_ptr<BackupMasterMigration>
        migration(
        new BackupMasterMigration(taskQueue, 456lu, sourceServerId,
                                  targetServerId, tableId, firstKeyHash,
                                  lastKeyHash, segmentSize, readSpeed,
                                  maxReplicasInMemory));
    TestLog::Enable _;
    migration->free();
    taskQueue.performTask();
    migration.release();
    EXPECT_EQ("free: Recovery 456 for crashed master 99.0 is no longer needed; "
              "will clean up as next possible chance. | "
              "schedule: scheduled | "
              "~BackupMasterMigration: Freeing migration state on backup for "
              "crashed master 99.0 (recovery 456), including 0 filtered "
              "replicas",
              TestLog::get());
}

TEST_F(BackupMasterMigrationTest, performTask)
{
    mockMetadata(88, true, true);
    mockMetadata(89, true, false);
    migration->testingSkipBuild = true;
    migration->start(frames, NULL, NULL);
    migration->setPartitionsAndSchedule();
    TestLog::Enable _;
    taskQueue.performTask();
    EXPECT_EQ(
        "schedule: scheduled | "
        "bufferNext: Added replica <99.0,88> to the recovery buffer | "
        "buildNext: <99.0,88> recovery segments took 0 ms to "
        "construct, notifying other threads",
        TestLog::get());
    TestLog::reset();
}

TEST_F(BackupMasterMigrationTest, CyclicReplicaBuffer_enqueue)
{
    BackupMasterMigration::CyclicReplicaBuffer *replicaBuffer =
        &migration->replicaBuffer;

    // Enqueue two normal priority primary replicas
    mockMetadata(88, true, true);
    mockMetadata(89, true, true);
    migration->testingSkipBuild = true;
    migration->start(frames, NULL, NULL);
    migration->setPartitionsAndSchedule();
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 2u);
    ASSERT_EQ(replicaBuffer->highPriorityQueuedReplicas.size(), 0u);
    ASSERT_EQ(replicaBuffer->inMemoryReplicas.size(), 0u);
    BackupMasterMigration::Replica *firstReplica =
        replicaBuffer->normalPriorityQueuedReplicas.front();
    // Make sure the replica enqueued first is at the front of the queue
    ASSERT_EQ(firstReplica->metadata->segmentId, 89u);

    // Try to re-enqueue at high priority, make sure still normal
    replicaBuffer->enqueue(firstReplica,
                           BackupMasterMigration::CyclicReplicaBuffer::HIGH);
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 2u);
    ASSERT_EQ(replicaBuffer->highPriorityQueuedReplicas.size(), 0u);
    ASSERT_EQ(replicaBuffer->inMemoryReplicas.size(), 0u);
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.front(),
              firstReplica);

    // Make sure we can't enqueue a buffered replica
    replicaBuffer->bufferNext();
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 1u);
    ASSERT_EQ(replicaBuffer->inMemoryReplicas.size(), 1u);
    replicaBuffer->enqueue(firstReplica,
                           BackupMasterMigration::CyclicReplicaBuffer::NORMAL);
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 1u);
}

TEST_F(BackupMasterMigrationTest, CyclicReplicaBuffer_bufferNext_eviction)
{
    migration->testingSkipBuild = true;
    BackupMasterMigration::CyclicReplicaBuffer *replicaBuffer =
        &migration->replicaBuffer;
    setupReplicaBuffer(false);

    BackupMasterMigration::Replica *firstReplica =
        replicaBuffer->inMemoryReplicas[0];

    // Replicas should not be evicted before they are built or fetched
    replicaBuffer->bufferNext();
    ASSERT_EQ(replicaBuffer->highPriorityQueuedReplicas.size(), 1u);
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 1u);
    firstReplica->built = true;
    firstReplica->fetchCount = 1;

    // Replicas should not be evicted until they've been inactive for half the
    // time it takes to read the buffer
    Cycles::mockTscValue =
        Cycles::fromSeconds(replicaBuffer->bufferReadTime / 2 - 1e-8);
    firstReplica->lastAccessTime = 0;
    replicaBuffer->bufferNext();
    ASSERT_EQ(replicaBuffer->highPriorityQueuedReplicas.size(), 1u);
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 1u);

    // Check that eviction succeeds if the properties above are met
    Cycles::mockTscValue =
        Cycles::fromSeconds(replicaBuffer->bufferReadTime / 2 + 1e-9);
    replicaBuffer->bufferNext();
    ASSERT_EQ(replicaBuffer->highPriorityQueuedReplicas.size(), 0u);
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 1u);

    Cycles::mockTscValue = 0;
}

TEST_F(BackupMasterMigrationTest, CyclicReplicaBuffer_bufferNext_priorities)
{
    migration->testingSkipBuild = true;
    BackupMasterMigration::CyclicReplicaBuffer *replicaBuffer =
        &migration->replicaBuffer;
    setupReplicaBuffer(true);

    // Make sure high priority replicas are buffered before normal priority
    // replicas
    replicaBuffer->bufferNext();
    ASSERT_EQ(replicaBuffer->highPriorityQueuedReplicas.size(), 0u);
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 1u);

    BackupMasterMigration::Replica *firstReplica;

    // Normal priority replicas are buffered when there are no high priority
    // replicas
    firstReplica = replicaBuffer->inMemoryReplicas[1];
    firstReplica->built = true;
    firstReplica->fetchCount = 1;
    replicaBuffer->bufferNext();
    ASSERT_EQ(replicaBuffer->highPriorityQueuedReplicas.size(), 0u);
    ASSERT_EQ(replicaBuffer->normalPriorityQueuedReplicas.size(), 0u);

    Cycles::mockTscValue = 0;
}

namespace {
bool buildNextFilter(string s)
{
    return s == "buildNext";
}
}

TEST_F(BackupMasterMigrationTest, CyclicReplicaBuffer_buildNext)
{
    mockMetadata(88, true, true);
    migration->testingSkipBuild = true;
    migration->start(frames, NULL, NULL);
    migration->replicaBuffer.bufferNext();

    TestLog::Enable _(buildNextFilter);
    migration->replicaBuffer.buildNext();
    EXPECT_EQ("buildNext: <99.0,88> recovery segments took 0 ms "
              "to construct, notifying other threads", TestLog::get());
    EXPECT_FALSE(migration->replicas.at(0).recoveryException);
    EXPECT_TRUE(migration->replicas.at(0).migrationSegment);
    EXPECT_TRUE(migration->replicas.at(0).built);
    TestLog::reset();
}

TEST_F(BackupMasterMigrationTest, buildRecoverySegments_buildThrows)
{
    mockMetadata(88, true, true);
    migration->start(frames, NULL, NULL);
    migration->setPartitionsAndSchedule();
    migration->replicaBuffer.bufferNext();

    TestLog::Enable _(buildNextFilter);
    migration->replicaBuffer.buildNext();
    EXPECT_TRUE(StringUtil::startsWith(TestLog::get(),
                                       "buildNext: Couldn't build recovery segments for "
                                       "<99.0,88>: RAMCloud::SegmentIteratorException: cannot iterate: "
                                       "corrupt segment, thrown "));
    EXPECT_TRUE(migration->replicas.at(0).recoveryException);
    EXPECT_FALSE(migration->replicas.at(0).migrationSegment);
    EXPECT_TRUE(migration->replicas.at(0).built);
}

}

