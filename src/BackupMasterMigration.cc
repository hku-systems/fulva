
#include "BackupMasterMigration.h"
#include "MigrationSegmentBuilder.h"

namespace RAMCloud {

SpinLock BackupMasterMigration::deletionMutex
    ("BackupMasterMigration::deletionMutex");

RAMCloud::BackupMasterMigration::BackupMasterMigration(
    TaskQueue &taskQueue, uint64_t migrationId, ServerId sourceServerId,
    ServerId targetServerId, uint64_t tableId, uint64_t firstKeyHash,
    uint64_t lastKeyHash, uint32_t segmentSize,
    uint32_t readSpeed, uint32_t maxReplicasInMemory)
    : Task(taskQueue),
      migrationId(migrationId),
      sourceServerId(sourceServerId),
      targetServerId(targetServerId),
      tableId(tableId),
      firstKeyHash(firstKeyHash),
      lastKeyHash(lastKeyHash),
      segmentSize(segmentSize),
      replicas(),
      segmentIdToReplica(),
      replicaBuffer(maxReplicasInMemory, segmentSize, readSpeed, this),
      logDigest(),
      logDigestSegmentId(~0lu),
      logDigestSegmentEpoch(),
      tableStatsDigest(),
      startCompleted(false),
      recoveryTicks(),
      readingDataTicks(),
      buildingStartTicks(),
      testingExtractDigest(),
      testingSkipBuild(),
      destroyer(taskQueue, this),
      pendingDeletion(false),
      startTime(0)
{

}

BackupMasterMigration::~BackupMasterMigration()
{
    RAMCLOUD_LOG(NOTICE, "Freeing migration state on backup for crashed master "
                         "%s (recovery %lu), including %lu filtered replicas",
                 sourceServerId.toString().c_str(), migrationId,
                 replicaBuffer.size());
}

void
BackupMasterMigration::start(const std::vector<BackupStorage::FrameRef> &frames,
                             Buffer *buffer,
                             StartResponse *response)
{
    if (startCompleted) {
        populateStartResponse(buffer, response);
        return;
    }

    metrics->backup.storageReadBytes = 0;
    metrics->backup.storageReadTicks = 0;
    recoveryTicks.construct(&metrics->backup.recoveryTicks);
    metrics->backup.recoveryCount++;
    startTime = Cycles::rdtsc();

    vector<BackupStorage::FrameRef> primaries;
    vector<BackupStorage::FrameRef> secondaries;

    for (auto &frame: frames) {
        const BackupReplicaMetadata *metadata =
            reinterpret_cast<const BackupReplicaMetadata *>(frame->getMetadata());
        if (!metadata->checkIntegrity()) {
            RAMCLOUD_LOG(NOTICE,
                         "Replica of <%s,%lu> metadata failed integrity check; "
                         "will not be used for recovery (note segment id in "
                         "this log message may be corrupted as a result)",
                         sourceServerId.toString().c_str(),
                         metadata->segmentId);
            continue;
        }
        if (metadata->logId != sourceServerId.getId())
            continue;
        (metadata->primary ? primaries : secondaries).push_back(frame);
    }
    RAMCLOUD_LOG(NOTICE,
                 "Backup preparing for recovery %lu of source server %s; "
                 "loading %lu primary replicas", migrationId,
                 sourceServerId.toString().c_str(),
                 primaries.size());

    // Build the deque and the mapping from segment ids to replicas.
    readingDataTicks.construct(&metrics->backup.readingDataTicks);

    // Arrange for replicas to be processed in reverse chronological order (most
    // recent replicas first) by enqueuing them in #replicaBuffer in reverse
    // order. This improves recovery performance by ensuring that the buffer
    // loads replicas in the same order they will be requested by the recovery
    // masters.
    vector<BackupStorage::FrameRef>::reverse_iterator rit;
    for (rit = primaries.rbegin(); rit != primaries.rend(); ++rit) {
        replicas.emplace_back(*rit);
        auto &replica = replicas.back();
        replicaBuffer.enqueue(&replica, CyclicReplicaBuffer::NORMAL);
        segmentIdToReplica[replica.metadata->segmentId] = &replica;
    }
    for (auto &frame: secondaries) {
        replicas.emplace_back(frame);
        auto &replica = replicas.back();
        segmentIdToReplica[replica.metadata->segmentId] = &replica;
    }

    startCompleted = true;
}

void BackupMasterMigration::setPartitionsAndSchedule()
{
    assert(startCompleted);
    RAMCLOUD_LOG(NOTICE, "Recovery %lu building recovery segment for each "
                         "replica for crashed master %s.",
                 migrationId, sourceServerId.toString().c_str());

    RAMCLOUD_LOG(DEBUG, "Kicked off building recovery segments");
    buildingStartTicks = Cycles::rdtsc();
    schedule();
}

Status BackupMasterMigration::getRecoverySegment(uint64_t migrationId,
                                                 uint64_t segmentId,
                                                 Buffer *buffer,
                                                 SegmentCertificate *certificate)
{
    if (this->migrationId != migrationId) {
        RAMCLOUD_LOG(ERROR,
                     "Requested recovery segment from recovery %lu, but "
                     "current recovery for that master is %lu",
                     migrationId, this->migrationId);
        throw BackupBadSegmentIdException(HERE);
    }
    auto replicaIt = segmentIdToReplica.find(segmentId);
    if (replicaIt == segmentIdToReplica.end()) {
        RAMCLOUD_LOG(NOTICE,
                     "Asked for a recovery segment for segment <%s,%lu> "
                     "which isn't part of this recovery",
                     sourceServerId.toString().c_str(), segmentId);
        throw BackupBadSegmentIdException(HERE);
    }


    Replica *replica = replicaIt->second;
    CyclicReplicaBuffer::ActiveReplica activeRecord(replica, &replicaBuffer);

    if (!replicaBuffer.contains(replica)) {
        // This replica isn't loaded into memory, so try to schedule it to be
        // added to the buffer.
        replicaBuffer.enqueue(replica, CyclicReplicaBuffer::HIGH);
        throw RetryException(HERE, 5000, 10000,
                             "desired segment has not yet been loaded "
                             "into buffer");
    }

    Fence::lfence();
    if (!replica->built) {
        if (replica->frame->isLoaded()) {
            RAMCLOUD_LOG(DEBUG,
                         "Deferring because <%s,%lu> not yet filtered: %p",
                         sourceServerId.toString().c_str(), segmentId, replica);
        } else {
            RAMCLOUD_LOG(DEBUG, "Deferring because <%s,%lu> not yet loaded: %p",
                         sourceServerId.toString().c_str(), segmentId, replica);
        }

        throw RetryException(HERE, 5000, 10000,
                             "desired segment not yet filtered");
    }

    if (replica->metadata->primary)
        ++metrics->backup.primaryLoadCount;
    else
        ++metrics->backup.secondaryLoadCount;

    if (replica->recoveryException) {
        auto e = SegmentRecoveryFailedException(*replica->recoveryException);
        replica->recoveryException.reset();
        throw e;
    }

    if (buffer)
        replica->migrationSegment->appendToBuffer(*buffer);
    if (certificate)
        replica->migrationSegment->getAppendedLength(certificate);

    replica->fetchCount++;
    return STATUS_OK;
}

void BackupMasterMigration::free()
{
    RAMCLOUD_LOG(DEBUG,
                 "Recovery %lu for crashed master %s is no longer needed;"
                 " will clean up as next possible chance.",
                 migrationId, sourceServerId.toString().c_str());
    SpinLock::Guard lock(deletionMutex);
    pendingDeletion = true;
    destroyer.schedule();
}

uint64_t BackupMasterMigration::
getMigrationId()
{
    return migrationId;
}

void BackupMasterMigration::performTask()
{
    {
        SpinLock::Guard lock(deletionMutex);
        if (!pendingDeletion)
            schedule();
    }

    replicaBuffer.bufferNext();
    replicaBuffer.buildNext();
}

void BackupMasterMigration::populateStartResponse(Buffer *responseBuffer,
                                                  StartResponse *response)
{
    if (responseBuffer == NULL)
        return;

    response->replicaCount = 0;
    response->primaryReplicaCount = 0;

    for (const auto &replica: replicas) {
        responseBuffer->emplaceAppend<
            WireFormat::MigrationStartReading::Replica>(
            replica.metadata->segmentId, replica.metadata->segmentEpoch,
            replica.metadata->closed);
        ++response->replicaCount;
        if (replica.metadata->primary)
            ++response->primaryReplicaCount;
        RAMCLOUD_LOG(DEBUG,
                     "Crashed master %s had %s %s replica for segment %lu",
                     sourceServerId.toString().c_str(),
                     replica.metadata->closed ? "closed" : "open",
                     replica.metadata->primary ? "primary" : "secondary",
                     replica.metadata->segmentId);
    }

    RAMCLOUD_LOG(DEBUG, "Sending %u segment ids for this master (%u primary)",
                 response->replicaCount, response->primaryReplicaCount);

    response->digestSegmentId = logDigestSegmentId;
    response->digestSegmentEpoch = logDigestSegmentEpoch;
    response->digestBytes = logDigest.size();
    if (response->digestBytes > 0) {
        responseBuffer->appendCopy(logDigest.getRange(0, response->digestBytes),
                                   response->digestBytes);
        RAMCLOUD_LOG(DEBUG, "Sent %u bytes of LogDigest to coordinator",
                     response->digestBytes);
    }

    response->tableStatsBytes = tableStatsDigest.size();
    if (response->tableStatsBytes > 0) {
        responseBuffer->appendCopy(
            tableStatsDigest.getRange(0, response->tableStatsBytes),
            response->tableStatsBytes);
        RAMCLOUD_LOG(DEBUG, "Sent %u bytes of table statistics to coordinator",
                     response->tableStatsBytes);
    }
}

bool
BackupMasterMigration::getLogDigest(BackupMasterMigration::Replica &replica,
                                    Buffer *digestBuffer)
{
    return false;
}

BackupMasterMigration::Replica::Replica(const BackupStorage::FrameRef &frame)
    : frame(frame),
      metadata(
          static_cast<const BackupReplicaMetadata *>(frame->getMetadata())),
      migrationSegment(),
      recoveryException(),
      built(),
      lastAccessTime(0),
      refCount(0),
      fetchCount(0),
      head(NULL)
{

}

BackupMasterMigration::Replica::~Replica()
{
}

BackupMasterMigration::CyclicReplicaBuffer::CyclicReplicaBuffer(
    uint32_t maxReplicasInMemory, uint32_t segmentSize,
    uint32_t readSpeed, BackupMasterMigration *migration)
    : mutex("cyclicReplicaBufferMutex"),
      maxReplicasInMemory(maxReplicasInMemory),
      inMemoryReplicas(),
      oldestReplicaIdx(0),
      normalPriorityQueuedReplicas(),
      highPriorityQueuedReplicas(),
      bufferReadTime(static_cast<double>(segmentSize)
                     * maxReplicasInMemory / readSpeed
                     / (1 << 20)) // MB to bytes
    , migration(migration)
{
    inMemoryReplicas.reserve(maxReplicasInMemory);
    RAMCLOUD_LOG(NOTICE, "Constructed cyclic recovery buffer with %u replicas "
                         "and a read time of %f s", maxReplicasInMemory,
                 bufferReadTime);

}

BackupMasterMigration::CyclicReplicaBuffer::~CyclicReplicaBuffer()
{
}

size_t BackupMasterMigration::CyclicReplicaBuffer::size()
{
    SpinLock::Guard lock(mutex);
    return inMemoryReplicas.size();
}

bool BackupMasterMigration::CyclicReplicaBuffer::contains(
    Replica *replica)
{
    SpinLock::Guard lock(mutex);
    return std::find(inMemoryReplicas.begin(), inMemoryReplicas.end(), replica)
           != inMemoryReplicas.end();
}

void BackupMasterMigration::CyclicReplicaBuffer::enqueue(
    Replica *replica, Priority priority)
{
    SpinLock::Guard lock(mutex);

    // Don't enqueue a replica already in the buffer
    if (std::find(inMemoryReplicas.begin(), inMemoryReplicas.end(), replica)
        != inMemoryReplicas.end()) {
        return;
    }

    if (std::find(normalPriorityQueuedReplicas.begin(),
                  normalPriorityQueuedReplicas.end(), replica)
        != normalPriorityQueuedReplicas.end()) {
        // This replica was scheduled normally and we just haven't gotten to
        // it yet.
        RAMCLOUD_LOG(NOTICE, "A master is ahead, requesting segment %lu",
                     replica->metadata->segmentId);
        return;
    } else if (priority == NORMAL) {
        RAMCLOUD_LOG(DEBUG,
                     "Adding replica for segment %lu to normal priority "
                     "recovery queue", replica->metadata->segmentId);
        normalPriorityQueuedReplicas.push_back(replica);
    } else {
        // Don't schedule a replica as high priority more than once
        if (std::find(highPriorityQueuedReplicas.begin(),
                      highPriorityQueuedReplicas.end(), replica)
            == highPriorityQueuedReplicas.end()) {
            RAMCLOUD_LOG(WARNING, "Adding replica for segment %lu to high "
                                  "priority recovery queue",
                         replica->metadata->segmentId);
            highPriorityQueuedReplicas.push_back(replica);
        }
    }
}

bool BackupMasterMigration::CyclicReplicaBuffer::bufferNext()
{
    SpinLock::Guard lock(mutex);

    Replica *nextReplica;
    std::deque<Replica *> *replicaDeque;
    if (!highPriorityQueuedReplicas.empty()) {
        nextReplica = highPriorityQueuedReplicas.front();
        replicaDeque = &highPriorityQueuedReplicas;
    } else if (!normalPriorityQueuedReplicas.empty()) {
        nextReplica = normalPriorityQueuedReplicas.front();
        replicaDeque = &normalPriorityQueuedReplicas;
    } else {
        return false;
    }

    if (inMemoryReplicas.size() < maxReplicasInMemory) {
        // We haven't filled up the buffer yet.
        inMemoryReplicas.push_back(nextReplica);
    } else {
        Replica *replicaToRemove = inMemoryReplicas[oldestReplicaIdx];
        if (!replicaToRemove->built || replicaToRemove->fetchCount == 0 ||
            replicaToRemove->refCount > 0) {
            // This replica isn't eligible for eviction yet.
            return false;
        }

        // Only evict a replica if it's been inactive for half the time it takes
        // to read the entire buffer from disk.
        double entryInactiveTime = Cycles::toSeconds(
            Cycles::rdtsc() - replicaToRemove->lastAccessTime);
        if (entryInactiveTime <= bufferReadTime / 2) {
            return false;
        }

        // Evict
        replicaToRemove->migrationSegment.release();
        replicaToRemove->built = false;
        inMemoryReplicas[oldestReplicaIdx] = nextReplica;
        RAMCLOUD_LOG(DEBUG, "Evicted replica <%s,%lu> from the recovery buffer",
                     migration->sourceServerId.toString().c_str(),
                     replicaToRemove->metadata->segmentId);

        oldestReplicaIdx = (oldestReplicaIdx + 1) % maxReplicasInMemory;
    }
    // Read the next replica from disk.
    void *tryHead = NULL;
    if (!nextReplica->head)
        tryHead = nextReplica->frame->copyIfOpen();
    if (tryHead)
        nextReplica->head = tryHead;
    else
        nextReplica->frame->startLoading();

    replicaDeque->pop_front();

    RAMCLOUD_LOG(DEBUG, "Added replica <%s,%lu> to the recovery buffer",
                 migration->sourceServerId.toString().c_str(),
                 nextReplica->metadata->segmentId);
    return true;
}

bool BackupMasterMigration::CyclicReplicaBuffer::buildNext()
{
    Replica *replicaToBuild = NULL;
    {
        // Find the next loaded, unbuilt replica.
        SpinLock::Guard lock(mutex);
        for (size_t i = 0; i < inMemoryReplicas.size(); i++) {
            size_t idx = (i + oldestReplicaIdx) % inMemoryReplicas.size();
            Replica *candidate = inMemoryReplicas[idx];
            if (!candidate->built)
                if (candidate->head || candidate->frame->isLoaded()) {
                    replicaToBuild = candidate;
                    break;
                }
        }
    }

    if (!replicaToBuild) {
        return false;
    }

    replicaToBuild->recoveryException.reset();
    replicaToBuild->migrationSegment.reset();

    void *replicaData = NULL;
    if (replicaToBuild->head)
        replicaData = replicaToBuild->head;
    else
        replicaData = replicaToBuild->frame->load();

    CycleCounter<RawMetric> _(&metrics->backup.filterTicks);

    // Recovery segments for this replica data are constructed by splitting data
    // among them according to #partitions. If we move to multiple worker
    // threads then replicas will need to be locked for this filtering.
    std::unique_ptr<Segment> migrationSegment(new Segment());
    uint64_t start = Cycles::rdtsc();
    try {
        if (!migration->testingSkipBuild) {
            MigrationSegmentBuilder::build(replicaData, migration->segmentSize,
                                           replicaToBuild->metadata->certificate,
                                           migrationSegment.get(),
                                           migration->tableId,
                                           migration->firstKeyHash,
                                           migration->lastKeyHash);
        }
    } catch (const Exception &e) {
        // Can throw SegmentIteratorException or SegmentRecoveryFailedException.
        // Exception is a little broad, but it catches them both; hopefully we
        // don't try to recover from anything else too serious.
        RAMCLOUD_LOG(NOTICE, "Couldn't build recovery segments for <%s,%lu>: "
                             "%s",
                     migration->sourceServerId.toString().c_str(),
                     replicaToBuild->metadata->segmentId, e.what());
        replicaToBuild->recoveryException.reset(
            new SegmentRecoveryFailedException(HERE));
        Fence::sfence();
        replicaToBuild->built = true;
        return true;
    }

    RAMCLOUD_LOG(NOTICE, "<%s,%lu> recovery segments took %lu us to construct, "
                         "notifying other threads",
                 migration->sourceServerId.toString().c_str(),
                 replicaToBuild->metadata->segmentId,
                 Cycles::toNanoseconds(Cycles::rdtsc() - start) / 1000);
    replicaToBuild->migrationSegment = std::move(migrationSegment);
    Fence::sfence();
    replicaToBuild->built = true;
    replicaToBuild->lastAccessTime = Cycles::rdtsc();
    if (replicaToBuild->head)
        std::free(replicaToBuild->head);
    else
        replicaToBuild->frame->unload();
    return true;
}

void BackupMasterMigration::CyclicReplicaBuffer::logState()
{
    SpinLock::Guard lock(mutex);
    for (size_t i = 0; i < inMemoryReplicas.size(); i++) {
        Replica *replica = inMemoryReplicas[i];
        std::string state;
        if (replica->built) {
            state = "is built";
        } else if (replica->frame->isLoaded()) {
            state = "is loaded but not built";
        } else {
            state = "is not loaded";
        }

        RAMCLOUD_LOG(NOTICE, "%sbuffer entry %lu: segment %lu, %s, fetched %d "
                             "times",
                     oldestReplicaIdx == i ? "-->" : "",
                     i,
                     replica->metadata->segmentId,
                     state.c_str(),
                     replica->fetchCount.load());
    }

}
}
