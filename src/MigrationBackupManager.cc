#include "MigrationBackupManager.h"
#include "MigrationSegmentBuilder.h"
#include "WorkerManager.h"
#include "MasterService.h"
#include "ObjectManager.h"
#include "SegmentManager.h"

namespace RAMCloud {

MigrationBackupManager::Replica::Replica(const BackupStorage::FrameRef &frame,
                                         Migration *migration)
    : frame(frame), migration(migration),
      metadata(
          static_cast<const BackupReplicaMetadata *>(frame->getMetadata())),
      chunks(),
      recoveryException(),
      built(),
      lastAccessTime(0),
      refCount(0),
      fetchCount(0),
      head(NULL),
      ackId(0),
      data(NULL),
      currentOffset(0),
      done(false),
      startTime(0)
{

}

MigrationBackupManager::Replica::~Replica()
{
    while (!chunks.empty()) {
        delete chunks.front();
        chunks.pop_front();
    }
}

void MigrationBackupManager::Replica::load()
{
    void *tryHead = frame->copyIfOpen();
    if (tryHead)
        head = tryHead;
    else
        frame->startLoading();
}

void MigrationBackupManager::Replica::filter()
{
    try {
        if (data == NULL) {
            filter_prepare();
        } else if (!done) {
            filter_scan();
        } else if (!built) {
            filter_finish();
        }
    } catch (const Exception &e) {
        // Can throw SegmentIteratorException or SegmentRecoveryFailedException.
        // Exception is a little broad, but it catches them both; hopefully we
        // don't try to recover from anything else too serious.
        RAMCLOUD_LOG(NOTICE, "Couldn't build migration segments for <%s,%lu>: "
                             "%s",
                     migration->sourceServerId.toString().c_str(),
                     metadata->segmentId, e.what());
        recoveryException.reset(
            new SegmentRecoveryFailedException(HERE));
        built = true;
    }
}

void MigrationBackupManager::Replica::filter_prepare()
{
    recoveryException.reset();
    startTime = Cycles::rdtsc();
    if (head)
        data = head;
    else
        data = frame->load();
    SegmentIterator it(data, migration->segmentSize,
                       metadata->certificate);
    it.checkMetadataIntegrity();
    chunks.push_back(new Chunk());
}

void MigrationBackupManager::Replica::filter_scan()
{
    SegmentIterator it(data, migration->segmentSize,
                       metadata->certificate);
    it.setOffset(currentOffset);

    Chunk *chunk = chunks.back();
    for (; !it.isDone(); it.next()) {

        if (it.getOffset() - currentOffset > Migration::MAX_SCAN_SIZE) {
            currentOffset = it.getOffset();
            break;
        }
        LogEntryType type = it.getType();
        if (type != LOG_ENTRY_TYPE_OBJ && type != LOG_ENTRY_TYPE_OBJTOMB)
            continue;

        Buffer entryBuffer;
        uint64_t tableId;
        KeyHash keyHash;

        it.appendToBuffer(entryBuffer);
        if (type == LOG_ENTRY_TYPE_OBJ) {
            Object object(entryBuffer);
            tableId = object.getTableId();
            keyHash = Key::getHash(tableId,
                                   object.getKey(), object.getKeyLength());
        } else if (type == LOG_ENTRY_TYPE_OBJTOMB) {
            ObjectTombstone tomb(entryBuffer);
            tableId = tomb.getTableId();
            keyHash = Key::getHash(tableId,
                                   tomb.getKey(), tomb.getKeyLength());
        } else {
                LOG(WARNING, "Unknown LogEntry (id=%u)", type);
            throw SegmentRecoveryFailedException(HERE);
        }
        if (!(tableId == migration->tableId &&
              migration->firstKeyHash <= keyHash &&
              keyHash <= migration->lastKeyHash)) {
            continue;
        }

        uint32_t length = entryBuffer.size();

        if (chunk->size() + length > Migration::MAX_BATCH_SIZE) {
            chunk = new Chunk();
            chunks.push_back(chunk);
        }

        chunk->append(type, length, entryBuffer);

    }
    done = it.isDone();
}

void MigrationBackupManager::Replica::filter_finish()
{
    RAMCLOUD_LOG(DEBUG,
                 "<%s,%lu> migration  segments took %lu us to construct.",
                 migration->sourceServerId.toString().c_str(),
                 metadata->segmentId,
                 Cycles::toNanoseconds(Cycles::rdtsc() - startTime) / 1000);
    Fence::sfence();
    built = true;
    lastAccessTime = Cycles::rdtsc();
    if (head)
        std::free(head);
    else
        frame->unload();
}

MigrationBackupManager::Migration::Migration(
    MigrationBackupManager *manager, uint64_t migrationId, uint64_t sourceId,
    uint64_t targetId, uint64_t tableId, uint64_t firstKeyHash,
    uint64_t lastKeyHash, const std::vector<BackupStorage::FrameRef> &frames)
    : manager(manager), context(manager->context), segmentManager(),
      migrationId(migrationId), sourceServerId(sourceId),
      targetServerId(targetId), tableId(tableId), firstKeyHash(firstKeyHash),
      lastKeyHash(lastKeyHash), segmentSize(manager->segmentSize),
      phase(LOADING), replicas(), replicasIterator(), replicaToFilter(),
      segmentIdToReplica(), replicaNum(), completedReplicaNum(), freeLoadRpcs(),
      busyLoadRpcs(), freeFilterRpcs(), busyFilterRpcs()
{
    MasterService *masterService = context->getMasterService();
    ObjectManager *objectManager = &(masterService->objectManager);
    segmentManager = &objectManager->segmentManager;

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

    RAMCLOUD_LOG(DEBUG,
                 "Backup preparing for migration %lu of source server %s; "
                 "loading %lu primary replicas", migrationId,
                 sourceServerId.toString().c_str(),
                 primaries.size());

    vector<BackupStorage::FrameRef>::reverse_iterator rit;
    for (rit = primaries.rbegin(); rit != primaries.rend(); ++rit) {
        replicas.emplace_back(*rit, this);
        auto &replica = replicas.back();
        segmentIdToReplica[replica.metadata->segmentId] = &replica;
    }

    replicasIterator = replicas.begin();
    replicaNum = static_cast<uint32_t>(replicas.size());

    for (uint32_t i = 0; i < MAX_PARALLEL_LOAD_RPCS; i++) {
        freeLoadRpcs.push_back(&(loadRpcs[i]));
    }

    for (uint32_t i = 0; i < MAX_PARALLEL_FILTER_RPCS; i++) {
        freeFilterRpcs.push_back(&(filterRpcs[i]));
    }

}

int MigrationBackupManager::Migration::poll()
{
    switch (phase) {
        case LOADING:
            return loadAndFilter_main();
        case SERVING:
            return 0;
        case COMPLETED:
            return 0;
    }
    return 0;
}

bool MigrationBackupManager::Migration::getSegment(
    uint64_t segmentId, uint32_t seqId, Buffer *buffer,
    SegmentCertificate *certificate)
{

    auto replicaIt = segmentIdToReplica.find(segmentId);
    if (replicaIt == segmentIdToReplica.end()) {
        RAMCLOUD_LOG(WARNING,
                     "Asked for segment <%s,%lu> "
                     "which isn't part of this migration",
                     sourceServerId.toString().c_str(), segmentId);
        throw BackupBadSegmentIdException(HERE);
    }

    Replica *replica = replicaIt->second;

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

    if (replica->recoveryException) {
        auto e = SegmentRecoveryFailedException(*replica->recoveryException);
        replica->recoveryException.reset();
        throw e;
    }

    if (seqId > replica->ackId) {
        delete replica->chunks.front();
        replica->chunks.pop_front();
        replica->ackId++;
    }

    Chunk *chunk = replica->chunks.front();

    if (buffer) {
        buffer->append(&chunk->buffer);
    }
    if (certificate) {
        chunk->createSegmentCertificate(certificate);
    }

    replica->fetchCount++;
    return replica->chunks.size() == 1;
}

int MigrationBackupManager::Migration::loadAndFilter_main()
{
    int workPerformed = 0;
    workPerformed += loadAndFilter_reapLoadRpcs();
    workPerformed += loadAndFilter_reapFilterRpcs();

    if (completedReplicaNum == replicaNum && phase < SERVING) {
        phase = SERVING;
        RAMCLOUD_LOG(NOTICE, "migration %lu finish loading", migrationId);
    }

    workPerformed += loadAndFilter_sendLoadRpcs();
    workPerformed += loadAndFilter_sendFilterRpcs();
    return workPerformed;
}

__inline __attribute__((always_inline))
int MigrationBackupManager::Migration::loadAndFilter_reapLoadRpcs()
{
    size_t numBusyLoadRpcs = busyLoadRpcs.size();
    for (size_t i = 0; i < numBusyLoadRpcs; i++) {
        Tub<LoadRpc> *loadRpc = busyLoadRpcs.front();
        if ((*loadRpc)->isReady()) {
            Replica *replica = (*loadRpc)->replica;
            RAMCLOUD_LOG(DEBUG, "load segmented %lu in memory",
                         replica->metadata->segmentId);
            replicaToFilter.push_back(replica);
            (*loadRpc).destroy();
            freeLoadRpcs.push_back(loadRpc);

        } else {
            busyLoadRpcs.push_back(loadRpc);
        }

        busyLoadRpcs.pop_front();

    }
    return 0;
}

__inline __attribute__((always_inline))
int MigrationBackupManager::Migration::loadAndFilter_reapFilterRpcs()
{
    int workPerformed = 0;
    size_t numBusyFilterRpcs = busyFilterRpcs.size();
    for (size_t i = 0; i < numBusyFilterRpcs; i++) {
        Tub<FilterRpc> *filterRpc = busyFilterRpcs.front();
        if ((*filterRpc)->isReady()) {

            Replica *replica = (*filterRpc)->replica;

            if (replica->built) {
                RAMCLOUD_LOG(DEBUG, "filter segmented %lu in memory",
                             replica->metadata->segmentId);
                completedReplicaNum++;
            } else {
                replicaToFilter.push_front(replica);
            }

            (*filterRpc).destroy();
            freeFilterRpcs.push_back(filterRpc);
            workPerformed++;
        } else {
            busyFilterRpcs.push_back(filterRpc);
        }

        busyFilterRpcs.pop_front();
    }
    return workPerformed;
}

__inline __attribute__((always_inline))
int MigrationBackupManager::Migration::loadAndFilter_sendLoadRpcs()
{
    int workPerformed = 0;
    while (!freeLoadRpcs.empty() && replicasIterator != replicas.end()) {
        Tub<LoadRpc> *loadRpc = freeLoadRpcs.front();

        Replica &replica = *replicasIterator;
        loadRpc->construct(manager->localLocator, &replica);
        context->workerManager->handleRpc(loadRpc->get());
        workPerformed++;

        replicasIterator++;
        freeLoadRpcs.pop_front();
        busyLoadRpcs.push_back(loadRpc);
    }
    return workPerformed;
}

__inline __attribute__((always_inline))
int MigrationBackupManager::Migration::loadAndFilter_sendFilterRpcs()
{
    int workPerformed = 0;
    while (!freeFilterRpcs.empty() && !replicaToFilter.empty()) {
        Tub<FilterRpc> *filterRpc = freeFilterRpcs.front();
        Replica *replica = replicaToFilter.front();

        RAMCLOUD_LOG(DEBUG, "send segmented %lu to filter",
                     replica->metadata->segmentId);
        filterRpc->construct(manager->localLocator, replica);

        busyFilterRpcs.push_back(filterRpc);
        context->workerManager->handleRpc(filterRpc->get());
        workPerformed++;

        freeFilterRpcs.pop_front();
        replicaToFilter.pop_front();
    }
    return workPerformed;
}


MigrationBackupManager::MigrationBackupManager(
    Context *context, string localLocator, uint32_t segmentSize)
    : Dispatch::Poller(context->dispatch, "MigrationBackupManager"),
      context(context), localLocator(localLocator), segmentSize(segmentSize),
      migrationsInProgress(), migrationMap()
{
}

int MigrationBackupManager::poll()
{
    int workPerformed = 0;

    for (auto migration = migrationsInProgress.begin();
         migration != migrationsInProgress.end();) {
        Migration *currentMigration = *migration;

        workPerformed += currentMigration->poll();

        if (currentMigration->phase == Migration::COMPLETED) {
            migration = migrationsInProgress.erase(migration);
            delete currentMigration;
        } else {
            migration++;
        }
    }

    return workPerformed == 0 ? 0 : 1;
}

void MigrationBackupManager::start(
    uint64_t migrationId, uint64_t sourceId, uint64_t targetId,
    uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
    const std::vector<BackupStorage::FrameRef> &frames)
{
    Migration *migration = new Migration(
        this, migrationId, sourceId, targetId, tableId, firstKeyHash,
        lastKeyHash, frames);

    migrationsInProgress.push_back(migration);
    migrationMap[migrationId] = migration;

}

bool MigrationBackupManager::getSegment(
    uint64_t migrationId, uint64_t segmentId, uint32_t seqId, Buffer *buffer,
    SegmentCertificate *certificate)
{

    auto migrationIt = migrationMap.find(migrationId);
    if (migrationIt == migrationMap.end()) {
        RAMCLOUD_LOG(NOTICE,
                     "couldn't find migration %lu", migrationId);
        throw BackupBadSegmentIdException(HERE);
    }

    return migrationIt->second->getSegment(segmentId, seqId, buffer,
                                           certificate);

}


void
MigrationBackupManager::Chunk::append(
    LogEntryType type, uint32_t length, Buffer &entryBuffer)
{
    Segment::EntryHeader entryHeader(type, length);

    buffer.appendCopy(&entryHeader);
    checksum.update(&entryHeader, sizeof(entryHeader));

    buffer.appendCopy(&length, entryHeader.getLengthBytes());
    checksum.update(&length, entryHeader.getLengthBytes());
    buffer.appendCopy(entryBuffer.getRange(0, length), length);
}

void MigrationBackupManager::Chunk::createSegmentCertificate(
    SegmentCertificate *certificate)
{
    certificate->segmentLength = buffer.size();

    checksum.update(certificate, static_cast<unsigned>(
        sizeof(*certificate) - sizeof(certificate->checksum)));

    certificate->checksum = checksum.getResult();
}

uint32_t MigrationBackupManager::Chunk::size()
{
    return buffer.size();
}

}
