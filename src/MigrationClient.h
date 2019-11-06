#ifndef RAMCLOUD_MIGRATIONCLIENT_H
#define RAMCLOUD_MIGRATIONCLIENT_H

#include "Key.h"
#include "Tablet.h"
#include "RamCloud.h"
#include "Dispatch.h"

#include <unordered_set>

namespace RAMCloud {

class MigrationClient {
  PRIVATE:

    struct TabletKey {
        uint64_t tableId;       // tableId of the tablet
        KeyHash keyHash;        // start key hash value

        /**
         * The operator < is overridden to implement the
         * correct comparison for the tableMap.
         */
        bool operator<(const TabletKey &key) const
        {
            return tableId < key.tableId ||
                   (tableId == key.tableId && keyHash < key.keyHash);
        }
    };

  PUBLIC:

    struct MigratingTablet {
        Tablet tablet;

        ServerId sourceId;
        ServerId targetId;

        MigratingTablet(Tablet tablet, uint64_t sourceId,
                        uint64_t targetId)
            : tablet(tablet), sourceId(sourceId),
              targetId(targetId)
        {}
    };

  PRIVATE:

    RamCloud *ramcloud;
    std::map<TabletKey, MigratingTablet> tableMap;

    DISALLOW_COPY_AND_ASSIGN(MigrationClient);
  PUBLIC:

    class migrationPartitionsProgress {
    public:
        explicit migrationPartitionsProgress(uint64_t startHTBucket,
                                            uint64_t endHTBucket)
            : startHTBucket(startHTBucket), endHTBucket(endHTBucket), currentHTBucket(startHTBucket) {
        }

        ~migrationPartitionsProgress() {}

        const uint64_t startHTBucket;

        const uint64_t endHTBucket;

        uint64_t currentHTBucket;
    };

    Tub<migrationPartitionsProgress> partitions[WireFormat::MAX_NUM_PARTITIONS];
    std::unordered_set<uint64_t> finishedPriorityHashes;

    uint64_t notFound;
    uint64_t regularPullFound;
    uint64_t priorityPullFound;
    uint64_t sourceNumHTBuckets;

    uint64_t findBucketIdx(uint64_t numBuckets, KeyHash keyHash) {
        uint64_t bucketHash = keyHash & 0x0000ffffffffffffUL;
        return (bucketHash & (numBuckets - 1));
    }

    bool lookupRegularPullProgress(uint64_t bucketIndex) {
        uint64_t partitionIdx = bucketIndex / (sourceNumHTBuckets / WireFormat::MAX_NUM_PARTITIONS);
        if (partitions[partitionIdx]->startHTBucket <= bucketIndex && bucketIndex < partitions[partitionIdx]->currentHTBucket) {
            return true;
        }
        return false;
    }

    bool lookupPriorityPullProgress(uint64_t hash) {
        if (finishedPriorityHashes.find(hash) != finishedPriorityHashes.end()) {
            return true;
        }
        return false;
    }

    void updateProgress(const WireFormat::Read::Response *respHdr, uint64_t hash, uint64_t bucket) {
        for (uint32_t i = 0; i < WireFormat::MAX_NUM_PARTITIONS; ++i) {
            partitions[i]->currentHTBucket = respHdr->partitionsProgress[i];
            // RAMCLOUD_LOG(NOTICE, "partition %u currentHTBucket is %lu.", i, partitions[i]->currentHTBucket);
        }
        if (respHdr->common.status == STATUS_OK && !lookupRegularPullProgress(bucket)) {
            finishedPriorityHashes.insert(hash);
        }
        // for (auto it = finishedPriorityHashes.begin(); it != finishedPriorityHashes.end();) {
        //     if (lookupRegularPullProgress(findBucketIdx(sourceNumHTBuckets, *it))) {
        //         it = finishedPriorityHashes.erase(it);
        //     } else {
        //         ++it;
        //     }
        // }
    }

    MigrationClient(RamCloud *ramcloud);

    void putTablet(uint64_t tableId, const void *key, uint16_t keyLength,
                   uint64_t sourceId, uint64_t targetId);

    MigratingTablet *
    getTablet(uint64_t tableId, const void *key, uint16_t keyLength);

    void removeTablet(uint64_t tableId, const void *key, uint16_t keyLength);
};

template<class Normal, class Migration>
class MigrationReadTask {
  PRIVATE:
    enum State {
        INIT, NORMAL, NORMAL_WAIT, MIGRATING, MIGRATING_WAIT, DONE
    };
    RamCloud *ramcloud;
    uint64_t tableId;
    void *key;
    uint16_t keyLength;
    Buffer *value;
    const RejectRules *rejectRules;

    Tub<Normal> readRpc;
    Tub<Migration> sourceReadRpc, targetReadRpc;

    State state;
    Buffer sourceBuffer, targetBuffer;
    uint64_t finishTime;

    uint64_t version;
    bool objectExists;
    bool sendToBoth;

    DISALLOW_COPY_AND_ASSIGN(MigrationReadTask)

  PUBLIC:
    KeyHash keyHash;

    MigrationReadTask(
        RamCloud *ramcloud, uint64_t tableId, const void *key,
        uint16_t keyLength, Buffer *value,
        const RejectRules *rejectRules = NULL)
        : ramcloud(ramcloud), tableId(tableId), key(NULL), keyLength(keyLength),
          value(value), rejectRules(rejectRules), readRpc(), sourceReadRpc(),
          targetReadRpc(), state(INIT), sourceBuffer(), targetBuffer(),
          finishTime(), version(), objectExists(false), sendToBoth(false), keyHash()
    {
        this->key = std::malloc(keyLength);
        std::memcpy(this->key, key, keyLength);
        keyHash = Key::getHash(tableId, key, keyLength);
    }

    ~MigrationReadTask()
    {
        std::free(key);
    }

    void performTask()
    {
        if (state == INIT) {
            MigrationClient::MigratingTablet *migratingTablet =
                ramcloud->migrationClient->getTablet(tableId, key, keyLength);
            if (migratingTablet) {
                targetReadRpc.construct(
                    ramcloud, migratingTablet->targetId, tableId, key,
                    keyLength, &targetBuffer, rejectRules);

                if (ramcloud->migrationClient->lookupRegularPullProgress(targetReadRpc->getBucketIdx())) {
                    ramcloud->migrationClient->regularPullFound++;
                } else if (ramcloud->migrationClient->lookupPriorityPullProgress(targetReadRpc->getHash())) {
                    ramcloud->migrationClient->priorityPullFound++;
                } else {
                    ramcloud->migrationClient->notFound++;
                    sendToBoth = true;
                    sourceReadRpc.construct(
                        ramcloud, migratingTablet->sourceId, tableId, key,
                        keyLength, &sourceBuffer, rejectRules);
                }
                state = MIGRATING;
            } else {
                readRpc.construct(ramcloud, tableId, key, keyLength, value,
                                  rejectRules);
                state = NORMAL;
            }
        }

        if (state == NORMAL) {
            if (readRpc->isReady()) {
                bool migrating;
                uint64_t sourceId, targetId;
                readRpc->wait(&version, &objectExists, &migrating, &sourceId,
                              &targetId);

                if (migrating) {
                    ramcloud->migrationClient->putTablet(
                        tableId, key, keyLength, sourceId, targetId);
                    state = INIT;
                } else {
                    state = DONE;
                }
            } else {
                ramcloud->clientContext->dispatch->poll();
            }
        }

        if (state == MIGRATING) {
            if (sendToBoth) {
                if (targetReadRpc->isReady() && sourceReadRpc->isReady()) {
                    bool migrating;
                    uint64_t sourceId;
                    uint64_t sourceVersion;
                    bool sourceObjectExists;
                    uint64_t targetId;
                    uint64_t targetVersion;
                    bool targetObjectExists;

                    bool success = true;
                    success = success && sourceReadRpc->wait(
                        &sourceVersion, &sourceObjectExists, &migrating,
                        &sourceId, &targetId);
                    success = success && targetReadRpc->wait(
                        &targetVersion, &targetObjectExists, &migrating,
                        &sourceId, &targetId);

                    if (!migrating) {
                        ramcloud->migrationClient->removeTablet(tableId, key,
                                                                keyLength);
                    }

                    if (!success) {
                        state = INIT;
                        return;
                    }

                    targetReadRpc->updateProgress();

                    value->reset();

                    uint64_t versionConclusion;
                    bool existsConclusion;
                    if (sourceVersion > targetVersion) {
                        versionConclusion = sourceVersion;
                        value->append(&sourceBuffer, 0u, sourceBuffer.size());
                        existsConclusion = sourceObjectExists;
                    } else {
                        versionConclusion = targetVersion;
                        value->append(&targetBuffer, 0u, targetBuffer.size());
                        existsConclusion = targetObjectExists;
                    }
                    version = versionConclusion;
                    objectExists = existsConclusion;
                    state = MIGRATING_WAIT;
                    finishTime = Cycles::rdtsc();
                } else {
                    ramcloud->clientContext->dispatch->poll();
                }                
            } else {
                if (targetReadRpc->isReady()) {
                    bool migrating;
                    uint64_t sourceId;
                    uint64_t targetId;
                    uint64_t targetVersion;
                    bool targetObjectExists;

                    bool success = true;
                    success = success && targetReadRpc->wait(
                        &targetVersion, &targetObjectExists, &migrating,
                        &sourceId, &targetId);

                    if (!migrating) {
                        ramcloud->migrationClient->removeTablet(tableId, key,
                                                                keyLength);
                    }

                    if (!success) {
                        state = INIT;
                        return;
                    }

                    targetReadRpc->updateProgress();

                    value->reset();

                    uint64_t versionConclusion;
                    bool existsConclusion;

                    versionConclusion = targetVersion;
                    value->append(&targetBuffer, 0u, targetBuffer.size());
                    existsConclusion = targetObjectExists;
                    
                    version = versionConclusion;
                    objectExists = existsConclusion;
                    state = MIGRATING_WAIT;
                    finishTime = Cycles::rdtsc();
                } else {
                    ramcloud->clientContext->dispatch->poll();
                }
            }

        }

        if (state == MIGRATING_WAIT) {
            state = DONE;
        }

    }

    bool isReady()
    {
        performTask();
        return state == DONE;
    }

    void wait(uint64_t *version, bool *objectExists)
    {
        while (!isReady());
        if (version)
            *version = this->version;
        if (objectExists)
            *objectExists = this->objectExists;
    }
};

}

#endif //RAMCLOUD_MIGRATIONCLIENT_H
