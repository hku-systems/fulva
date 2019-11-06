
#ifndef RAMCLOUD_MIGRATIONBACKUPMANAGER_H
#define RAMCLOUD_MIGRATIONBACKUPMANAGER_H

#include "BackupMasterRecovery.h"
#include "Dispatch.h"

namespace RAMCloud {

class SegmentManager;

class SegmentIterator;

class MigrationBackupManager : public Dispatch::Poller {

  PUBLIC:

    class Migration;

    class Chunk {
      PUBLIC:
        Buffer buffer;
        Crc32C checksum;

        Chunk() : buffer(), checksum()
        {

        };

        void append(LogEntryType type, uint32_t length, Buffer &entryBuffer);


        void createSegmentCertificate(SegmentCertificate *certificate);

        uint32_t size();

        DISALLOW_COPY_AND_ASSIGN(Chunk);

    };

    class Replica {

      PUBLIC:
        BackupStorage::FrameRef frame;

        Migration *migration;

        const BackupReplicaMetadata *metadata;

        std::deque<Chunk *> chunks;

        std::unique_ptr<SegmentRecoveryFailedException> recoveryException;

        bool built;

        Atomic<uint64_t> lastAccessTime;

        Atomic<int> refCount;

        Atomic<int> fetchCount;

        void *head;

        uint32_t ackId;

        void *data;
        uint32_t currentOffset;
        bool done;
        uint64_t startTime;

        explicit Replica(const BackupStorage::FrameRef &frame,
                         Migration *migration);

        ~Replica();

        void load();

        void filter();

        void filter_prepare();

        void filter_scan();

        void filter_finish();

        DISALLOW_COPY_AND_ASSIGN(Replica);
    };

    class Migration {

      PRIVATE:
        MigrationBackupManager *manager;
        Context *context;
        SegmentManager *segmentManager;
        uint64_t migrationId;
        ServerId sourceServerId;
        ServerId targetServerId;
        uint64_t tableId;
        uint64_t firstKeyHash;
        uint64_t lastKeyHash;
        uint32_t segmentSize;

        enum Phase {
            LOADING,

            SERVING,

            COMPLETED
        };

        Phase phase;
        std::deque<Replica> replicas;
        std::deque<Replica>::iterator replicasIterator;
        std::deque<Replica *> replicaToFilter;
        std::unordered_map<uint64_t, Replica *> segmentIdToReplica;

        uint32_t replicaNum;
        uint32_t completedReplicaNum;

        static const uint32_t MAX_BATCH_SIZE = 40 * 1024;
        static const uint32_t MAX_SCAN_SIZE = 80 * 1024;

        class LoadRpc : public Transport::ServerRpc {
          PRIVATE:
            const string localLocator;
            Replica *replica;
          PUBLIC:

            LoadRpc(string localLocator, Replica *replica)
                : localLocator(localLocator), replica(replica)
            {
                WireFormat::MigrationLoad::Request *reqHdr =
                    requestPayload.emplaceAppend<
                        WireFormat::MigrationLoad::Request>();
                reqHdr->common.opcode =
                    WireFormat::MigrationLoad::opcode;
                reqHdr->common.service =
                    WireFormat::MigrationLoad::service;

                reqHdr->replicaPtr = reinterpret_cast<uintptr_t>(replica);
            }

            bool isReady()
            {
                return replica->head != NULL || replica->frame->isLoaded();
            }

            void sendReply()
            {
            }

            string getClientServiceLocator()
            {
                return this->localLocator;
            }

            DISALLOW_COPY_AND_ASSIGN(LoadRpc);

            friend class Migration;
        };

        static const uint32_t MAX_PARALLEL_LOAD_RPCS = 1;

        Tub<LoadRpc> loadRpcs[MAX_PARALLEL_LOAD_RPCS];

        std::deque<Tub<LoadRpc> *> freeLoadRpcs;

        std::deque<Tub<LoadRpc> *> busyLoadRpcs;

        class FilterRpc : public Transport::ServerRpc {
          PRIVATE:
            bool completed;

            const string localLocator;

            Replica *replica;
          PUBLIC:

            FilterRpc(string localLocator, Replica *replica)
                : completed(false), localLocator(localLocator), replica(replica)
            {
                WireFormat::MigrationFilter::Request *reqHdr =
                    requestPayload.emplaceAppend<
                        WireFormat::MigrationFilter::Request>();
                reqHdr->common.opcode =
                    WireFormat::MigrationFilter::opcode;
                reqHdr->common.service =
                    WireFormat::MigrationFilter::service;

                reqHdr->replicaPtr = reinterpret_cast<uintptr_t>(replica);
            }

            ~FilterRpc()
            {

            }

            void sendReply()
            {
                completed = true;
            }

            string getClientServiceLocator()
            {
                return this->localLocator;
            }

            bool isReady()
            {
                return completed;
            }

            DISALLOW_COPY_AND_ASSIGN(FilterRpc);

            friend class Migration;
        };

        static const uint32_t MAX_PARALLEL_FILTER_RPCS = 1;

        Tub<FilterRpc> filterRpcs[MAX_PARALLEL_FILTER_RPCS];

        std::deque<Tub<FilterRpc> *> freeFilterRpcs;

        std::deque<Tub<FilterRpc> *> busyFilterRpcs;


        int loadAndFilter_main();

        int loadAndFilter_reapLoadRpcs();

        int loadAndFilter_reapFilterRpcs();

        int loadAndFilter_sendLoadRpcs();

        int loadAndFilter_sendFilterRpcs();

      PUBLIC:

        Migration(MigrationBackupManager *manager,
                  uint64_t migrationId,
                  uint64_t sourceId,
                  uint64_t targetId,
                  uint64_t tableId,
                  uint64_t firstKeyHash,
                  uint64_t lastKeyHash,
                  const std::vector<BackupStorage::FrameRef> &frames);

        int poll();

        bool getSegment(uint64_t segmentId, uint32_t seqId, Buffer *buffer,
                        SegmentCertificate *certificate);

        friend class MigrationBackupManager;
        DISALLOW_COPY_AND_ASSIGN(Migration)
    };

  PUBLIC:

    MigrationBackupManager(Context *context, string localLocator,
                           uint32_t segmentSize);

    int poll();

    void start(
        uint64_t migrationId, uint64_t sourceId, uint64_t targetId,
        uint64_t tableId, uint64_t firstKeyHash, uint64_t lastKeyHash,
        const std::vector<BackupStorage::FrameRef> &frames);

    bool getSegment(
        uint64_t migrationId, uint64_t segmentId, uint32_t seqId,
        Buffer *buffer, SegmentCertificate *certificate);

  PRIVATE:
    Context *context;
    string localLocator;
    uint32_t segmentSize;
    std::vector<Migration *> migrationsInProgress;
    std::unordered_map<uint64_t, Migration *> migrationMap;

    DISALLOW_COPY_AND_ASSIGN(MigrationBackupManager);

};

}

#endif //RAMCLOUD_MIGRATIONBACKUPMANAGER_H
