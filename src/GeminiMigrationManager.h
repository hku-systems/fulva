#ifndef RAMCLOUD_GEMINIMIGRATIONMANAGER_H
#define RAMCLOUD_GEMINIMIGRATIONMANAGER_H

#include "Dispatch.h"
#include "Context.h"
#include "ServerId.h"
#include "ObjectManager.h"

#include <unordered_set>

#define SPLIT_COPY

namespace RAMCloud {

class GeminiMigration;

class GeminiMigrationManager : Dispatch::Poller {
  public:
    explicit GeminiMigrationManager(Context *context, string localLocator);

    ~GeminiMigrationManager();

    int poll();

    bool startMigration(ServerId sourceServerId, ServerId targetServerId,
                        uint64_t tableId, uint64_t startKeyHash,
                        uint64_t endKeyHash);

    bool requestPriorityHash(uint64_t tableId, uint64_t startKeyHash,
                             uint64_t endKeyHash, uint64_t priorityHash);

    uint64_t updateRegularPullProgress(uint32_t i);

  PRIVATE:
    // Shared RAMCloud information.
    Context *context;

    Tub<ObjectManager::TombstoneProtector> tombstoneProtector;

    // Address of this RAMCloud master. Required by RocksteadyMigration.
    const string localLocator;

    // The list of in-progress migrations for which this RAMCloud master
    // is the destination.
    std::vector<GeminiMigration *> migrationsInProgress;

    bool active;

    uint64_t timestamp;
    uint64_t lastTime;
    uint64_t bandwidth;

    DISALLOW_COPY_AND_ASSIGN(GeminiMigrationManager);
};

class GeminiMigration {
  public:
    explicit GeminiMigration(Context *context, string localLocator,
                             ServerId sourceServerId, ServerId targetServerId,
                             uint64_t tableId, uint64_t startKeyHash,
                             uint64_t endKeyHash);

    ~GeminiMigration();

    int poll();

    bool addPriorityHash(uint64_t priorityHash);

  PRIVATE:

    int prepare();

    int pullAndReplay_main();

    int pullAndReplay_priorityHashes();

    int pullAndReplay_reapPullRpcs();

    int pullAndReplay_reapReplayRpcs();

    int pullAndReplay_sendPullRpcs();

    int pullAndReplay_sendReplayRpcs();

    int sideLogCommit();

    int tearDown();

    // Change as necessary.
    LogLevel ll = DEBUG;

    Context *context;

    TabletManager *tabletManager;

    ObjectManager *objectManager;

    const string localLocator;

    const ServerId sourceServerId;

    const ServerId targetServerId;

    const uint64_t tableId;

    const uint64_t startKeyHash;

    const uint64_t endKeyHash;

    enum MigrationPhase {
        SETUP,

        MIGRATING_DATA,

        SIDELOG_COMMIT,

        TEAR_DOWN,

        COMPLETED
    };

    MigrationPhase phase;

    uint64_t sourceNumHTBuckets;

    Tub<uint64_t> sourceSafeVersion;

    string sourceAuxLocator;

    Transport::SessionRef sourceSession;

    Tub<GeminiPrepForMigrationRpc> prepareSourceRpc;

    class GeminiGetHeadOfLog : public Transport::ServerRpc {
      public:
        explicit GeminiGetHeadOfLog(ServerId serverId, string localLocator)
            : Transport::ServerRpc(true), localLocator(localLocator),
              completed(false)
        {
            WireFormat::GetHeadOfLog::Request *reqHdr =
                requestPayload.emplaceAppend<
                    WireFormat::GetHeadOfLog::Request>();

            reqHdr->common.opcode =
                WireFormat::GetHeadOfLog::opcode;
            reqHdr->common.service =
                WireFormat::GetHeadOfLog::service;
            reqHdr->common.targetId = serverId.getId();
        }

        ~GeminiGetHeadOfLog()
        {}

        string getClientServiceLocator() override
        {
            return localLocator;
        }

        void sendReply() override
        {
            completed = true;
        }

        bool isReady()
        {
            return completed;
        }

        LogPosition wait()
        {
            uint32_t respHdrLength =
                sizeof32(WireFormat::GetHeadOfLog::Response);

            const WireFormat::GetHeadOfLog::Response *respHdr =
                reinterpret_cast<WireFormat::GetHeadOfLog::Response *>(
                    replyPayload.getRange(0, respHdrLength));

            return {respHdr->headSegmentId, respHdr->headSegmentOffset};
        }

      PRIVATE:
        string localLocator;

        bool completed;
    };

    Tub<GeminiGetHeadOfLog> getHeadOfLogRpc;

    Tub<RocksteadyTakeTabletOwnershipRpc> takeOwnershipRpc;

    static const uint32_t MAX_PRIORITY_HASHES = 16;

    SpinLock priorityLock;

    std::vector<uint64_t> waitingPriorityHashes;

    std::vector<uint64_t> inProgressPriorityHashes;

    std::unordered_set<uint64_t> finishedPriorityHashes;

    Tub<Buffer> priorityHashesRequestBuffer;

    Tub<Buffer> priorityHashesResponseBuffer;

    Tub<GeminiMigrationPriorityHashesRpc> priorityPullRpc;

    bool priorityHashesSideLogCommitted;

    Tub<SideLog> priorityHashesSideLog;

    static const uint32_t PARTITION_PIPELINE_DEPTH = 8;

    class GeminiHashPartition {
      public:
        explicit GeminiHashPartition(uint64_t startHTBucket,
                                     uint64_t endHTBucket)
            : startHTBucket(startHTBucket), endHTBucket(endHTBucket),
              currentHTBucket(startHTBucket), currentHTBucketEntry(0),
              totalPulledBytes(0), totalReplayedBytes(0), allDataPulled(false),
              pullRpcInProgress(false), numReplaysInProgress(0), rpcBuffers(),
              freePullBuffers(), freeReplayBuffers()
        {
            // In the beginning, all buffers can be used for pull requests
            // to the destination.
            for (uint32_t i = 0; i < PARTITION_PIPELINE_DEPTH; i++) {
                freePullBuffers.push_back(&(rpcBuffers[i]));
            }
        }

        ~GeminiHashPartition()
        {}

      PRIVATE:
        const uint64_t startHTBucket;

        const uint64_t endHTBucket;

        uint64_t currentHTBucket;

        uint64_t currentHTBucketEntry;

        uint64_t totalPulledBytes;

        uint64_t totalReplayedBytes;

        bool allDataPulled;

        bool pullRpcInProgress;

        uint32_t numReplaysInProgress;

        Tub<Buffer> rpcBuffers[PARTITION_PIPELINE_DEPTH];

        std::deque<Tub<Buffer> *>
            freePullBuffers;

        std::deque<Tub<Buffer> *>
            freeReplayBuffers;

        friend class GeminiMigration;
        friend class GeminiMigrationManager;
        DISALLOW_COPY_AND_ASSIGN(GeminiHashPartition);
    };

    static const uint32_t MAX_NUM_PARTITIONS = 8;

    Tub<GeminiHashPartition> partitions[MAX_NUM_PARTITIONS];

    uint32_t numCompletedPartitions;

    class GeminiPullRpc {
      public:
        GeminiPullRpc(Context *context, Transport::SessionRef session,
                      uint64_t tableId, uint64_t startKeyHash,
                      uint64_t endKeyHash,
                      uint64_t currentHTBucket,
                      uint64_t currentHTBucketEntry,
                      uint64_t endHTBucket, uint32_t numRequestedBytes,
                      Tub<Buffer> *response,
                      Tub<GeminiHashPartition> *partition)
            : partition(partition), responseBuffer(response), rpc()
        {
            rpc.construct(context, session, tableId, startKeyHash,
                          endKeyHash, currentHTBucket, currentHTBucketEntry,
                          endHTBucket, numRequestedBytes, response->get());
        }

        ~GeminiPullRpc()
        {
            rpc.destroy();
        }

      PRIVATE:
        Tub<GeminiHashPartition> *partition;

        Tub<Buffer> *responseBuffer;

        Tub<GeminiMigrationPullHashesRpc> rpc;

        friend class GeminiMigration;
        DISALLOW_COPY_AND_ASSIGN(GeminiPullRpc);
    };

    static const uint32_t MAX_PARALLEL_PULL_RPCS = 8;

    Tub<GeminiPullRpc> pullRpcs[MAX_PARALLEL_PULL_RPCS];

    std::deque<Tub<GeminiPullRpc> *>
        freePullRpcs;

    std::deque<Tub<GeminiPullRpc> *>
        busyPullRpcs;

    static const uint32_t MAX_PARALLEL_REPLAY_RPCS = 6;

    class GeminiReplayRpc : public Transport::ServerRpc {
      public:
        explicit GeminiReplayRpc(Tub<GeminiHashPartition> *partition,
                                 Tub<Buffer> *response,
                                 Tub<SideLog> *sideLog,
                                 string localLocator,
                                 SegmentCertificate certificate)
            : Transport::ServerRpc(true), partition(partition),
              responseBuffer(response), sideLog(sideLog),
              completed(false), localLocator(localLocator)
        {
            WireFormat::RocksteadyMigrationReplay::Request *reqHdr =
                requestPayload.emplaceAppend<
                    WireFormat::RocksteadyMigrationReplay::Request>();

            reqHdr->common.opcode =
                WireFormat::RocksteadyMigrationReplay::opcode;
            reqHdr->common.service =
                WireFormat::RocksteadyMigrationReplay::service;

            reqHdr->bufferPtr = reinterpret_cast<uintptr_t>(responseBuffer);
            reqHdr->sideLogPtr = reinterpret_cast<uintptr_t>(sideLog);
            reqHdr->certificate = certificate;
        }

        ~GeminiReplayRpc()
        {}

        void
        sendReply()
        {
            completed = true;
        }

        string
        getClientServiceLocator()
        {
            return this->localLocator;
        }

        bool
        isReady()
        {
            return completed;
        }

      PRIVATE:
        Tub<GeminiHashPartition> *partition;

        Tub<Buffer> *responseBuffer;

        Tub<SideLog> *sideLog;

        bool completed;

        const string localLocator;

        friend class GeminiMigration;
        DISALLOW_COPY_AND_ASSIGN(GeminiReplayRpc);
    };

    class GeminiPriorityReplayRpc : public Transport::ServerRpc {
      public:
        explicit GeminiPriorityReplayRpc(Tub<GeminiHashPartition> *
        partition, Tub<Buffer> *response, Tub<SideLog> *sideLog,
                                         string localLocator,
                                         SegmentCertificate certificate)
            : Transport::ServerRpc(true), partition(partition),
              responseBuffer(response), sideLog(sideLog),
              completed(false), localLocator(localLocator)
        {
            WireFormat::RocksteadyMigrationPriorityReplay::Request *reqHdr =
                requestPayload.emplaceAppend<
                    WireFormat::RocksteadyMigrationPriorityReplay::Request>();

            reqHdr->common.opcode =
                WireFormat::RocksteadyMigrationPriorityReplay::opcode;
            reqHdr->common.service =
                WireFormat::RocksteadyMigrationPriorityReplay::service;

            reqHdr->bufferPtr = reinterpret_cast<uintptr_t>(responseBuffer);
            reqHdr->sideLogPtr = reinterpret_cast<uintptr_t>(sideLog);
            reqHdr->certificate = certificate;
        }

        ~GeminiPriorityReplayRpc()
        {}

        void
        sendReply()
        {
            completed = true;
        }

        string
        getClientServiceLocator()
        {
            return this->localLocator;
        }

        bool
        isReady()
        {
            return completed;
        }

      PRIVATE:
        Tub<GeminiHashPartition> *partition;

        Tub<Buffer> *responseBuffer;

        Tub<SideLog> *sideLog;

        bool completed;

        const string localLocator;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(GeminiPriorityReplayRpc);
    };

    Tub<GeminiPriorityReplayRpc> priorityReplayRpc;

    Tub<GeminiReplayRpc> replayRpcs[MAX_PARALLEL_REPLAY_RPCS];

    std::deque<Tub<GeminiReplayRpc> *>
        freeReplayRpcs;

    std::deque<Tub<GeminiReplayRpc> *>
        busyReplayRpcs;

    Tub<SideLog> sideLogs[MAX_PARALLEL_REPLAY_RPCS];

    std::deque<Tub<SideLog> *>
        freeSideLogs;

    class GeminiSideLogCommitRpc : public Transport::ServerRpc {
      public:
        explicit GeminiSideLogCommitRpc(Tub<SideLog> *sideLog,
                                        string localLocator)
            : sideLog(sideLog), completed(false), localLocator(localLocator)
        {
            WireFormat::RocksteadySideLogCommit::Request *reqHdr =
                requestPayload.emplaceAppend<
                    WireFormat::RocksteadySideLogCommit::Request>();

            reqHdr->common.opcode =
                WireFormat::RocksteadySideLogCommit::opcode;
            reqHdr->common.service =
                WireFormat::RocksteadySideLogCommit::service;

            reqHdr->sideLogPtr = reinterpret_cast<uintptr_t>(sideLog);
        }

        ~GeminiSideLogCommitRpc()
        {}

        void
        sendReply()
        {
            completed = true;
        }

        bool
        isReady()
        {
            return completed;
        }

        string
        getClientServiceLocator()
        {
            return localLocator;
        }

      PRIVATE:
        Tub<SideLog> *sideLog;

        bool completed;

        const string localLocator;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(GeminiSideLogCommitRpc);
    };

    bool tookOwnership;

    bool droppedSourceTablet;

    Tub<RocksteadyDropSourceTabletRpc> dropSourceTabletRpc;

    uint32_t nextSideLogCommit;

    Tub<GeminiSideLogCommitRpc> sideLogCommitRpc;

    uint64_t migrationStartTS;

    uint64_t migrationEndTS;

    double migratedMegaBytes;

    uint64_t sideLogCommitStartTS;

    uint64_t sideLogCommitEndTS;

    friend class GeminiMigrationManager;
    DISALLOW_COPY_AND_ASSIGN(GeminiMigration);
};

}

#endif //RAMCLOUD_GEMINIMIGRATIONMANAGER_H
