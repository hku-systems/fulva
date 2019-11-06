#ifndef RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H
#define RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H

#include "Buffer.h"
#include "Context.h"
#include "SideLog.h"
#include "Dispatch.h"
#include "ServerId.h"
#include "Transport.h"
#include "LogMetadata.h"
#include "MasterClient.h"
#include "ObjectManager.h"
#include "TabletManager.h"
#include "CoordinatorClient.h"

// Uncomment to disable replay of migrated data. Useful for benchmarking
// RocksteadyMigrationPullHashesRpc() throughput. This can also be enabled
// by compiling RAMCloud with EXTRACXXFLAGS=-DROCKSTEADY_NO_REPLAY
// #define ROCKSTEADY_NO_REPLAY

// Uncomment to disable priority migration requests. Useful for benchmarking
// the impact of the background migration process. This can also be enabled
// by compiling RAMCloud with EXTRACXXFLAGS=-DROCKSTEADY_NO_PRIORITY_HASHES
// #define ROCKSTEADY_NO_PRIORITY_HASHES

// Uncomment to retain tablet ownership on the source during migration. This
// will result in an unsafe protocol because some writes to the tablet that
// were performed during the migration will never be transferred over. This
// can also be enabled by compiling RAMCloud with
// EXTRACXXFLAGS=-DROCKSTEADY_SOURCE_OWNS_TABLET
// #define ROCKSTEADY_SOURCE_OWNS_TABLET

// Uncomment to enable synchronous priority-hash requests on the read path.
// This will also disable batched priority pulls at the migration manager.
// This can also be enabled by compiling RAMCloud with
// EXTRACXXFLAGS=-DROCKSTEADY_SYNC_PRIORITY_HASHES
// #define ROCKSTEADY_SYNC_PRIORITY_HASHES

// Uncomment to disable bulk pulls and replays. Useful for benchmarking the
// priority pulls and replays. This can also be enabled by compiling RAMCloud
// with EXTRACXXFLAGS=-DROCKSTEADY_DISABLE_BACKGROUND
// #define ROCKSTEADY_DISABLE_BACKGROUND

namespace RAMCloud {

// Forward declaration.
class RocksteadyMigration;

class RocksteadyMigrationManager : Dispatch::Poller {
  public:
    explicit RocksteadyMigrationManager(Context *context, string localLocator);

    ~RocksteadyMigrationManager();

    int poll();

    bool startMigration(ServerId sourceServerId, uint64_t tableId,
                        uint64_t startKeyHash, uint64_t endKeyHash);

    bool requestPriorityHash(uint64_t tableId, uint64_t startKeyHash,
                             uint64_t endKeyHash, uint64_t priorityHash);

  PRIVATE:
    // Shared RAMCloud information.
    Context *context;

    Tub <ObjectManager::TombstoneProtector> tombstoneProtector;

    // Address of this RAMCloud master. Required by RocksteadyMigration.
    const string localLocator;

    // The list of in-progress migrations for which this RAMCloud master
    // is the destination.
    std::vector<RocksteadyMigration *> migrationsInProgress;

    bool active;

    uint64_t timestamp;
    uint64_t lastTime;
    uint64_t bandwidth;

    DISALLOW_COPY_AND_ASSIGN(RocksteadyMigrationManager);
};

class RocksteadyMigration {
  public:
    explicit RocksteadyMigration(Context *context, string localLocator,
                                 ServerId sourceServerId, uint64_t tableId,
                                 uint64_t startKeyHash,
                                 uint64_t endKeyHash);

    ~RocksteadyMigration();

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

    void pullAndReplay_checkEfficiency();

    int sideLogCommit();

    int tearDown();

    // Change as necessary.
    LogLevel ll = DEBUG;

    Context *context;

    TabletManager *tabletManager;

    ObjectManager *objectManager;

    const string localLocator;

    const ServerId sourceServerId;

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

    Tub <uint64_t> sourceSafeVersion;

    Tub <RocksteadyPrepForMigrationRpc> prepareSourceRpc;

    class RocksteadyGetHeadOfLog : public Transport::ServerRpc {
      public:
        explicit RocksteadyGetHeadOfLog(ServerId serverId, string localLocator)
            : localLocator(localLocator), completed(false)
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

        ~RocksteadyGetHeadOfLog()
        {}

        string getClientServiceLocator()
        {
            return localLocator;
        }

        void sendReply()
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

    Tub <RocksteadyGetHeadOfLog> getHeadOfLogRpc;

    Tub <RocksteadyTakeTabletOwnershipRpc> takeOwnershipRpc;

    static const uint32_t MAX_PRIORITY_HASHES = 16;

    SpinLock priorityLock;

    std::vector<uint64_t> waitingPriorityHashes;

    std::vector<uint64_t> inProgressPriorityHashes;

    Tub <Buffer> priorityHashesRequestBuffer;

    Tub <Buffer> priorityHashesResponseBuffer;

    Tub <RocksteadyMigrationPriorityHashesRpc> priorityPullRpc;

    bool priorityHashesSideLogCommitted;

    Tub <SideLog> priorityHashesSideLog;

    static const uint32_t PARTITION_PIPELINE_DEPTH = 8;

    class RocksteadyHashPartition {
      public:
        explicit RocksteadyHashPartition(uint64_t startHTBucket,
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

        ~RocksteadyHashPartition()
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

        Tub <Buffer> rpcBuffers[PARTITION_PIPELINE_DEPTH];

        std::deque<Tub < Buffer>*>
        freePullBuffers;

        std::deque<Tub < Buffer>*>
        freeReplayBuffers;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyHashPartition);
    };

    static const uint32_t MAX_NUM_PARTITIONS = 8;

    Tub <RocksteadyHashPartition> partitions[MAX_NUM_PARTITIONS];

    uint32_t numCompletedPartitions;

    class RocksteadyPullRpc {
      public:
        RocksteadyPullRpc(Context *context, ServerId sourceServerId,
                          uint64_t tableId, uint64_t startKeyHash,
                          uint64_t endKeyHash,
                          uint64_t currentHTBucket,
                          uint64_t currentHTBucketEntry,
                          uint64_t endHTBucket, uint32_t numRequestedBytes,
                          Tub <Buffer> *response,
                          Tub <RocksteadyHashPartition> *partition)
            : partition(partition), responseBuffer(response), rpc()
        {
            rpc.construct(context, sourceServerId, tableId, startKeyHash,
                          endKeyHash, currentHTBucket, currentHTBucketEntry,
                          endHTBucket, numRequestedBytes, response->get());
        }

        ~RocksteadyPullRpc()
        {
            rpc.destroy();
        }

      PRIVATE:
        Tub <RocksteadyHashPartition> *partition;

        Tub <Buffer> *responseBuffer;

        Tub <RocksteadyMigrationPullHashesRpc> rpc;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyPullRpc);
    };

    static const uint32_t MAX_PARALLEL_PULL_RPCS = 8;

    Tub <RocksteadyPullRpc> pullRpcs[MAX_PARALLEL_PULL_RPCS];

    std::deque<Tub < RocksteadyPullRpc>*>
    freePullRpcs;

    std::deque<Tub < RocksteadyPullRpc>*>
    busyPullRpcs;

    static const uint32_t MAX_PARALLEL_REPLAY_RPCS = 6;

    class RocksteadyReplayRpc : public Transport::ServerRpc {
      public:
        explicit RocksteadyReplayRpc(Tub <RocksteadyHashPartition> *partition,
                                     Tub <Buffer> *response,
                                     Tub <SideLog> *sideLog,
                                     string localLocator,
                                     SegmentCertificate certificate)
            : partition(partition), responseBuffer(response), sideLog(sideLog),
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

        ~RocksteadyReplayRpc()
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
        Tub <RocksteadyHashPartition> *partition;

        Tub <Buffer> *responseBuffer;

        Tub <SideLog> *sideLog;

        bool completed;

        const string localLocator;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyReplayRpc);
    };

    class RocksteadyPriorityReplayRpc : public Transport::ServerRpc {
      public:
        explicit RocksteadyPriorityReplayRpc(Tub <RocksteadyHashPartition> *
        partition, Tub <Buffer> *response, Tub <SideLog> *sideLog,
                                             string localLocator,
                                             SegmentCertificate certificate)
            : partition(partition), responseBuffer(response), sideLog(sideLog),
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

        ~RocksteadyPriorityReplayRpc()
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
        Tub <RocksteadyHashPartition> *partition;

        Tub <Buffer> *responseBuffer;

        Tub <SideLog> *sideLog;

        bool completed;

        const string localLocator;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadyPriorityReplayRpc);
    };

    Tub <RocksteadyPriorityReplayRpc> priorityReplayRpc;

    Tub <RocksteadyReplayRpc> replayRpcs[MAX_PARALLEL_REPLAY_RPCS];

    std::deque<Tub < RocksteadyReplayRpc>*>
    freeReplayRpcs;

    std::deque<Tub < RocksteadyReplayRpc>*>
    busyReplayRpcs;

    Tub <SideLog> sideLogs[MAX_PARALLEL_REPLAY_RPCS];

    std::deque<Tub < SideLog>*>
    freeSideLogs;

    class RocksteadySideLogCommitRpc : public Transport::ServerRpc {
      public:
        explicit RocksteadySideLogCommitRpc(Tub <SideLog> *sideLog,
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

        ~RocksteadySideLogCommitRpc()
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
        Tub <SideLog> *sideLog;

        bool completed;

        const string localLocator;

        friend class RocksteadyMigration;
        DISALLOW_COPY_AND_ASSIGN(RocksteadySideLogCommitRpc);
    };

    bool tookOwnership;

    bool droppedSourceTablet;

    Tub <RocksteadyDropSourceTabletRpc> dropSourceTabletRpc;

    uint32_t nextSideLogCommit;

    Tub <RocksteadySideLogCommitRpc> sideLogCommitRpc;

    uint64_t migrationStartTS;

    uint64_t migrationEndTS;

    double migratedMegaBytes;

    uint64_t sideLogCommitStartTS;

    uint64_t sideLogCommitEndTS;

#ifdef PRIORITY_PULL_BREAKDOWN
    uint64_t lastUpdate;
    uint64_t totalNumber;
    uint64_t priorityPullTime;
#endif

    friend class RocksteadyMigrationManager;
    DISALLOW_COPY_AND_ASSIGN(RocksteadyMigration);
};

}  // namespace RAMCloud

#endif  // RAMCLOUD_ROCKSTEADYMIGRATIONMANAGER_H
