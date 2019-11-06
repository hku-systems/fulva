/* Copyright (c) 2009-2015 Stanford University
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

#ifndef RAMCLOUD_BACKUPMASTERMIGRATION_H
#define RAMCLOUD_BACKUPMASTERMIGRATION_H

#include "TaskQueue.h"
#include "ServerId.h"
#include "BackupMasterRecovery.h"


namespace RAMCloud {

class BackupMasterMigration : public Task {

  PUBLIC:
    typedef WireFormat::MigrationStartReading::Response StartResponse;

    BackupMasterMigration(TaskQueue &taskQueue,
                          uint64_t migrationId,
                          ServerId sourceServerId,
                          ServerId targetServerId,
                          uint64_t tableId,
                          uint64_t firstKeyHash,
                          uint64_t lastKeyHash,
                          uint32_t segmentSize,
                          uint32_t readSpeed,
                          uint32_t maxReplicasInMemory);

    ~BackupMasterMigration();

    void start(const std::vector<BackupStorage::FrameRef> &frames,
               Buffer *buffer,
               StartResponse *response);

    void setPartitionsAndSchedule();

    Status getRecoverySegment(uint64_t migrationId,
                              uint64_t segmentId,
                              Buffer *buffer,
                              SegmentCertificate *certificate);

    void free();

    uint64_t getMigrationId();

    void performTask();

  PRIVATE:

    void populateStartResponse(Buffer *responseBuffer, StartResponse *response);

    struct Replica;

    bool getLogDigest(Replica &replica, Buffer *digestBuffer);

    uint64_t migrationId;

    ServerId sourceServerId;
    ServerId targetServerId;
    uint64_t tableId;
    uint64_t firstKeyHash;
    uint64_t lastKeyHash;

    uint32_t segmentSize;

    struct Replica {
        explicit Replica(const BackupStorage::FrameRef &frame);

        ~Replica();

        BackupStorage::FrameRef frame;

        const BackupReplicaMetadata *metadata;

        std::unique_ptr<Segment> migrationSegment;

        std::unique_ptr<SegmentRecoveryFailedException> recoveryException;

        bool built;

        Atomic<uint64_t> lastAccessTime;

        Atomic<int> refCount;

        Atomic<int> fetchCount;

        void *head;

        DISALLOW_COPY_AND_ASSIGN(Replica);
    };

    std::deque<Replica> replicas;
    std::unordered_map<uint64_t, Replica *> segmentIdToReplica;

    class CyclicReplicaBuffer {
      PUBLIC:

        CyclicReplicaBuffer(uint32_t maxReplicasInMemory, uint32_t segmentSize,
                            uint32_t readSpeed,
                            BackupMasterMigration *migration);

        ~CyclicReplicaBuffer();

        size_t size();

        bool contains(Replica *replica);

        enum Priority {
            NORMAL, HIGH
        };

        void enqueue(Replica *replica, Priority priority);

        bool bufferNext();

        bool buildNext();

        void logState();

        class ActiveReplica {
          PUBLIC:

            ActiveReplica(Replica *replica, CyclicReplicaBuffer *buffer)
                : replica(replica), buffer(buffer)
            {
                SpinLock::Guard lock(buffer->mutex);
                replica->refCount++;
                replica->lastAccessTime = Cycles::rdtsc();
            }

            ~ActiveReplica()
            {
                replica->refCount--;
            }

          PRIVATE:
            Replica *replica;
            CyclicReplicaBuffer *buffer;

            DISALLOW_COPY_AND_ASSIGN(ActiveReplica);
        };


      PRIVATE:
        SpinLock mutex;

        uint32_t maxReplicasInMemory;

        std::vector<Replica *> inMemoryReplicas;

        size_t oldestReplicaIdx;

        std::deque<Replica *> normalPriorityQueuedReplicas;

        std::deque<Replica *> highPriorityQueuedReplicas;

        double bufferReadTime;

        BackupMasterMigration *migration;

        DISALLOW_COPY_AND_ASSIGN(CyclicReplicaBuffer);
    };

    CyclicReplicaBuffer replicaBuffer;

    Buffer logDigest;

    uint64_t logDigestSegmentId;

    uint64_t logDigestSegmentEpoch;

    Buffer tableStatsDigest;

    bool startCompleted;

    Tub<CycleCounter<RawMetric>> recoveryTicks;

    Tub<CycleCounter<RawMetric>> readingDataTicks;

    uint64_t buildingStartTicks;

    bool (*testingExtractDigest)(uint64_t segmentId,
                                 Buffer *digestBuffer,
                                 Buffer *tableStatsBuffer);

    bool testingSkipBuild;

    TaskKiller destroyer;

    bool pendingDeletion;

    static SpinLock deletionMutex;

    DISALLOW_COPY_AND_ASSIGN(BackupMasterMigration);

  PUBLIC:
    uint64_t startTime;
};

}

#endif //RAMCLOUD_BACKUPMASTERMIGRATION_H
