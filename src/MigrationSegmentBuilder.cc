/* Copyright (c) 2009-2016 Stanford University
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

#include "MigrationSegmentBuilder.h"
#include "SegmentIterator.h"
#include "ShortMacros.h"
#include "ParticipantList.h"
#include "Object.h"
#include "RpcResult.h"
#include "PreparedOp.h"
#include "TxDecisionRecord.h"

namespace RAMCloud {

void
MigrationSegmentBuilder::build(
    const void *buffer, uint32_t length,
    const SegmentCertificate &certificate,
    Segment *migrationSegment, uint64_t migrationTableId,
    uint64_t firstKeyHash, uint64_t lastKeyHash)
{
    SegmentIterator it(buffer, length, certificate);
    it.checkMetadataIntegrity();

    // Buffer must be retained for iteration to provide storage for header.
    Buffer headerBuffer;
    const SegmentHeader *header = NULL;
    for (; !it.isDone(); it.next()) {
        LogEntryType type = it.getType();

        if (type == LOG_ENTRY_TYPE_SEGHEADER) {
            it.appendToBuffer(headerBuffer);
            header = headerBuffer.getStart<SegmentHeader>();
            continue;
        }
        if (type != LOG_ENTRY_TYPE_OBJ && type != LOG_ENTRY_TYPE_OBJTOMB)
            continue;

        if (header == NULL) {
            RAMCLOUD_DIE("Found log entry before header while "
                         "building recovery segments");
        }

        Buffer entryBuffer;
        it.appendToBuffer(entryBuffer);

        uint64_t tableId;
        KeyHash keyHash;
        if (type == LOG_ENTRY_TYPE_SAFEVERSION) {
            // Copy SAFEVERSION to all the partitions for safeVersion recovery
            // on all recovery masters
            if (!migrationSegment->append(type, entryBuffer)) {
                    LOG(WARNING, "Failure appending to a recovery segment "
                                 "for a replica of <%s,%lu>",
                        ServerId(header->logId).toString().c_str(),
                        header->segmentId);
                throw SegmentRecoveryFailedException(HERE);
            }
            continue;
        }

        if (type == LOG_ENTRY_TYPE_TXPLIST) {
            // Copy ParticipantLists all partitions that should own the entry.
            ParticipantList plist(entryBuffer);
            for (uint32_t i = 0; i < plist.getParticipantCount(); ++i) {
                tableId = plist.participants[i].tableId;
                keyHash = plist.participants[i].keyHash;
                if (tableId == migrationTableId && firstKeyHash <= keyHash &&
                    keyHash <= lastKeyHash) {

                    LogPosition position(header->segmentId, it.getOffset());
                    if (!migrationSegment->append(type, entryBuffer)) {
                            LOG(WARNING,
                                "Failure appending to a recovery segment "
                                "for a replica of <%s,%lu>",
                                ServerId(header->logId).toString().c_str(),
                                header->segmentId);
                        throw SegmentRecoveryFailedException(HERE);
                    }
                }
            }
            continue;
        }

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
        } else if (type == LOG_ENTRY_TYPE_RPCRESULT) {
            RpcResult rpcResult(entryBuffer);
            tableId = rpcResult.getTableId();
            keyHash = rpcResult.getKeyHash();
        } else if (type == LOG_ENTRY_TYPE_PREP) {
            PreparedOp op(entryBuffer, 0, entryBuffer.size());
            tableId = op.object.getTableId();
            keyHash = Key::getHash(tableId,
                                   op.object.getKey(),
                                   op.object.getKeyLength());
        } else if (type == LOG_ENTRY_TYPE_PREPTOMB) {
            PreparedOpTombstone opTomb(entryBuffer, 0);
            tableId = opTomb.header.tableId;
            keyHash = opTomb.header.keyHash;
        } else if (type == LOG_ENTRY_TYPE_TXDECISION) {
            TxDecisionRecord decisionRecord(entryBuffer);
            tableId = decisionRecord.getTableId();
            keyHash = decisionRecord.getKeyHash();
        } else {
                LOG(WARNING, "Unknown LogEntry (id=%u)", type);
            throw SegmentRecoveryFailedException(HERE);
        }
        if (!(tableId == migrationTableId && firstKeyHash <= keyHash &&
              keyHash <= lastKeyHash)) {
            continue;
        }

        if (!migrationSegment->append(type, entryBuffer)) {
                LOG(WARNING, "Failure appending to a recovery segment "
                             "for a replica of <%s,%lu>",
                    ServerId(header->logId).toString().c_str(),
                    header->segmentId);
            throw SegmentRecoveryFailedException(HERE);
        }
    }
}

bool MigrationSegmentBuilder::extractDigest(const void *buffer,
                                            uint32_t length,
                                            const SegmentCertificate &certificate,
                                            Buffer *digestBuffer,
                                            Buffer *tableStatsBuffer)
{
    // If the Segment is malformed somehow, just ignore it. The
    // coordinator will have to deal.
    SegmentIterator it(buffer, length, certificate);
    bool foundDigest = false;
    bool foundTableStats = false;
    try {
        it.checkMetadataIntegrity();
    } catch (SegmentIteratorException &e) {
            LOG(NOTICE,
                "Replica failed integrity check; skipping extraction of "
                "log digest: %s", e.str().c_str());
        return false;
    }
    while (!it.isDone()) {
        if (it.getType() == LOG_ENTRY_TYPE_LOGDIGEST) {
            digestBuffer->reset();
            it.appendToBuffer(*digestBuffer);
            foundDigest = true;
        }
        if (it.getType() == LOG_ENTRY_TYPE_TABLESTATS) {
            tableStatsBuffer->reset();
            it.appendToBuffer(*tableStatsBuffer);
            foundTableStats = true;
        }
        if (foundDigest && foundTableStats) {
            return true;
        }
        it.next();
    }
    return foundDigest;
}


}
