
#ifndef RAMCLOUD_MIGRATIONSEGMENTBUILDER_H
#define RAMCLOUD_MIGRATIONSEGMENTBUILDER_H

#include "MigrationPartition.pb.h"
#include "Common.h"
#include "Segment.h"
#include "Key.h"

namespace RAMCloud {

class MigrationSegmentBuilder {
  PUBLIC:

    static void build(const void *buffer, uint32_t length,
                      const SegmentCertificate &certificate,
                      Segment *migrationSegment, uint64_t migrationTableId,
                      uint64_t firstKeyHash, uint64_t lastKeyHash);

    static bool extractDigest(const void *buffer, uint32_t length,
                              const SegmentCertificate &certificate,
                              Buffer *digestBuffer, Buffer *tableStatsBuffer);

  PRIVATE:

    // Disallow construction.
    MigrationSegmentBuilder()
    {}
};


}
#endif //RAMCLOUD_MIGRATIONSEGMENTBUILDER_H
