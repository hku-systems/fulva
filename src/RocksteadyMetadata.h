#ifndef RAMCLOUD_ROCKSTEADYMETADATA_H
#define RAMCLOUD_ROCKSTEADYMETADATA_H

#include "Segment.h"
#include "LogMetadata.h"
#include "LogEntryTypes.h"

namespace RAMCloud {

class RocksteadyBufferCertificate {
  public:
    explicit RocksteadyBufferCertificate()
        : checksum()
    {}

    ~RocksteadyBufferCertificate()
    {}

    void
    updateChecksum(LogEntryType type, uint32_t length)
    {
        Segment::EntryHeader entryHeader(type, length);

        checksum.update(&entryHeader, sizeof(entryHeader));
        checksum.update(&length, entryHeader.getLengthBytes());
    }

    void
    createSegmentCertificate(uint32_t length, SegmentCertificate* certificate)
    {
        certificate->segmentLength = length;

        checksum.update(certificate, static_cast<unsigned>(
                sizeof(*certificate) - sizeof(certificate->checksum)));

        certificate->checksum = checksum.getResult();
    }

  PRIVATE:
    Crc32C checksum;

    DISALLOW_COPY_AND_ASSIGN(RocksteadyBufferCertificate);
};

}

#endif // RAMCLOUD_ROCKSTEADYMETADATA_H
