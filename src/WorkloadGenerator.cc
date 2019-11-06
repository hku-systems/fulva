#include <cstring>
#include <string>
#include "WorkloadGenerator.h"
#include "RawMetrics.h"

namespace RAMCloud {

WorkloadGenerator::WorkloadGenerator(
    std::string workloadName, uint64_t targetOps,
    uint32_t objectCount, uint32_t objectSize, Client *client)
    : workloadName(workloadName), targetOps(targetOps),
      objectCount(objectCount),
      objectSize(objectSize), readPercent(100),
      generator(), client(client), experimentStartTime(0), samples()
{
    if (workloadName == "YCSB-A") {
        readPercent = 50;
    } else if (workloadName == "YCSB-B") {
        readPercent = 95;
    } else if (workloadName == "YCSB-C") {
        readPercent = 100;
    } else if (workloadName == "WRITE-ONLY") {
        readPercent = 0;
    } else {
        throw std::runtime_error("unknown workload type");
    }

    generator = new ZipfianGenerator(objectCount);
}

std::string WorkloadGenerator::debugString() const
{
    std::stringstream ss;
    ss << "Workload: " << workloadName << std::endl
       << "Record Count: " << objectCount << std::endl
       << "Record Size: " << objectSize << std::endl
       << "Total Table Size: "
       << uint64_t(objectCount) * objectSize / (1lu << 20) << std::endl
       << "Read Percentage: " << readPercent << std::endl;
    std::string ret;
    ss >> ret;
    return ret;
}

void WorkloadGenerator::run(bool issueMigration)
{
    const uint16_t keyLen = 30;
    char key[keyLen];
    char value[objectSize];
    uint64_t targetNSPO = 0;
    uint64_t opCount = 0;
    uint64_t targetMissCount = 0;
    uint64_t readCount = 0;
    uint64_t writeCount = 0;
    uint64_t finishTryTime = 1;
    const uint64_t hundredMS = Cycles::fromMicroseconds(100000);
    const uint64_t oneSecond = Cycles::fromSeconds(1);

    if (targetOps > 0) {
        targetNSPO = 1000000000 / targetOps;
        // Randomize start time
        Cycles::sleep(static_cast<uint64_t>((rand() % targetNSPO) / 1000));
    }
    client->setup(objectCount, objectSize);

    int readThreshold = (RAND_MAX / 100) * readPercent;

    uint64_t stop = 0;
    uint64_t nextStop = 0;
    experimentStartTime = Cycles::rdtsc();
    uint64_t timestamp = 0;
    uint64_t lastTime = experimentStartTime;
    uint64_t incomingBandwidth = metrics->transport.receive.byteCount;
    uint64_t outcomingBandwidth = metrics->transport.transmit.byteCount;

    while (true) {

        memset(key, 0, keyLen);
        string keyStr = std::to_string(generator->nextNumber());
        SampleType type;
        uint64_t start = Cycles::rdtsc();

        if (rand() <= readThreshold) {
            client->read(keyStr.c_str(), keyStr.length());
            readCount++;
            type = READ;
        } else {
            client->write(keyStr.c_str(), keyStr.length(), value, objectSize);
            writeCount++;
            type = WRITE;
        }
        opCount++;
        stop = Cycles::rdtsc();
        if (Cycles::toMicroseconds(stop - lastTime) > 100000) {
            uint64_t currentIncoming = metrics->transport.receive.byteCount;
            uint64_t currentOutcoming = metrics->transport.transmit.byteCount;
            timestamp++;

            RAMCLOUD_LOG(NOTICE, "%lu: %lf, %lf", timestamp,
                         static_cast<double > (currentIncoming -
                                               incomingBandwidth) / 1024 / 102,
                         static_cast<double >                        (
                             currentOutcoming - outcomingBandwidth) / 1024 /
                         102);

            incomingBandwidth = currentIncoming;
            outcomingBandwidth = currentOutcoming;
            lastTime = stop;
        }
        samples.emplace_back(start, stop, type,
                             Key::getHash(client->getTableId(), keyStr.c_str(),
                                          (uint16_t)keyStr.length()));

        if (issueMigration && stop > experimentStartTime + oneSecond) {
            issueMigration = false;
            client->startMigration();
        }
        if (stop > experimentStartTime + finishTryTime * hundredMS) {
            finishTryTime++;
            if (client->isFinished())
                break;
        }

        if (targetNSPO > 0) {
            nextStop = experimentStartTime +
                       Cycles::fromNanoseconds(
                           (opCount * targetNSPO) +
                           (rand() % targetNSPO) -
                           (targetNSPO / 2));
            if (Cycles::rdtsc() > nextStop) {
                targetMissCount++;
            }
            while (Cycles::rdtsc() < nextStop);
        }


    }
}

void WorkloadGenerator::statistics(
    vector<WorkloadGenerator::TimeDist> &result, SampleType type, int tablet)
{
    std::map<uint64_t, vector<uint64_t> *> latency;
    for (Sample &sample: samples) {
        uint64_t timestamp = Cycles::toMicroseconds(
            sample.startTicks - experimentStartTime) / 1000 / 100;
        if (latency.find(timestamp) == latency.end()) {
            for (uint64_t i = latency.size(); i <= timestamp; i++)
                latency[i] = new vector<uint64_t>();
        }

        if (tablet == 1 && sample.hash > client->splitHash())
            continue;

        if (tablet == 2 && sample.hash <= client->splitHash())
            continue;

        if (sample.type == type || type == ALL)
            latency[timestamp]->push_back(
                sample.endTicks - sample.startTicks);
    }

    for (uint64_t i = 0; i < latency.size(); i++) {
        result.emplace_back();
        getDist(*latency[i], &result.back());
    }
}


}
