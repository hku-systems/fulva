#ifndef RAMCLOUD_WORKLOADGENERATOR_H
#define RAMCLOUD_WORKLOADGENERATOR_H

#include <string>
#include <stdexcept>
#include <sstream>
#include <cmath>
#include <vector>
#include <algorithm>
#include <list>

#include "Cycles.h"
#include "RamCloud.h"
#include "PerfStats.h"

namespace RAMCloud {


class WorkloadGenerator {
  public:
    class Client {
      public:

        virtual ~Client()
        {
        }

        virtual void setup(uint32_t objectCount, uint32_t objectSize) = 0;

        virtual void read(const char *key, uint64_t keyLen) = 0;

        virtual void
        write(const char *key, uint64_t keyLen, char *value,
              uint32_t valueLen) = 0;

        virtual void startMigration() = 0;

        virtual bool isFinished() = 0;

        virtual RamCloud *getRamCloud() = 0;

        virtual uint64_t getTableId() = 0;

        virtual uint64_t splitHash()
        { return 0; }
    };

    struct TimeDist {
        uint64_t min;
        uint64_t avg;
        uint64_t p50;
        uint64_t p90;
        uint64_t p99;
        uint64_t p999;
        uint64_t p9999;
        uint64_t p99999;
        uint64_t bandwidth;
    };

    enum SampleType {
        READ, WRITE, ALL
    };

  private:
    class ZipfianGenerator {
      public:
        explicit ZipfianGenerator(uint64_t n, double theta = 0.99)
            : n(n), theta(theta), alpha(1 / (1 - theta)), zetan(zeta(n, theta)),
              eta((1 - std::pow(2.0 / static_cast<double>(n), 1 - theta)) /
                  (1 - zeta(2, theta) / zetan))
        {}

        uint64_t nextNumber()
        {
            double u =
                static_cast<double>(rand()) / static_cast<double>(RAND_MAX);
            double uz = u * zetan;
            if (uz < 1)
                return 0;
            if (uz < 1 + std::pow(0.5, theta))
                return 1;
            return 0 + static_cast<uint64_t>(static_cast<double>(n) *
                                             std::pow(eta * u - eta + 1.0,
                                                      alpha));
        }

      private:
        const uint64_t n;
        const double theta;
        const double alpha;
        const double zetan;
        const double eta;

        static double zeta(uint64_t n, double theta)
        {
            double sum = 0;
            for (uint64_t i = 0; i < n; i++) {
                sum = sum + 1.0 / (std::pow(i + 1, theta));
            }
            return sum;
        }
    };

    struct Sample {
        uint64_t startTicks;
        uint64_t endTicks;
        SampleType type;
        uint64_t hash;

        Sample(uint64_t startTicks,
               uint64_t endTicks,
               SampleType type)
            : Sample(startTicks, endTicks, type, 0)
        {

        }

        Sample(uint64_t startTicks,
               uint64_t endTicks,
               SampleType type,
               uint64_t hash)
            : startTicks(startTicks), endTicks(endTicks), type(type),
              hash(hash)
        {}

    };

    std::string workloadName;
    uint64_t targetOps;
    uint32_t objectCount;
    uint32_t objectSize;
    int readPercent;
    ZipfianGenerator *generator;
    Client *client;
    uint64_t experimentStartTime;

    std::vector<Sample> samples;

    void getDist(std::vector<uint64_t> &times, TimeDist *dist)
    {
        int count = static_cast<int>(times.size());
        std::sort(times.begin(), times.end());
        dist->avg = 0;
        dist->min = 0;
        uint64_t last = 0;
        uint64_t sum = 0;

        for (uint64_t time: times) {
            sum += Cycles::toMicroseconds(time);
        }

        if (count > 0) {
            dist->avg = sum / count;
            dist->min = Cycles::toMicroseconds(times[0]);
            last = times.back();
        }


        dist->bandwidth = times.size();
        int index = count / 2;
        if (index < count) {
            dist->p50 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p50 = last;
        }
        index = count - (count + 5) / 10;
        if (index < count) {
            dist->p90 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p90 = last;
        }
        index = count - (count + 50) / 100;
        if (index < count) {
            dist->p99 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p99 = last;
        }
        index = count - (count + 500) / 1000;
        if (index < count) {
            dist->p999 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p999 = last;
        }
        index = count - (count + 5000) / 10000;
        if (index < count) {
            dist->p9999 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p9999 = last;
        }
        index = count - (count + 50000) / 100000;
        if (index < count) {
            dist->p99999 = Cycles::toMicroseconds(times.at(index));
        } else {
            dist->p99999 = last;
        }
    }

  public:

    explicit WorkloadGenerator(std::string workloadName, uint64_t targetOps,
                               uint32_t objectCount,
                               uint32_t objectSize, Client *client);

    std::string debugString() const;

    void run(bool issueMigration);

    template<class ReadOp>
    void asyncRun(bool issueMigration)
    {
        char value[objectSize];
        uint64_t finishTryTime = 1;
        uint64_t stop = 0;
        const uint64_t hundredMS = Cycles::fromMicroseconds(100000);
        const uint64_t oneSecond = Cycles::fromSeconds(1);

        client->setup(objectCount, objectSize);

        int readThreshold = (RAND_MAX / 100) * readPercent;

        RamCloud *ramcloud = client->getRamCloud();
        uint64_t tableId = client->getTableId();

        std::list<Buffer *> freeBuffers;
        int concurrency = 2;
        for (int i = 0; i < concurrency; i++) {
            freeBuffers.push_back(new Buffer());
        }

        int operationInFlight = 0;

        std::list<std::pair<ReadOp *, std::pair<uint64_t, Buffer *> >> readQueue;
        std::list<std::pair<WriteRpc *, uint64_t> > writeQueue;

        experimentStartTime = Cycles::rdtsc();
        RAMCLOUD_LOG(WARNING, "benchmark start");

        uint64_t timestamp = 0;
        uint64_t bandwidth = PerfStats::threadStats.networkOutputBytes +
                             PerfStats::threadStats.networkInputBytes;
        uint64_t lastTime = experimentStartTime;

        while (true) {

            string keyStr = std::to_string(generator->nextNumber());
            if (operationInFlight < concurrency) {
                operationInFlight++;
                if (rand() <= readThreshold) {
                    Buffer *buffer = freeBuffers.front();
                    freeBuffers.pop_front();
                    readQueue.emplace_back(
                        new ReadOp(ramcloud, tableId, keyStr.c_str(),
                                   (uint16_t) keyStr.size(),
                                   buffer),
                        std::pair<uint64_t, Buffer *>(Cycles::rdtsc(), buffer));
                } else {
                    writeQueue.emplace_back(
                        new WriteRpc(ramcloud, tableId, keyStr.c_str(),
                                     (uint16_t) keyStr.size(), value,
                                     objectSize),
                        Cycles::rdtsc());
                }
            }

            bool exists;
            ramcloud->poll();
            for (auto op = readQueue.begin(); op != readQueue.end();) {
                if (op->first->isReady()) {
                    op->first->wait(NULL, &exists);

                    stop = Cycles::rdtsc();
                    samples.emplace_back(op->second.first, stop, READ,
                                         op->first->keyHash);
                    freeBuffers.push_back(op->second.second);
                    operationInFlight--;
                    delete op->first;
                    op = readQueue.erase(op);
                } else {
                    op++;
                }
            }

            for (auto op = writeQueue.begin(); op != writeQueue.end();) {
                if (op->first->isReady()) {
                    op->first->wait();
                    stop = Cycles::rdtsc();
                    samples.emplace_back(op->second, stop, WRITE,
                                         op->first->keyHash);
                    operationInFlight--;
                    delete op->first;
                    op = writeQueue.erase(op);
                } else {
                    op++;
                }
            }

            stop = Cycles::rdtsc();
            if (Cycles::toMicroseconds(stop - lastTime) > 100000) {
                uint64_t current = PerfStats::threadStats.networkOutputBytes +
                                   PerfStats::threadStats.networkInputBytes;
                timestamp++;

                RAMCLOUD_LOG(NOTICE, "notFound: %lu, regularPullFound: %lu, priorityPullFound: %lu, priorityHashSize: %lu, %lu: %lf",
                             ramcloud->getNotFound(ramcloud),
                             ramcloud->getRegularPullFound(ramcloud),
                             ramcloud->getPriorityPullFound(ramcloud),
                             ramcloud->getPriorityHashSize(ramcloud),
                             timestamp, static_cast<double > (current - bandwidth) / 1024 / 102);

                bandwidth = current;
                lastTime = stop;
            }
            if (issueMigration && stop > experimentStartTime + oneSecond) {
                issueMigration = false;
                client->startMigration();
            }
            if (stop > experimentStartTime + finishTryTime * hundredMS) {
                finishTryTime++;
                if (client->isFinished()) {
                    RAMCLOUD_LOG(WARNING, "finish");
                    break;
                }
            }
        }
    }

    void
    statistics(std::vector<TimeDist> &result, SampleType type, int tablet = 0);

    WorkloadGenerator(const WorkloadGenerator &) = delete;

    WorkloadGenerator &operator=(const WorkloadGenerator &) = delete;
};

}

#endif //RAMCLOUD_WORKLOADGENERATOR_H
