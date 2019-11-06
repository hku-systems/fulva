/* Copyright (c) 2011-2014 Stanford University
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

#include <iostream>
#include "RawMetrics.h"
#include "ServerList.h"
#include "Cycles.h"
#include "CycleCounter.h"
#include "Common.h"
#include "MasterClient.h"
#include "Context.h"
#include "RamCloud.h"
#include "ShortMacros.h"
#include "WorkloadGenerator.h"
#include "TpcC.h"
#include "MigrationClient.h"
#include "GeminiMigrationManager.h"

using namespace RAMCloud;


Tub<RamCloud> client;

uint64_t controlHubId = 0;
string testControlHub = "testControlHub";

int clientIndex;
int numClients;
uint64_t targetOps;

string tableName;
uint64_t firstKey;
uint64_t lastKey;
uint32_t newOwnerMasterId;

uint32_t objectCount = 0;
uint32_t objectSize = 0;
uint32_t otherObjectCount = 0;

void makeKey(int value, uint32_t length, char *dest)
{
    memset(dest, 'x', length);
    *(reinterpret_cast<int *>(dest)) = value;
}

string status = "status";
string filling = "filling";
string ending = "ending";

void printStatistics(RAMCloud::WorkloadGenerator &workloadGenerator)
{
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> result;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> readSource;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> writeSource;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> readTarget;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> writeTarget;
    workloadGenerator.statistics(result, RAMCloud::WorkloadGenerator::ALL);
    workloadGenerator.statistics(readSource,
                                 RAMCloud::WorkloadGenerator::READ, 1);
    workloadGenerator.statistics(writeSource,
                                 RAMCloud::WorkloadGenerator::WRITE, 1);
    workloadGenerator.statistics(readTarget,
                                 RAMCloud::WorkloadGenerator::READ, 2);
    workloadGenerator.statistics(writeTarget,
                                 RAMCloud::WorkloadGenerator::WRITE, 2);
    RAMCLOUD_LOG(WARNING,
                 "    overall   |    target    |    source    ");
    for (uint64_t i = 0; i < result.size(); i++) {
        RAMCLOUD_LOG(NOTICE,
                     "%lu:%lu, %lu, %lu, %lf | %lu, %lu, %lu, %.2lf | %lu, %lu, %lu, %.2lf | %lu, %lu, %lu, %.2lf | %lu, %lu, %lu, %.2lf",
                     i, result[i].p50, result[i].p999, result[i].avg,
                     static_cast<double>(result[i].bandwidth) / 100.,
                     readSource[i].p50, readSource[i].p999,
                     readSource[i].avg,
                     static_cast<double>(readSource[i].bandwidth) / 100.,
                     writeSource[i].p50, writeSource[i].p999,
                     writeSource[i].avg,
                     static_cast<double>(writeSource[i].bandwidth) / 100.,
                     readTarget[i].p50, readTarget[i].p999,
                     readTarget[i].avg,
                     static_cast<double>(readTarget[i].bandwidth) / 100.,
                     writeTarget[i].p50, writeTarget[i].p999,
                     writeTarget[i].avg,
                     static_cast<double>(writeTarget[i].bandwidth) / 100.
        );
    }
}

class BasicClient : public RAMCloud::WorkloadGenerator::Client {

  PUBLIC:

    struct Migration {
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        ServerId targetServerId;
        bool skipMaster;

        Migration(uint64_t tableId, uint64_t firstKey,
                  uint64_t lastKey, ServerId targetServerId, bool skipMaster)
            : tableId(tableId), firstKey(firstKey), lastKey(lastKey),
              targetServerId(targetServerId), skipMaster(skipMaster)
        {
        }
    };

    BasicClient(RamCloud *ramcloud, int clientIndex, Migration *migration,
                uint16_t keyLength, uint32_t valueLength, uint32_t numObjects)
        : ramcloud(ramcloud), clientIndex(clientIndex), migration(migration),
          controlHubId(), keyLength(keyLength),
          valueLength(valueLength), numObjects(numObjects),
          experimentStartTime(0), migrationId(), migrationStartTime(0),
          migrationFinishTime(0), pressTableId(), generateWorkload(true)
    {
    }

    ~BasicClient()
    {
    }

    void setup(uint32_t objectCount, uint32_t objectSize)
    {
        if (clientIndex == 0) {
            ServerId server1 = ServerId(1u, 0u);
            ServerId server3 = ServerId(3u, 0u);
            pressTableId = client->createTableToServer(tableName.c_str(),
                                                       server1);
            migration->tableId = pressTableId;
            controlHubId = client->createTableToServer(testControlHub.c_str(),
                                                       server3);
            client->testingFill(pressTableId, "", 0, objectCount, objectSize);

            client->splitTablet(tableName.c_str(), lastKey + 1);

            client->write(controlHubId, status.c_str(),
                          static_cast<uint16_t>(status.length()),
                          filling.c_str(),
                          static_cast<uint32_t>(filling.length()));
            RAMCLOUD_LOG(WARNING, "write status to %lu", controlHubId);

        } else {

            while (true) {
                try {
                    controlHubId = client->getTableId(testControlHub.c_str());
                    pressTableId = client->getTableId(tableName.c_str());
                    break;
                } catch (TableDoesntExistException &e) {
                }
            }

            Buffer statusValue;
            bool exists = false;
            while (true) {
                client->read(controlHubId, status.c_str(),
                             static_cast<uint16_t>(status.length()),
                             &statusValue, NULL, NULL, &exists);

                if (exists) {
                    statusValue.size();
                    string currentStatus = string(
                        reinterpret_cast<const char *>(
                            statusValue.getRange(0, statusValue.size())),
                        statusValue.size());

                    RAMCLOUD_LOG(WARNING, "status:%s", currentStatus.c_str());
                    if (currentStatus == filling)
                        break;
                }

                RAMCLOUD_CLOG(WARNING, "wait for filling");
            }

        }
    }

    void read(const char *key, uint64_t keyLen)
    {
        Buffer value;
        bool exists;
        ramcloud->readMigrating(pressTableId, key,
                                static_cast<uint16_t>(keyLen), &value,
                                NULL, NULL, &exists);
    }

    void write(const char *key, uint64_t keyLen, char *value, uint32_t valueLen)
    {
        ramcloud->write(pressTableId, key, static_cast<uint16_t>(keyLen), value,
                        valueLen);
    }

    void startMigration()
    {
        RAMCLOUD_LOG(NOTICE, "Issuing migration request:");
        RAMCLOUD_LOG(NOTICE, "  table (%lu)", migration->tableId);
        RAMCLOUD_LOG(NOTICE, "  first key %lu", migration->firstKey);
        RAMCLOUD_LOG(NOTICE, "  last key  %lx", migration->lastKey);
        RAMCLOUD_LOG(NOTICE, "  recipient master id %u",
                     migration->targetServerId.indexNumber());

        migrationId = ramcloud->backupMigrate(migration->tableId,
                                              migration->firstKey,
                                              migration->lastKey,
                                              migration->targetServerId,
                                              migration->skipMaster);
        migrationStartTime = Cycles::rdtsc();
    }


    bool isFinished()
    {
        if (clientIndex == 0) {
            if (migrationStartTime == 0)
                return false;
            if (migrationFinishTime == 0) {
                if (Cycles::toMicroseconds(
                    Cycles::rdtsc() - migrationStartTime) < 100000)
                    return false;
                bool isFinished = ramcloud->migrationQuery(migrationId);
                if (isFinished) {
                    migrationFinishTime = Cycles::rdtsc();
                }
                return false;
            } else {
                if (Cycles::toSeconds(Cycles::rdtsc() - migrationFinishTime)
                    > 7) {
                    ramcloud->write(controlHubId, status.c_str(),
                                    static_cast<uint16_t>(status.length()),
                                    ending.c_str(),
                                    static_cast<uint32_t>(ending.length()));
                    RAMCLOUD_LOG(NOTICE, "migration finish");
                    return true;
                } else {
                    return false;
                }
            }
        } else {
            Buffer statusValue;
            ramcloud->read(controlHubId, status.c_str(),
                           static_cast<uint16_t>(status.length()),
                           &statusValue);
            string currentStatus = string(
                reinterpret_cast<const char *>(
                    statusValue.getRange(0, statusValue.size())),
                statusValue.size());
            RAMCLOUD_CLOG(WARNING, "finish status:%s", currentStatus.c_str());
            return currentStatus == ending;
        }
    }


    uint64_t migrationDuration()
    {
        return migrationFinishTime - migrationStartTime;
    }

    RamCloud *getRamCloud()
    {
        return ramcloud;
    }

    uint64_t getTableId()
    {
        return pressTableId;
    }

  PRIVATE:
    RamCloud *ramcloud;
    int clientIndex;
    Migration *migration;
    uint64_t controlHubId;
    uint16_t keyLength;
    uint32_t valueLength;
    uint32_t numObjects;
    uint64_t experimentStartTime;
    uint64_t migrationId;
    uint64_t migrationStartTime;
    uint64_t migrationFinishTime;
    uint64_t pressTableId;
    bool generateWorkload;

    DISALLOW_COPY_AND_ASSIGN(BasicClient)

};

void basic()
{
    uint64_t tableId = 0;

    const uint16_t keyLength = 30;
    const uint32_t valueLength = 100;
    BasicClient::Migration migration(tableId, firstKey, lastKey,
                                     ServerId(newOwnerMasterId, 0), false);
    BasicClient basicClient(client.get(), clientIndex, &migration,
                            keyLength, valueLength, objectCount);

    RAMCloud::WorkloadGenerator workloadGenerator(
        "YCSB-B", targetOps, objectCount, objectSize, &basicClient);

    bool issueMigration = false;
    if (clientIndex == 0)
        issueMigration = true;
    workloadGenerator.run(issueMigration);

    std::vector<RAMCloud::WorkloadGenerator::TimeDist> result;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> readResult;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> writeResult;
    workloadGenerator.statistics(result, RAMCloud::WorkloadGenerator::ALL);
    workloadGenerator.statistics(readResult, RAMCloud::WorkloadGenerator::READ);
    workloadGenerator.statistics(writeResult,
                                 RAMCloud::WorkloadGenerator::WRITE);
    RAMCLOUD_LOG(NOTICE,
                 "time: all median, 99th | read median, 99th | write median, 99th");
    for (uint64_t i = 0; i < result.size(); i++) {
        RAMCLOUD_LOG(NOTICE,
                     "%lu:%lu, %lu, %lu, %lf | %lu, %lu, %lu, %lf | %lu, %lu, %lu, %lf",
                     i, result[i].p50, result[i].p999, result[i].avg,
                     static_cast<double>(result[i].bandwidth) / 100.,
                     readResult[i].p50, readResult[i].p999, readResult[i].avg,
                     static_cast<double>(readResult[i].bandwidth) / 100.,
                     writeResult[i].p50, writeResult[i].p999,
                     writeResult[i].avg,
                     static_cast<double>(writeResult[i].bandwidth) / 100.);
    }
}

class RocksteadyClient : public RAMCloud::WorkloadGenerator::Client {
  PUBLIC:

    struct Migration {
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        ServerId sourceServerId;
        ServerId targetServerId;
        bool skipMaster;

        Migration(uint64_t tableId, uint64_t firstKey, uint64_t lastKey,
                  ServerId sourceServerId, ServerId targetServerId,
                  bool skipMaster)
            : tableId(tableId), firstKey(firstKey), lastKey(lastKey),
              sourceServerId(sourceServerId), targetServerId(targetServerId),
              skipMaster(skipMaster)
        {
        }
    };

    RocksteadyClient(RamCloud *ramcloud, int clientIndex, Migration *migration,
                     uint16_t keyLength, uint32_t valueLength,
                     uint32_t numObjects, uint64_t time)
        : ramcloud(ramcloud), clientIndex(clientIndex), migration(migration),
          controlHubId(), keyLength(keyLength), valueLength(valueLength),
          numObjects(numObjects), experimentStartTime(0), migrationId(),
          migrationStartTime(0), migrationFinishTime(0), pressTableId(),
          generateWorkload(true), time(time), rocksteadyMigration()
    {
    }

    uint64_t splitHash()
    {
        return lastKey;
    }

    ~RocksteadyClient()
    {

    }

    void setup(uint32_t objectCount, uint32_t objectSize)
    {
        if (clientIndex == 0) {
            ServerId server1 = ServerId(1u, 0u);
            ServerId server3 = ServerId(3u, 0u);

            uint64_t migrationTableId =
                client->createTableToServer(tableName.c_str(), server1);

            pressTableId = migrationTableId;

            migration->tableId = migrationTableId;
            controlHubId = client->createTableToServer(testControlHub.c_str(),
                                                       server3);
            client->testingFill(migrationTableId, "", 0, objectCount,
                                objectSize);

            client->splitTablet(tableName.c_str(), lastKey + 1);

            client->write(controlHubId, status.c_str(),
                          static_cast<uint16_t>(status.length()),
                          filling.c_str(),
                          static_cast<uint32_t>(filling.length()));
            RAMCLOUD_LOG(WARNING, "write status to %lu", controlHubId);

        } else {

            while (true) {
                try {
                    controlHubId = client->getTableId(testControlHub.c_str());
                    pressTableId = client->getTableId(tableName.c_str());
                    break;
                } catch (TableDoesntExistException &e) {
                }
            }

            Buffer statusValue;
            bool exists = false;
            while (true) {
                client->read(controlHubId, status.c_str(),
                             static_cast<uint16_t>(status.length()),
                             &statusValue, NULL, NULL, &exists);

                if (exists) {
                    statusValue.size();
                    string currentStatus = string(
                        reinterpret_cast<const char *>(
                            statusValue.getRange(0, statusValue.size())),
                        statusValue.size());

                    RAMCLOUD_CLOG(WARNING, "status:%s", currentStatus.c_str());
                    if (currentStatus == filling)
                        break;
                }

                RAMCLOUD_CLOG(WARNING, "wait for filling");
            }

        }
    }

    void read(const char *key, uint64_t keyLen)
    {
        Buffer value;
        bool exists;
        ramcloud->read(pressTableId, key, static_cast<uint16_t>(keyLen), &value,
                       NULL, NULL, &exists);
    }

    void write(const char *key, uint64_t keyLen, char *value, uint32_t valueLen)
    {
        ramcloud->write(pressTableId, key, static_cast<uint16_t>(keyLen), value,
                        valueLen, NULL, NULL, true);
    }

    void startMigration()
    {
        RAMCLOUD_LOG(WARNING, "Issuing migration request:");
        RAMCLOUD_LOG(NOTICE, "  table (%lu)", migration->tableId);
        RAMCLOUD_LOG(NOTICE, "  first key %lu", migration->firstKey);
        RAMCLOUD_LOG(NOTICE, "  last key  %lx", migration->lastKey);
        RAMCLOUD_LOG(NOTICE, "  recipient master id %u",
                     migration->targetServerId.indexNumber());

        rocksteadyMigration.construct(ramcloud, migration->tableId,
                                      migration->firstKey, migration->lastKey,
                                      migration->sourceServerId,
                                      migration->targetServerId);
        rocksteadyMigration->wait();
        rocksteadyMigration.destroy();
        migrationStartTime = Cycles::rdtsc();
    }


    bool isFinished()
    {
        if (clientIndex == 0) {
            if (migrationStartTime == 0 || Cycles::toSeconds(
                Cycles::rdtsc() - migrationStartTime) < time)
                return false;
            else {
                RAMCLOUD_LOG(WARNING, "finish");
                ramcloud->write(controlHubId, status.c_str(),
                                static_cast<uint16_t>(status.length()),
                                ending.c_str(),
                                static_cast<uint32_t>(ending.length()));
                return true;
            }
        } else {
            Buffer statusValue;
            ramcloud->read(controlHubId, status.c_str(),
                           static_cast<uint16_t>(status.length()),
                           &statusValue);
            string currentStatus = string(
                reinterpret_cast<const char *>(
                    statusValue.getRange(0, statusValue.size())),
                statusValue.size());
            RAMCLOUD_CLOG(WARNING, "finish status:%s", currentStatus.c_str());
            return currentStatus == ending;
        }
    }

    uint64_t migrationDuration()
    {
        return migrationFinishTime - migrationStartTime;
    }

    RamCloud *getRamCloud()
    {
        return ramcloud;
    }

    uint64_t getTableId()
    {
        return pressTableId;
    }

  PRIVATE:
    RamCloud *ramcloud;
    int clientIndex;
    Migration *migration;
    uint64_t controlHubId;
    uint16_t keyLength;
    uint32_t valueLength;
    uint32_t numObjects;
    uint64_t experimentStartTime;
    uint64_t migrationId;
    uint64_t migrationStartTime;
    uint64_t migrationFinishTime;
    uint64_t pressTableId;
    bool generateWorkload;
    uint64_t time;
    Tub<RocksteadyMigrateTabletRpc> rocksteadyMigration;

    DISALLOW_COPY_AND_ASSIGN(RocksteadyClient)
};

TPCC::TpccStat
tpcc_oneClient(double runSeconds, TPCC::Driver *driver,
               bool latencyTest = false, bool migrating = false)
{
    TPCC::TpccStat stat;
    uint64_t total = 0;
    double totalLatency = 0;

    uint64_t runCycles = Cycles::fromSeconds(runSeconds);
    uint64_t start = Cycles::rdtsc();

    uint32_t W_ID = clientIndex % (driver->getContext()->numWarehouse) + 1;

    while (true) {
        if (Cycles::rdtsc() - start > runCycles) {
            break;
        }
        bool outcome = false;
        int randNum = rand() % 100;
        if (latencyTest) {
            randNum = 99;
        }
        double latency = 0;
        int txType;
        try {
            if (randNum < 43) {
                txType = 0;
                latency = driver->txPayment(W_ID, &outcome);
            } else if (randNum < 47) {
                txType = 1;
                latency = driver->txOrderStatus(W_ID, &outcome);
            } else if (randNum < 51) {
                txType = 2;
                for (uint32_t D_ID = 1; D_ID <= 10; D_ID++) {
                    latency += driver->txDelivery(W_ID, D_ID, &outcome);
                }
            } else if (randNum < 55) {
                txType = 3;
                latency = driver->txStockLevel(W_ID, 1U /*fixed D_ID*/,
                                               &outcome);
            } else {
                txType = 4;
                latency = driver->txNewOrder(W_ID, &outcome, migrating);
            }
            if (outcome) {
                stat.cumulativeLatency[txType] += latency;
                stat.txPerformedCount[txType]++;
                total++;
                totalLatency += latency;
            } else {
                stat.txAbortCount[txType]++;
            }
        } catch (std::exception e) {
            RAMCLOUD_LOG(NOTICE, "exception thrown TX job, type=%d. %s", txType,
                         e.what());
            throw e;
        }

        if (latencyTest) {
            Cycles::sleep(100);
        }
    }

    double interval = Cycles::toSeconds(Cycles::rdtsc() - start);
    double tput = (double) total / interval;
    double avgLatency = totalLatency / (double) total;

    RAMCLOUD_LOG(NOTICE, "%.2lf, %.2lf, %d", tput, avgLatency,
                 stat.txAbortCount[4]);
    return stat;
}

class TpccClient {

  PRIVATE:

    string
    keyVal(int client, const char *name)
    {
        return format("%d:%s", client, name);
    }

    void
    setSlaveState(const char *state)
    {
        string key = keyVal(clientIndex, "state");
        ramcloud->write(controlTable, key.c_str(),
                        downCast<uint16_t>(key.length()),
                        state);
    }

    char *
    readObject(uint64_t tableId, const void *key, uint16_t keyLength,
               char *value, uint32_t size)
    {
        Buffer buffer;
        ramcloud->read(tableId, key, keyLength, &buffer);
        uint32_t actual = buffer.size();
        if (size <= actual) {
            actual = size - 1;
        }
        buffer.copy(0, size, value);
        value[actual] = 0;
        return value;
    }

    void
    waitForObject(uint64_t tableId, const void *key, uint16_t keyLength,
                  const char *desired, Buffer &value, double timeout = 1.0)
    {
        uint64_t start = Cycles::rdtsc();
        size_t length = desired ? strlen(desired) : -1;
        while (true) {
            try {
                ramcloud->read(tableId, key, keyLength, &value);
                if (desired == NULL) {
                    return;
                }
                const char *actual = value.getStart<char>();
                if ((length == value.size()) &&
                    (memcmp(actual, desired, length) == 0)) {
                    return;
                }
                double elapsed = Cycles::toSeconds(Cycles::rdtsc() - start);
                if (elapsed > timeout) {
                    // Slave is taking too long; time out.
                    throw Exception(HERE, format(
                        "Object <%lu, %.*s> didn't reach desired state '%s' "
                        "(actual: '%.*s')",
                        tableId, keyLength, reinterpret_cast<const char *>(key),
                        desired, downCast<int>(value.size()),
                        actual));
                    exit(1);
                }
            }
            catch (TableDoesntExistException &e) {
            }
            catch (ObjectDoesntExistException &e) {
            }
        }
    }

    void
    waitSlave(int slave, const char *state, double timeout = 3.0)
    {
        Buffer value;
        string key = keyVal(slave, "state");
        waitForObject(controlTable, key.c_str(),
                      downCast<uint16_t>(key.length()),
                      state, value, timeout);
    }

    const char *
    getCommand(char *buffer, uint32_t size, bool remove = true)
    {
        while (true) {
            try {
                string key = keyVal(clientIndex, "command");
                readObject(controlTable, key.c_str(),
                           downCast<uint16_t>(key.length()), buffer, size);
                if (strcmp(buffer, "idle") != 0) {
                    if (remove) {
                        // Delete the command value so we don't process the same
                        // command twice.
                        ramcloud->remove(controlTable, key.c_str(),
                                         downCast<uint16_t>(key.length()));
                    }
                    return buffer;
                }
            }
            catch (TableDoesntExistException &e) {
            }
            catch (ObjectDoesntExistException &e) {
            }
            Cycles::sleep(10000);
        }
    }

    void
    sendCommand(const char *command, const char *state, int firstSlave,
                int numSlaves = 1)
    {
        if (command != NULL) {
            for (int i = 0; i < numSlaves; i++) {
                string key = keyVal(firstSlave + i, "command");
                ramcloud->write(controlTable, key.c_str(),
                                downCast<uint16_t>(key.length()), command);
            }
        }
        if (state != NULL) {
            for (int i = 0; i < numSlaves; i++) {
                waitSlave(firstSlave + i, state);
            }
        }
    }

    void setup()
    {

    }

  PUBLIC:

    struct Migration {
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        ServerId targetServerId;
        bool skipMaster;

        Migration(uint64_t tableId, uint64_t firstKey,
                  uint64_t lastKey, ServerId targetServerId, bool skipMaster)
            : tableId(tableId), firstKey(firstKey), lastKey(lastKey),
              targetServerId(targetServerId), skipMaster(skipMaster)
        {
        }
    };

    TpccClient(RamCloud *ramcloud, uint64_t controlTable, uint64_t time)
        : ramcloud(ramcloud), controlTable(controlTable), time(time),
          geminiMigration(), ramcloudMigration()
    {

    };

    void run()
    {
        ServerId server1 = ServerId(1u, 0u);
        ServerId server2 = ServerId(2u, 0u);
        ServerId server3 = ServerId(3u, 0u);
        TPCC::Driver driver(client.get());
        if (clientIndex == 0) {
            driver.initTables(server1, server3,
                              firstKey, lastKey);
            sendCommand("init", "initializing", 1, numClients - 1);
        } else {
            char command[20];
            while (true) {
                getCommand(command, sizeof(command), false);
                if (strcmp(command, "init") == 0) {
                    setSlaveState("initializing");
                    break;
                }
            }

        }
        for (uint32_t i = static_cast<uint32_t>(clientIndex + 1);
             i <= driver.getContext()->numWarehouse; i += numClients) {
            driver.initBenchmark(i);
        }

        if (clientIndex == 0) {
            for (int slave = 1; slave < numClients; ++slave) {
                waitSlave(slave, "ready", 60.0);
            }
            sendCommand("run", "running", 1, numClients - 1);
            for (int k = 0; k < 50; k++) {
                tpcc_oneClient(0.1, &driver, true);
            }

            RAMCLOUD_LOG(WARNING, "Issuing migration request:");
            RAMCLOUD_LOG(NOTICE, "  table (%lu)", driver.getTableId(1));
            RAMCLOUD_LOG(NOTICE, "  first key %lu", firstKey);
            RAMCLOUD_LOG(NOTICE, "  last key  %lx", lastKey);
            RAMCLOUD_LOG(NOTICE, "  recipient master id %u",
                         server2.indexNumber());

            geminiMigration.construct(ramcloud, driver.getTableId(1),
                                      firstKey, lastKey,
                                      server1,
                                      server2);
            geminiMigration->wait();
            geminiMigration.destroy();

//            ramcloudMigration.construct(ramcloud, driver.getTableId(1),
//                                    firstKey, lastKey,
//                                    server2);
//            ramcloudMigration->wait();
//            ramcloudMigration.destroy();
            uint64_t experimentEndTime =
                Cycles::rdtsc() + time * Cycles::fromSeconds(1);

            while (true) {
                tpcc_oneClient(0.1, &driver, true, true);
                if (Cycles::rdtsc() > experimentEndTime)
                    break;
            }

            for (int k = 0; k < 50; k++) {
                tpcc_oneClient(0.1, &driver, true);
            }

            try {
                sendCommand("done", "done", 1, numClients - 1);
            } catch (Exception &e) {
                RAMCLOUD_LOG(ERROR, "%s", e.what());
            }
        } else {
            bool running = false;
            setSlaveState("ready");
            char command[20];
            while (true) {
                getCommand(command, sizeof(command), false);
                if (strcmp(command, "run") == 0) {
                    if (!running) {
                        setSlaveState("running");
                        running = true;
                        RAMCLOUD_LOG(NOTICE,
                                     "Starting TPC-C benchmark");
                    }

                    tpcc_oneClient(0.5, &driver, true);

                } else if (strcmp(command, "done") == 0) {
                    //Save result to data table.
                    setSlaveState("done");
                    RAMCLOUD_LOG(NOTICE, "Ending TPC-C benchmark");
                    return;
                } else if (strcmp(command, "init") == 0) {
                    continue;
                } else {
                    RAMCLOUD_LOG(ERROR, "unknown command %s", command);
                    return;
                }
            }
        }
    };

    DISALLOW_COPY_AND_ASSIGN(TpccClient)

  PRIVATE:
    RamCloud *ramcloud;
    uint64_t controlTable;
    uint64_t time;
    Tub<GeminiMigrateTabletRpc> geminiMigration;
    Tub<MigrateTabletRpc> ramcloudMigration;
};

class RamcloudClient : public RAMCloud::WorkloadGenerator::Client {
  PUBLIC:

    struct Migration {
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        ServerId sourceServerId;
        ServerId targetServerId;
        bool skipMaster;

        Migration(uint64_t tableId, uint64_t firstKey, uint64_t lastKey,
                  ServerId sourceServerId, ServerId targetServerId,
                  bool skipMaster)
            : tableId(tableId), firstKey(firstKey), lastKey(lastKey),
              sourceServerId(sourceServerId), targetServerId(targetServerId),
              skipMaster(skipMaster)
        {
        }
    };

    RamcloudClient(RamCloud *ramcloud, int clientIndex, Migration *migration,
                   uint16_t keyLength, uint32_t valueLength,
                   uint32_t numObjects, uint64_t time)
        : ramcloud(ramcloud), clientIndex(clientIndex), migration(migration),
          controlHubId(), keyLength(keyLength), valueLength(valueLength),
          numObjects(numObjects), experimentStartTime(0), migrationId(),
          migrationStartTime(0), migrationFinishTime(0), pressTableId(),
          generateWorkload(true), time(time), ramcloudMigration()
    {
    }

    ~RamcloudClient()
    {

    }

    void setup(uint32_t objectCount, uint32_t objectSize)
    {
        if (clientIndex == 0) {
            ServerId server1 = ServerId(1u, 0u);
            ServerId server3 = ServerId(3u, 0u);
            pressTableId = client->createTableToServer(tableName.c_str(),
                                                       server1);
            migration->tableId = pressTableId;
            controlHubId = client->createTableToServer(testControlHub.c_str(),
                                                       server3);
            client->testingFill(pressTableId, "", 0, objectCount, objectSize);

            client->splitTablet(tableName.c_str(), lastKey + 1);

            client->write(controlHubId, status.c_str(),
                          static_cast<uint16_t>(status.length()),
                          filling.c_str(),
                          static_cast<uint32_t>(filling.length()));
            RAMCLOUD_LOG(WARNING, "write status to %lu", controlHubId);

        } else {

            while (true) {
                try {
                    controlHubId = client->getTableId(testControlHub.c_str());
                    pressTableId = client->getTableId(tableName.c_str());
                    break;
                } catch (TableDoesntExistException &e) {
                }
            }

            Buffer statusValue;
            bool exists = false;
            while (true) {
                client->read(controlHubId, status.c_str(),
                             static_cast<uint16_t>(status.length()),
                             &statusValue, NULL, NULL, &exists);

                if (exists) {
                    statusValue.size();
                    string currentStatus = string(
                        reinterpret_cast<const char *>(
                            statusValue.getRange(0, statusValue.size())),
                        statusValue.size());

                    RAMCLOUD_CLOG(WARNING, "status:%s", currentStatus.c_str());
                    if (currentStatus == filling)
                        break;
                }

                RAMCLOUD_CLOG(WARNING, "wait for filling");
            }

        }
    }

    void read(const char *key, uint64_t keyLen)
    {
        Buffer value;
        bool exists;
        ramcloud->read(pressTableId, key, static_cast<uint16_t>(keyLen), &value,
                       NULL, NULL, &exists);
    }

    void write(const char *key, uint64_t keyLen, char *value, uint32_t valueLen)
    {
        ramcloud->write(pressTableId, key, static_cast<uint16_t>(keyLen), value,
                        valueLen, NULL, NULL, true);
    }

    void startMigration()
    {
        RAMCLOUD_LOG(WARNING, "Issuing migration request:");
        RAMCLOUD_LOG(NOTICE, "  table (%lu)", migration->tableId);
        RAMCLOUD_LOG(NOTICE, "  first key %lu", migration->firstKey);
        RAMCLOUD_LOG(NOTICE, "  last key  %lx", migration->lastKey);
        RAMCLOUD_LOG(NOTICE, "  recipient master id %u",
                     migration->targetServerId.indexNumber());

        ramcloudMigration.construct(ramcloud, migration->tableId,
                                    migration->firstKey, migration->lastKey,
                                    migration->targetServerId);
        migrationStartTime = Cycles::rdtsc();
    }


    bool isFinished()
    {
        if (clientIndex == 0) {
            if (migrationStartTime == 0 || Cycles::toSeconds(
                Cycles::rdtsc() - migrationStartTime) < time)
                return false;
            else {
                RAMCLOUD_LOG(WARNING, "finish");
                ramcloud->write(controlHubId, status.c_str(),
                                static_cast<uint16_t>(status.length()),
                                ending.c_str(),
                                static_cast<uint32_t>(ending.length()));
                return true;
            }
        } else {
            Buffer statusValue;
            ramcloud->read(controlHubId, status.c_str(),
                           static_cast<uint16_t>(status.length()),
                           &statusValue);
            string currentStatus = string(
                reinterpret_cast<const char *>(
                    statusValue.getRange(0, statusValue.size())),
                statusValue.size());
            RAMCLOUD_CLOG(WARNING, "finish status:%s", currentStatus.c_str());
            return currentStatus == ending;
        }
    }

    uint64_t migrationDuration()
    {
        return migrationFinishTime - migrationStartTime;
    }

    RamCloud *getRamCloud()
    {
        return ramcloud;
    }

    uint64_t getTableId()
    {
        return pressTableId;
    }

  PRIVATE:
    RamCloud *ramcloud;
    int clientIndex;
    Migration *migration;
    uint64_t controlHubId;
    uint16_t keyLength;
    uint32_t valueLength;
    uint32_t numObjects;
    uint64_t experimentStartTime;
    uint64_t migrationId;
    uint64_t migrationStartTime;
    uint64_t migrationFinishTime;
    uint64_t pressTableId;
    bool generateWorkload;
    uint64_t time;
    Tub<MigrateTabletRpc> ramcloudMigration;

    DISALLOW_COPY_AND_ASSIGN(RamcloudClient)
};

void basic_tpcc()
{
    uint64_t controlTable;
    ServerId server4 = ServerId(4u, 0u);
    controlTable = client->createTableToServer(testControlHub.c_str(),
                                               server4);
    TpccClient tpccClient(client.get(), controlTable, 6);
    tpccClient.run();
}

void rocksteadyBasic()
{
    uint64_t tableId = 0;

    const uint16_t keyLength = 30;
    const uint32_t valueLength = 100;
    RocksteadyClient::Migration
        migration(tableId, firstKey, lastKey, ServerId(1, 0),
                  ServerId(newOwnerMasterId, 0), false);
    RocksteadyClient basicClient(client.get(), clientIndex, &migration,
                                 keyLength, valueLength, objectCount, 7);
    RAMCloud::WorkloadGenerator workloadGenerator(
        "YCSB-A", targetOps, objectCount, objectSize, &basicClient);

    bool issueMigration = false;
    if (clientIndex == 0)
        issueMigration = true;
    workloadGenerator.asyncRun<ReadRpc>(issueMigration);

    std::vector<RAMCloud::WorkloadGenerator::TimeDist> result;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> readResult;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> writeResult;
    workloadGenerator.statistics(result, RAMCloud::WorkloadGenerator::ALL);
    workloadGenerator.statistics(readResult,
                                 RAMCloud::WorkloadGenerator::ALL, 1);
    workloadGenerator.statistics(writeResult,
                                 RAMCloud::WorkloadGenerator::ALL, 2);
    RAMCLOUD_LOG(WARNING,
                 "    overall   |    target    |    source    ");
    for (uint64_t i = 0; i < result.size(); i++) {
        RAMCLOUD_LOG(NOTICE,
                     "%lu:%lu, %lu, %lu, %lf | %lu, %lu, %lu, %lf | %lu, %lu, %lu, %lf",
                     i, result[i].p50, result[i].p999, result[i].avg,
                     static_cast<double>(result[i].bandwidth) / 100.,
                     readResult[i].p50, readResult[i].p999,
                     readResult[i].avg,
                     static_cast<double>(readResult[i].bandwidth) / 100.,
                     writeResult[i].p50, writeResult[i].p999,
                     writeResult[i].avg,
                     static_cast<double>(writeResult[i].bandwidth) / 100.);
    }
}

void ramcloudBasic()
{
    uint64_t tableId = 0;

    const uint16_t keyLength = 30;
    const uint32_t valueLength = 100;
    RamcloudClient::Migration
        migration(tableId, firstKey, lastKey, ServerId(1, 0),
                  ServerId(newOwnerMasterId, 0), false);
    RamcloudClient basicClient(client.get(), clientIndex, &migration,
                               keyLength, valueLength, objectCount, 25);
    RAMCloud::WorkloadGenerator workloadGenerator(
        "YCSB-A", targetOps, objectCount, objectSize, &basicClient);

    bool issueMigration = false;
    if (clientIndex == 0)
        issueMigration = true;
    workloadGenerator.asyncRun<ReadRpc>(issueMigration);

    std::vector<RAMCloud::WorkloadGenerator::TimeDist> result;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> readResult;
    std::vector<RAMCloud::WorkloadGenerator::TimeDist> writeResult;
    workloadGenerator.statistics(result, RAMCloud::WorkloadGenerator::ALL);
    workloadGenerator.statistics(readResult,
                                 RAMCloud::WorkloadGenerator::READ);
    workloadGenerator.statistics(writeResult,
                                 RAMCloud::WorkloadGenerator::WRITE);
    RAMCLOUD_LOG(WARNING,
                 "time: all median, 99th | read median, 99th | write median, 99th");
    for (uint64_t i = 0; i < result.size(); i++) {
        RAMCLOUD_LOG(NOTICE,
                     "%lu:%lu, %lu, %lu, %lf | %lu, %lu, %lu, %lf | %lu, %lu, %lu, %lf",
                     i, result[i].p50, result[i].p999, result[i].avg,
                     static_cast<double>(result[i].bandwidth) / 100.,
                     readResult[i].p50, readResult[i].p999,
                     readResult[i].avg,
                     static_cast<double>(readResult[i].bandwidth) / 100.,
                     writeResult[i].p50, writeResult[i].p999,
                     writeResult[i].avg,
                     static_cast<double>(writeResult[i].bandwidth) / 100.);
    }
}

struct BenchmarkInfo {
    const char *name;

    void (*func)();
};

BenchmarkInfo tests[] = {
    {"basic", basic}
};


class GeminiClient : public RAMCloud::WorkloadGenerator::Client {
  PUBLIC:

    struct Migration {
        uint64_t tableId;
        uint64_t firstKey;
        uint64_t lastKey;
        ServerId sourceServerId;
        ServerId targetServerId;
        bool skipMaster;

        Migration(uint64_t tableId, uint64_t firstKey, uint64_t lastKey,
                  ServerId sourceServerId, ServerId targetServerId,
                  bool skipMaster)
            : tableId(tableId), firstKey(firstKey), lastKey(lastKey),
              sourceServerId(sourceServerId), targetServerId(targetServerId),
              skipMaster(skipMaster)
        {
        }
    };

    GeminiClient(RamCloud *ramcloud, int clientIndex, Migration *migration,
                 uint16_t keyLength, uint32_t valueLength,
                 uint32_t numObjects, uint64_t time)
        : ramcloud(ramcloud), clientIndex(clientIndex), migration(migration),
          controlHubId(), keyLength(keyLength), valueLength(valueLength),
          numObjects(numObjects), experimentStartTime(0), migrationId(),
          migrationStartTime(0), migrationFinishTime(0), pressTableId(),
          generateWorkload(true), time(time), geminiMigration()
    {
    }

    uint64_t splitHash()
    {
        return lastKey;
    }

    ~GeminiClient()
    {

    }

    void setup(uint32_t objectCount, uint32_t objectSize)
    {
        if (clientIndex == 0) {
            ServerId server1 = ServerId(1u, 0u);
            ServerId server3 = ServerId(3u, 0u);

            uint64_t migrationTableId =
                client->createTableToServer(tableName.c_str(), server1);

            pressTableId = migrationTableId;

            migration->tableId = migrationTableId;
            controlHubId = client->createTableToServer(testControlHub.c_str(),
                                                       server3);
            client->testingFill(migrationTableId, "", 0, objectCount,
                                objectSize);

            client->splitTablet(tableName.c_str(), lastKey + 1);

            client->write(controlHubId, status.c_str(),
                          static_cast<uint16_t>(status.length()),
                          filling.c_str(),
                          static_cast<uint32_t>(filling.length()));
            RAMCLOUD_LOG(WARNING, "write status to %lu", controlHubId);

        } else {

            while (true) {
                try {
                    controlHubId = client->getTableId(testControlHub.c_str());
                    pressTableId = client->getTableId(tableName.c_str());
                    break;
                } catch (TableDoesntExistException &e) {
                }
            }

            Buffer statusValue;
            bool exists = false;
            while (true) {
                client->read(controlHubId, status.c_str(),
                             static_cast<uint16_t>(status.length()),
                             &statusValue, NULL, NULL, &exists);

                if (exists) {
                    statusValue.size();
                    string currentStatus = string(
                        reinterpret_cast<const char *>(
                            statusValue.getRange(0, statusValue.size())),
                        statusValue.size());

                    RAMCLOUD_CLOG(WARNING, "status:%s", currentStatus.c_str());
                    if (currentStatus == filling)
                        break;
                }

                RAMCLOUD_CLOG(WARNING, "wait for filling");
            }

        }
    }

    void read(const char *key, uint64_t keyLen)
    {
        Buffer value;
        bool exists;
        ramcloud->read(pressTableId, key, static_cast<uint16_t>(keyLen), &value,
                       NULL, NULL, &exists);
    }

    void write(const char *key, uint64_t keyLen, char *value, uint32_t valueLen)
    {
        ramcloud->write(pressTableId, key, static_cast<uint16_t>(keyLen), value,
                        valueLen, NULL, NULL, true);
    }

    void startMigration()
    {
        RAMCLOUD_LOG(WARNING, "Issuing migration request:");
        RAMCLOUD_LOG(NOTICE, "  table (%lu)", migration->tableId);
        RAMCLOUD_LOG(NOTICE, "  first key %lu", migration->firstKey);
        RAMCLOUD_LOG(NOTICE, "  last key  %lx", migration->lastKey);
        RAMCLOUD_LOG(NOTICE, "  recipient master id %u",
                     migration->targetServerId.indexNumber());

        geminiMigration.construct(ramcloud, migration->tableId,
                                  migration->firstKey, migration->lastKey,
                                  migration->sourceServerId,
                                  migration->targetServerId);
        geminiMigration->wait();
        geminiMigration.destroy();
        migrationStartTime = Cycles::rdtsc();
    }

    bool isFinished()
    {
        if (clientIndex == 0) {
            if (migrationStartTime == 0 || Cycles::toSeconds(
                Cycles::rdtsc() - migrationStartTime) < time)
                return false;
            else {
                RAMCLOUD_LOG(WARNING, "finish");
                ramcloud->write(controlHubId, status.c_str(),
                                static_cast<uint16_t>(status.length()),
                                ending.c_str(),
                                static_cast<uint32_t>(ending.length()));
                return true;
            }
        } else {
            Buffer statusValue;
            ramcloud->read(controlHubId, status.c_str(),
                           static_cast<uint16_t>(status.length()),
                           &statusValue);
            string currentStatus = string(
                reinterpret_cast<const char *>(
                    statusValue.getRange(0, statusValue.size())),
                statusValue.size());
            RAMCLOUD_CLOG(WARNING, "finish status:%s", currentStatus.c_str());
            return currentStatus == ending;
        }
    }

    uint64_t migrationDuration()
    {
        return migrationFinishTime - migrationStartTime;
    }

    RamCloud *getRamCloud()
    {
        return ramcloud;
    }

    uint64_t getTableId()
    {
        return pressTableId;
    }

  PRIVATE:
    RamCloud *ramcloud;
    int clientIndex;
    Migration *migration;
    uint64_t controlHubId;
    uint16_t keyLength;
    uint32_t valueLength;
    uint32_t numObjects;
    uint64_t experimentStartTime;
    uint64_t migrationId;
    uint64_t migrationStartTime;
    uint64_t migrationFinishTime;
    uint64_t pressTableId;
    bool generateWorkload;
    uint64_t time;
    Tub<GeminiMigrateTabletRpc> geminiMigration;

    DISALLOW_COPY_AND_ASSIGN(GeminiClient)
};

void geminiBasic()
{
    uint64_t tableId = 0;

    const uint16_t keyLength = 30;
    const uint32_t valueLength = 100;
    GeminiClient::Migration
        migration(tableId, firstKey, lastKey, ServerId(1, 0),
                  ServerId(newOwnerMasterId, 0), false);
    GeminiClient basicClient(client.get(), clientIndex, &migration,
                             keyLength, valueLength, objectCount, 20);
    RAMCloud::WorkloadGenerator workloadGenerator(
        "YCSB-B", targetOps, objectCount, objectSize, &basicClient);

    bool issueMigration = false;
    if (clientIndex == 0)
        issueMigration = true;

#ifdef SPLIT_COPY
    workloadGenerator.asyncRun<MigrationReadTask<ReadRpc, MigrationReadRpc>>(
        issueMigration);
#else
    workloadGenerator.asyncRun<ReadRpc>(issueMigration);
#endif

    printStatistics(workloadGenerator);
}

int
main(int argc, char *argv[])
try
{

    OptionsDescription migrateOptions("Migrate");
    migrateOptions.add_options()
        ("clientIndex",
         ProgramOptions::value<int>(&clientIndex)->
             default_value(0),
         "Index of this client (first client is 0)")
        ("numClients",
         ProgramOptions::value<int>(&numClients)->
             default_value(1),
         "Total number of clients running")
        ("table,t",
         ProgramOptions::value<string>(&tableName)->
             default_value("benchmark"),
         "name of the table to migrate.")
        ("firstKey,f",
         ProgramOptions::value<uint64_t>(&firstKey)->
             default_value(0),
         "First key of the tablet range to migrate")
        ("lastKey,z",
         ProgramOptions::value<uint64_t>(&lastKey)->
             default_value((~0ul) / 100 * 99),
         "Last key of the tablet range to migrate")
        ("recipient,r",
         ProgramOptions::value<uint32_t>(&newOwnerMasterId)->
             default_value(2),
         "ServerId of the master to migrate to")
        ("objectCount",
         ProgramOptions::value<uint32_t>(&objectCount)->
             default_value(30000000),
         "Number of objects to pre-populate in the table to be migrated")
        ("objectSize",
         ProgramOptions::value<uint32_t>(&objectSize)->
             default_value(100),
         "Size of objects to pre-populate in tables")
        ("otherObjectCount",
         ProgramOptions::value<uint32_t>(&otherObjectCount)->
             default_value(0),
         "Number of objects to pre-populate in the table to be "
         "NOT TO BE migrated");

    OptionParser optionParser(migrateOptions, argc, argv);
    if (tableName == "") {
        RAMCLOUD_DIE("error: please specify the table name");
        exit(1);
    }
    if (newOwnerMasterId == 0) {
        RAMCLOUD_DIE("error: please specify the recipient's ServerId");
        exit(1);
    }

    targetOps = 200000;
    string coordinatorLocator = optionParser.options.getCoordinatorLocator();
    RAMCLOUD_LOG(NOTICE, "client: Connecting to coordinator %s",
                 coordinatorLocator.c_str());
    client.construct(&optionParser.options);

    Context *context = client->clientContext;
    ProtoBuf::ServerList protoServerList;
    CoordinatorClient::getServerList(context, &protoServerList);

    ServerList serverList(context);
    serverList.applyServerList(protoServerList);

//    basic();
//    rocksteadyBasic();
//    ramcloudBasic();
//    basic_tpcc();
    geminiBasic();

    return 0;
} catch (ClientException &e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception &e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
