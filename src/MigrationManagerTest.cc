#include "TestUtil.h"
#include "MockCluster.h"
#include "RamCloud.h"

namespace RAMCloud {
struct MigrationManagerTest : public ::testing::Test {
    TestLog::Enable logEnabler;
    Context context;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    MigrationManager *migrationManager;

    Server *master1Server;
    Server *master2Server;
    Server *targetServer;
    MasterService *master1Service;
    MasterService *master2Service;
    MasterService *targetService;
    uint64_t tableId;

    std::mutex mutex;

    typedef std::unique_lock<std::mutex> Lock;

    MigrationManagerTest(uint32_t segmentSize = 256 * 1024)
        : logEnabler(), context(), cluster(&context), ramcloud(),
          migrationManager(), master1Server(), master2Server(), targetServer(),
          master1Service(), master2Service(), targetService(), tableId(),
          mutex()
    {
        Logger::get().setLogLevels(RAMCloud::WARNING);

        migrationManager = &cluster.coordinator->migrationManager;

        ServerConfig backupConfig(ServerConfig::forTesting());
        backupConfig.localLocator = "mock:host=backup1";
        backupConfig.services = {WireFormat::BACKUP_SERVICE,
                                 WireFormat::ADMIN_SERVICE};
        backupConfig.segmentSize = segmentSize;
        backupConfig.backup.numSegmentFrames = 30;

        Server *server = cluster.addServer(backupConfig);
        server->backup->testingSkipCallerIdCheck = true;

        ServerConfig masterConfig(ServerConfig::forTesting());

        masterConfig.segmentSize = segmentSize;
        masterConfig.maxObjectDataSize = segmentSize / 4;
        masterConfig.services = {WireFormat::MASTER_SERVICE,
                                 WireFormat::ADMIN_SERVICE};
        masterConfig.master.logBytes = segmentSize * 30;
        masterConfig.master.numReplicas = 0;

        masterConfig.localLocator = "mock:host=master1";
        master1Server = cluster.addServer(masterConfig);
        master1Service = master1Server->master.get();
        master1Service->objectManager.log.sync();

        masterConfig.localLocator = "mock:host=master2";
        master2Server = cluster.addServer(masterConfig);
        master2Service = master2Server->master.get();
        master2Service->objectManager.log.sync();

        masterConfig.localLocator = "mock:host=target";
        targetServer = cluster.addServer(masterConfig);
        targetService = targetServer->master.get();
        targetService->objectManager.log.sync();
        targetService->migrationTargetManager.disableMigrationRecover = true;

        ramcloud.construct(&context, "mock:host=coordinator");

        tableId = ramcloud->createTableToServer(
            "test", master1Service->serverId);

        ramcloud->splitTablet("test", (~0lu) / 2);
    }

    void writeValues(uint64_t tableId, int requestNum, int iteration)
    {
        for (int i = 0; i < requestNum; i++) {
            string keyStr = format("%.8d", i);
            string valueStr = format("%.8d-%.8d", i, iteration);
            ramcloud->write(tableId, keyStr.c_str(), 8, valueStr.c_str(), 17);
        }
    }

    void validateValues(uint64_t tableId, int requestNum, int iteration)
    {
        for (int i = 0; i < requestNum; i++) {
            string keyStr = format("%.8d", i);
            Buffer value;
            ramcloud->readMigrating(tableId, keyStr.c_str(), 8, &value);
            string valueStr = string(reinterpret_cast<const char *>(
                                         value.getRange(0, value.size())),
                                     value.size());
            string expectedStr = format("%.8d-%.8d", i, iteration);
            EXPECT_EQ(expectedStr, valueStr);
        }
    }

    DISALLOW_COPY_AND_ASSIGN(MigrationManagerTest);
};

TEST_F(MigrationManagerTest, migratingReadAndWrite)
{
    int requestNum = 100, iteration = 0;
    writeValues(tableId, requestNum, iteration);
    for (int i = 0; i < 2; i++) {
        validateValues(tableId, requestNum, iteration);
        iteration += 1;
        writeValues(tableId, requestNum, iteration);
    }

    uint64_t migrationId = ramcloud->backupMigrate(
        tableId, (~0lu) / 2, ~0lu, targetServer->serverId);

    MigrationTargetManager::Migration *migrationTarget = NULL;
    while (migrationTarget == NULL) {
//        RAMCLOUD_LOG(WARNING, "validate :%d", iteration);
        validateValues(tableId, requestNum, iteration);
        iteration += 1;
//        RAMCLOUD_LOG(WARNING, "write :%d", iteration);
        writeValues(tableId, requestNum, iteration);

        migrationTarget =
            targetService->migrationTargetManager.getMigration(migrationId);
    }

}

TEST_F(MigrationManagerTest, startMigration)
{

}

}
