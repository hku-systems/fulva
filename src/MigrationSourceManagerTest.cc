
#include "TestUtil.h"
#include "MasterService.h"
#include "MockCluster.h"
#include "RamCloud.h"
#include "MasterClient.h"
#include "Transaction.h"
#include "ClientTransactionTask.h"

namespace RAMCloud {

class TabletHub {
  public:
    vector<TabletWithLocator> tablets;

    TabletHub() : tablets()
    {

    }

    void init()
    {
        tablets.clear();
        Tablet rawEntry1({2, 0, uint64_t(~0ul), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry1, "mock:host=master1");
        Tablet rawEntry2({3, 0, uint64_t(~0ul), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry2, "mock:host=master2");
        Tablet rawEntry3({4, 0, uint64_t(~0ul), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry3, "mock:host=target");
    }

    void allInMaster1()
    {
        init();
        Tablet rawEntry1({1, 0, uint64_t(~0ul), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry1, "mock:host=master1");
    }

    void master1And2()
    {
        init();
        Tablet rawEntry1({1, 0, uint64_t(~0ul / 2), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry1, "mock:host=master1");
        Tablet rawEntry2({1, uint64_t(~0ul / 2 + 1), uint64_t(~0), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry2, "mock:host=master2");
    }

    void targetAndMaster2()
    {
        init();
        Tablet rawEntry1({1, 0, uint64_t(~0ul / 2), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry1, "mock:host=target");
        Tablet rawEntry2({1, uint64_t(~0ul / 2 + 1), uint64_t(~0ul), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry2, "mock:host=master2");
    }

    void targetAndMaster1()
    {
        init();
        Tablet rawEntry1({1, 0, uint64_t(~0ul / 2), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry1, "mock:host=target");
        Tablet rawEntry2({1, uint64_t(~0ul / 2 + 1), uint64_t(~0ul), ServerId(),
                          Tablet::NORMAL, LogPosition()});
        tablets.emplace_back(rawEntry2, "mock:host=master1");
    }


};

class MigrationRefresher : public ObjectFinder::TableConfigFetcher {
  public:
    MigrationRefresher(TabletHub *tabletHub) : tabletHub(tabletHub)
    {}

    bool tryGetTableConfig(
        uint64_t tableId,
        std::map<TabletKey, TabletWithLocator> *tableMap,
        std::multimap<std::pair<uint64_t, uint8_t>,
            IndexletWithLocator> *tableIndexMap)
    {
        tableMap->clear();

        for (auto entry: tabletHub->tablets) {
            TabletKey key{entry.tablet.tableId, entry.tablet.startKeyHash};
            tableMap->insert(std::make_pair(key, entry));
        }
        return true;
    }

    // After this many refreshes we stop including table 99 in the
    // map; used to detect that misdirected requests are rejected by
    // the target server.
    TabletHub *tabletHub;
  private:
    DISALLOW_COPY_AND_ASSIGN(MigrationRefresher);
};


class MigrationSourceManagerTest : public ::testing::Test {
  public:
    TestLog::Enable logEnabler;
    Context context;
    ServerList serverList;
    MockCluster cluster;
    Tub<RamCloud> ramcloud;
    ServerConfig backup1Config;
    ServerId backup1Id;

    ServerConfig masterConfig;
    Server *master1Server;
    Server *master2Server;
    Server *targetServer;
    MasterService *master1Service;
    MasterService *master2Service;
    MasterService *targetService;

    mutable std::mutex mutex;
    typedef std::unique_lock<std::mutex> Lock;
    TabletHub tabletHub;
    MigrationRefresher *clientRefresher;

    explicit MigrationSourceManagerTest(uint32_t segmentSize = 256 * 1024)
        : logEnabler(), context(), serverList(&context), cluster(&context),
          ramcloud(), backup1Config(ServerConfig::forTesting()), backup1Id(),
          masterConfig(ServerConfig::forTesting()), master1Server(),
          master2Server(), targetServer(), master1Service(), master2Service(),
          targetService(), mutex(), tabletHub(), clientRefresher()
    {
        Logger::get().setLogLevels(RAMCloud::SILENT_LOG_LEVEL);
        backup1Config.localLocator = "mock:host=backup1";
        backup1Config.services = {WireFormat::BACKUP_SERVICE,
                                  WireFormat::ADMIN_SERVICE};
        backup1Config.segmentSize = segmentSize;
        backup1Config.backup.numSegmentFrames = 30;
        Server *server = cluster.addServer(backup1Config);
        server->backup->testingSkipCallerIdCheck = true;
        backup1Id = server->serverId;

        masterConfig = ServerConfig::forTesting();
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
        master1Service->tabletManager.addTablet(2, 0, ~0UL,
                                                TabletManager::NORMAL);

        masterConfig.localLocator = "mock:host=master2";
        master2Server = cluster.addServer(masterConfig);
        master2Service = master2Server->master.get();
        master2Service->objectManager.log.sync();
        master2Service->tabletManager.addTablet(3, 0, ~0UL,
                                                TabletManager::NORMAL);

        masterConfig.localLocator = "mock:host=target";
        targetServer = cluster.addServer(masterConfig);
        targetService = targetServer->master.get();
        targetService->objectManager.log.sync();
        targetService->migrationTargetManager.disableMigrationRecover = true;
        targetService->tabletManager.addTablet(4, 0, ~0UL,
                                               TabletManager::NORMAL);

        ramcloud.construct(&context, "mock:host=coordinator");
        clientRefresher = new MigrationRefresher(&tabletHub);
        context.objectFinder->tableConfigFetcher.reset(clientRefresher);
        master1Server->context->objectFinder->tableConfigFetcher.reset(
            new MigrationRefresher(&tabletHub));
        master2Server->context->objectFinder->tableConfigFetcher.reset(
            new MigrationRefresher(&tabletHub));
        targetServer->context->objectFinder->tableConfigFetcher.reset(
            new MigrationRefresher(&tabletHub));
    }

    DISALLOW_COPY_AND_ASSIGN(MigrationSourceManagerTest);

    bool
    isObjectLocked(MasterService *service, Key &key)
    {
        service->objectManager.objectMap.prefetchBucket(key.getHash());
        ObjectManager::HashTableBucketLock lock(service->objectManager,
                                                key);

        TabletManager::Tablet tablet;
        if (!service->objectManager.tabletManager->getTablet(key, &tablet))
            throw UnknownTabletException(HERE);

        return service->objectManager.lockTable.isLockAcquired(key);
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
};

TEST_F(MigrationSourceManagerTest, increment_basic)
{
    Buffer buffer;
    uint64_t version = 0;
    int64_t oldInt64 = 1;
    int64_t newInt64;
    double oldDouble = 1.0;
    double newDouble;

    tabletHub.allInMaster1();
    master1Service->tabletManager.addTablet(1, 0, ~0UL,
                                            TabletManager::NORMAL);
    ramcloud->write(1, "key0", 4, &oldInt64, sizeof(oldInt64), NULL, NULL);
    newInt64 = ramcloud->incrementInt64(1, "key0", 4, 2, NULL, &version);
    EXPECT_EQ(2U, version);
    EXPECT_EQ(3, newInt64);

    ramcloud->read(1, "key0", 4, &buffer);
    buffer.copy(0, sizeof(newInt64), &newInt64);
    EXPECT_EQ(3, newInt64);

    ramcloud->write(1, "key1", 4, &oldDouble, sizeof(oldDouble), NULL, NULL);
    newDouble = ramcloud->incrementDouble(1, "key1", 4, 2.0, NULL, &version);
    EXPECT_EQ(3U, version);
    EXPECT_DOUBLE_EQ(3.0, newDouble);

    buffer.reset();
    ramcloud->read(1, "key1", 4, &buffer);
    buffer.copy(0, sizeof(newDouble), &newDouble);
    EXPECT_EQ(3.0, newDouble);
}

TEST_F(MigrationSourceManagerTest, txPrepare_basics)
{
    uint64_t version;
    tabletHub.allInMaster1();
    master1Service->tabletManager.addTablet(1, 0, ~0UL,
                                            TabletManager::NORMAL);
    ramcloud->write(1, "key1", 4, "item1", 5);
    ramcloud->write(1, "key2", 4, "item2", 5);
    ramcloud->write(1, "key3", 4, "item3", 5);

    using WireFormat::TxParticipant;
    using WireFormat::TxPrepare;
    using WireFormat::TxDecision;

    Key key1(1, "key1", 4);
    Key key2(1, "key2", 4);
    Key key3(1, "key3", 4);
    Key key4(1, "key4", 4);
    Buffer buffer, buffer2;

    WireFormat::TxParticipant participants[4];
    participants[0] = TxParticipant(key1.getTableId(), key1.getHash(), 10U);
    participants[1] = TxParticipant(key2.getTableId(), key2.getHash(), 11U);
    participants[2] = TxParticipant(key3.getTableId(), key3.getHash(), 12U);
    participants[3] = TxParticipant(key4.getTableId(), key4.getHash(), 12U);

    WireFormat::TxPrepare::Request reqHdr;
    WireFormat::TxPrepare::Response respHdr;
    Buffer reqBuffer, respBuffer;
    Service::Rpc rpc(NULL, &reqBuffer, &respBuffer);

    reqHdr.common.opcode = WireFormat::Opcode::TX_PREPARE;
    reqHdr.common.service = WireFormat::MASTER_SERVICE;
    reqHdr.lease = {1, 10, 5};
    reqHdr.clientTxId = 1;
    reqHdr.ackId = 8;
    reqHdr.participantCount = 4;
    reqHdr.opCount = 3;
    reqBuffer.appendCopy(&reqHdr, sizeof32(reqHdr));
    reqBuffer.appendExternal(participants, sizeof32(TxParticipant) * 4);
    TransactionId txId(1U, 1U);

    // 2A. ReadOp
    RejectRules rejectRules;
    rejectRules = {1UL, false, false, false, true};
    TxPrepare::Request::ReadOp op1(key1.getTableId(),
                                   10,
                                   key1.getStringKeyLength(),
                                   rejectRules);
    reqBuffer.appendExternal(&op1, sizeof32(op1));
    reqBuffer.appendExternal(key1.getStringKey(), key1.getStringKeyLength());

    // 2B. RemoveOp
    rejectRules = {2UL, false, false, false, true};
    TxPrepare::Request::RemoveOp op2(key2.getTableId(),
                                     11,
                                     key2.getStringKeyLength(),
                                     rejectRules);

    reqBuffer.appendExternal(&op2, sizeof32(op2));
    reqBuffer.appendExternal(key2.getStringKey(), key2.getStringKeyLength());

    // 2C. WriteOp
    rejectRules = {3UL, false, false, false, true};
    Buffer keysAndValueBuf;
    Object::appendKeysAndValueToBuffer(key3, "new", 3, &keysAndValueBuf);
    TxPrepare::Request::WriteOp op3(key3.getTableId(),
                                    12,
                                    keysAndValueBuf.size(),
                                    rejectRules);

    reqBuffer.appendExternal(&op3, sizeof32(op3));
    reqBuffer.appendExternal(&keysAndValueBuf);

    // 3. Prepare.
    EXPECT_FALSE(isObjectLocked(master1Service, key1));
    EXPECT_FALSE(isObjectLocked(master1Service, key2));
    EXPECT_FALSE(isObjectLocked(master1Service, key3));
    EXPECT_FALSE(master1Service->objectManager.unackedRpcResults->hasRecord(
        txId.clientLeaseId,
        txId.clientTransactionId));
    {
        Buffer value;
        ramcloud->read(1, "key1", 4, &value, NULL, &version);
        EXPECT_EQ(1U, version);
        EXPECT_EQ("item1", string(reinterpret_cast<const char *>(
                                      value.getRange(0, value.size())),
                                  value.size()));
        value.reset();
        ramcloud->read(1, "key2", 4, &value, NULL, &version);
        EXPECT_EQ(2U, version);
        EXPECT_EQ("item2", string(reinterpret_cast<const char *>(
                                      value.getRange(0, value.size())),
                                  value.size()));
        value.reset();
        ramcloud->read(1, "key3", 4, &value, NULL, &version);
        EXPECT_EQ(3U, version);
        EXPECT_EQ("item3", string(reinterpret_cast<const char *>(
                                      value.getRange(0, value.size())),
                                  value.size()));
    }
    master1Service->txPrepare(&reqHdr, &respHdr, &rpc);

    uint64_t migrationId = 1;
    MigrationSourceStartRpc migrationRpc(
        &context, master1Server->serverId, migrationId, master1Server->serverId,
        targetServer->serverId, 1ul, 0ul, ~0ul);
    migrationRpc.wait();

    EXPECT_EQ(STATUS_OK, respHdr.common.status);
    EXPECT_EQ(TxPrepare::PREPARED, respHdr.vote);

    // 4. Check outcome of Prepare.
    EXPECT_EQ(3U, master1Service->transactionManager.items.size());
    EXPECT_TRUE(isObjectLocked(master1Service, key1));
    EXPECT_TRUE(isObjectLocked(master1Service, key2));
    EXPECT_TRUE(isObjectLocked(master1Service, key3));
    {
        TransactionManager::Lock lock(master1Service->transactionManager.mutex);
        EXPECT_TRUE(master1Service->transactionManager.getTransaction(txId,
                                                                      lock) !=
                    NULL);
    }
    Buffer value;
    ramcloud->read(1, "key1", 4, &value, NULL, &version);
    EXPECT_EQ(1U, version);
    EXPECT_EQ("item1", string(reinterpret_cast<const char *>(
                                  value.getRange(0, value.size())),
                              value.size()));
    value.reset();
    ramcloud->read(1, "key2", 4, &value, NULL, &version);
    EXPECT_EQ(2U, version);
    EXPECT_EQ("item2", string(reinterpret_cast<const char *>(
                                  value.getRange(0, value.size())),
                              value.size()));
    value.reset();
    ramcloud->read(1, "key3", 4, &value, NULL, &version);
    EXPECT_EQ(3U, version);
    EXPECT_EQ("item3", string(reinterpret_cast<const char *>(
                                  value.getRange(0, value.size())),
                              value.size()));

    MigrationSourceManager::Migration *migration =
        master1Service->migrationSourceManager.migrations[migrationId];
    EXPECT_FALSE(migration->active);

    WireFormat::TxDecision::Request decisionReqHdr;
    WireFormat::TxDecision::Response decisionRespHdr;
    Buffer decisionReqBuffer;
    Service::Rpc decisionRpc(NULL, &decisionReqBuffer, NULL);

    decisionReqHdr.decision = TxDecision::COMMIT;
    decisionReqHdr.leaseId = 1U;
    decisionReqHdr.transactionId = 1U;
    decisionReqHdr.recovered = false;
    decisionReqHdr.participantCount = 3U;
    decisionReqBuffer.appendExternal(&decisionReqHdr,
                                     sizeof32(decisionReqHdr));
    decisionReqBuffer.appendExternal(participants, sizeof32(TxParticipant) * 3);

    master1Service->txDecision(&decisionReqHdr, &decisionRespHdr, &decisionRpc);

    value.reset();
    ramcloud->read(1, "key3", 4, &value, NULL, &version);
    EXPECT_EQ(4U, version);
    EXPECT_EQ("new", string(reinterpret_cast<const char *>(
                                value.getRange(0, value.size())),
                            value.size()));

    EXPECT_FALSE(isObjectLocked(master1Service, key2));
    master1Service->migrationSourceManager.manager->handleTimerEvent();
    while (!migration->active);
    master1Service->transactionManager.cleaner.stop();
    master1Service->migrationSourceManager.stop();
}

TEST_F(MigrationSourceManagerTest, commit_basic)
{
    uint64_t tableId1 = 1;
    int txNum = 20;
    tabletHub.master1And2();
    master1Service->tabletManager.addTablet(1, 0, (~0UL) / 2,
                                            TabletManager::NORMAL);
    master2Service->tabletManager.addTablet(1, (~0UL) / 2 + 1, ~0UL,
                                            TabletManager::NORMAL);
    ramcloud->write(tableId1, "00000001", 1, "00000001", 6);
    Buffer value;
    std::unique_ptr<Tub<Transaction>[]> beforeInit(new Tub<Transaction>[txNum]);

    for (int i = 0; i < txNum; i++) {
        beforeInit[i].construct(ramcloud.get());
        string key1Str = format("%.8x", i * 2);
        string value1Str = format("%.8x before init", i * 2);
        string key2Str = format("%.8x", i * 2 + 1);
        string value2Str = format("%.8x before init", i * 2 + 1);
        beforeInit[i]->write(tableId1, key1Str.c_str(),
                             static_cast<uint16_t>(key1Str.length()),
                             value1Str.c_str(),
                             static_cast<uint32_t>(value1Str.length()));
        beforeInit[i]->write(tableId1, key2Str.c_str(),
                             static_cast<uint16_t>(key2Str.length()),
                             value2Str.c_str(),
                             static_cast<uint32_t>(value2Str.length()));
    }


    bool allPrepared = false;

    while (!allPrepared) {
        allPrepared = true;
        for (int i = 0; i < txNum; i++) {
            beforeInit[i]->taskPtr->testPrepare();
            if (beforeInit[i]->taskPtr->state !=
                ClientTransactionTask::DECISION &&
                beforeInit[i]->taskPtr->state != ClientTransactionTask::DONE)
                allPrepared = false;
        }
    }

    bool allDone = false;
    while (!allDone) {
        allDone = true;
        for (int i = 0; i < txNum / 2; i++) {
            beforeInit[i]->taskPtr->testDecision();
            if (beforeInit[i]->taskPtr->state != ClientTransactionTask::DONE)
                allDone = false;
        }
    }

    master1Service->migrationSourceManager.manager->handleTimerEvent();
    string key = format("%.8x", 1);
    ramcloud->read(tableId1, key.c_str(), static_cast<uint16_t>(key.length()),
                   &value);
    EXPECT_EQ("00000001 before init", string(
        reinterpret_cast<const char *>(value.getRange(0, value.size())),
            value.size()));


    uint64_t migrationId = 1;
    MigrationSourceStartRpc migrationRpc(
        &context, master1Server->serverId, migrationId, master1Server->serverId,
        targetServer->serverId, tableId1, 0ul, (~0ul) / 2);
    migrationRpc.wait();

    std::unique_ptr<Tub<Transaction>[]> afterInit(new Tub<Transaction>[txNum]);

    for (int i = 0; i < txNum; i++) {
        afterInit[i].construct(ramcloud.get());
        string key1Str = format("%.8x", i * 2 + 2 * txNum);
        string value1Str = format("%.8x after init", i * 2);
        string key2Str = format("%.8x", i * 2 + 1 + 2 * txNum);
        string value2Str = format("%.8x after init", i * 2 + 1);
        afterInit[i]->write(tableId1, key1Str.c_str(),
                            static_cast<uint16_t>(key1Str.length()),
                            value1Str.c_str(),
                            static_cast<uint32_t>(value1Str.length()));
        afterInit[i]->write(tableId1, key2Str.c_str(),
                            static_cast<uint16_t>(key2Str.length()),
                            value2Str.c_str(),
                            static_cast<uint32_t>(value2Str.length()));
    }

    MigrationSourceManager::Migration *migrationSource =
        master1Service->migrationSourceManager.migrations[migrationId];

    EXPECT_FALSE(migrationSource->active);

    allPrepared = false;

    while (!allPrepared) {
        allPrepared = true;
        for (int i = 0; i < txNum; i++) {
            afterInit[i]->taskPtr->testPrepare();
            if (afterInit[i]->taskPtr->state !=
                ClientTransactionTask::DECISION &&
                afterInit[i]->taskPtr->state != ClientTransactionTask::DONE)
                allPrepared = false;
        }
    }

    allDone = false;
    while (!allDone) {
        allDone = true;
        for (int i = txNum / 2; i < txNum; i++) {
            beforeInit[i]->taskPtr->testDecision();
            if (beforeInit[i]->taskPtr->state != ClientTransactionTask::DONE)
                allDone = false;
        }
    }

    master1Service->migrationSourceManager.manager->handleTimerEvent();

    while (!migrationSource->active);
    MigrationTargetManager::Migration *migrationTarget = NULL;
    while (migrationTarget == NULL) {
        migrationTarget =
            targetService->migrationTargetManager.getMigration(migrationId);
    }

    tabletHub.targetAndMaster2();

    std::unique_ptr<Tub<Transaction>[]> afterStart(new Tub<Transaction>[txNum]);
    for (int i = 0; i < txNum; i++) {
        afterStart[i].construct(ramcloud.get());
        string key1Str = format("%.8x", i * 2 + 2 * txNum);
        string value1Str = format("%.8x after start", i * 2);
        string key2Str = format("%.8x", i * 2 + 1 + 2 * txNum);
        string value2Str = format("%.8x after start", i * 2 + 1);
        afterStart[i]->write(tableId1, key1Str.c_str(),
                             static_cast<uint16_t>(key1Str.length()),
                             value1Str.c_str(),
                             static_cast<uint32_t>(value1Str.length()));
        afterStart[i]->write(tableId1, key2Str.c_str(),
                             static_cast<uint16_t>(key2Str.length()),
                             value2Str.c_str(),
                             static_cast<uint32_t>(value2Str.length()));
    }

    allDone = false;

    master1Service->transactionManager.cleaner.manager->handleTimerEvent();
    master2Service->transactionManager.cleaner.manager->handleTimerEvent();
    while (!allDone) {
        allDone = true;
        for (int i = 0; i < txNum; i++) {
            afterStart[i]->taskPtr->performTask();
            if (afterStart[i]->taskPtr->state != ClientTransactionTask::DONE) {
                allDone = false;
            }
        }
    }

    allDone = false;
    while (!allDone) {
        allDone = true;
        for (int i = 0; i < txNum; i++) {
            afterInit[i]->taskPtr->testDecision();

            if (afterInit[i]->taskPtr->state != ClientTransactionTask::DONE)
                allDone = false;
        }
    }

    //master 1
    ramcloud->write(2, "10000002", 8, "10000001", 8);
    //master 2
    ramcloud->write(3, "10000001", 8, "10000001", 8);
    //target
    ramcloud->write(4, "10000001", 8, "10000001", 8);

    master1Service->transactionManager.cleaner.manager->handleTimerEvent();
    master2Service->transactionManager.cleaner.manager->handleTimerEvent();

    master1Service->migrationSourceManager.manager->handleTimerEvent();
    master2Service->migrationSourceManager.manager->handleTimerEvent();
    targetService->migrationSourceManager.manager->handleTimerEvent();

    master1Service->transactionManager.cleaner.stop();
    master1Service->migrationSourceManager.stop();

    master2Service->transactionManager.cleaner.stop();
    master2Service->migrationSourceManager.stop();

    targetService->transactionManager.cleaner.stop();
    targetService->migrationSourceManager.stop();
}

struct TestStartNotifier : public MigrationSourceManager::StartNotifier {

    TestStartNotifier(TabletHub *tabletHub)
        : tabletHub(tabletHub)
    {

    }

    void notify(uint64_t migrationId)
    {
        tabletHub->targetAndMaster1();
    }

  private:
    TabletHub *tabletHub;
    DISALLOW_COPY_AND_ASSIGN(TestStartNotifier)
};

struct TestFinishNotifier : public MigrationTargetManager::FinishNotifier {

    void notify(uint64_t migrationId)
    {

    }

};

TEST_F(MigrationSourceManagerTest, migratingReadAndWrite)
{
    uint64_t tableId1 = 1;
    tabletHub.allInMaster1();
    master1Service->tabletManager.addTablet(1, 0, (~0UL) / 2,
                                            TabletManager::NORMAL);
    master1Service->tabletManager.addTablet(1, (~0UL) / 2 + 1, ~0UL,
                                            TabletManager::NORMAL);
    master1Service->migrationSourceManager.startNotifier.reset(
        new TestStartNotifier(&tabletHub));
    int requestNum = 10, iteration = 0;
    writeValues(tableId1, requestNum, iteration);

    uint64_t migrationId = 1;
    MigrationSourceStartRpc migrationRpc(
        &context, master1Server->serverId, migrationId, master1Server->serverId,
        targetServer->serverId, tableId1, 0ul, (~0ul) / 2);
    migrationRpc.wait();
    master1Service->transactionManager.cleaner.manager->handleTimerEvent();
    master1Service->migrationSourceManager.manager->handleTimerEvent();


    MigrationTargetManager::Migration *migrationTarget = NULL;
    while (migrationTarget == NULL) {
        validateValues(tableId1, requestNum, iteration);
        iteration += 1;
        usleep(1000);

        writeValues(tableId1, requestNum, iteration);

        migrationTarget =
            targetService->migrationTargetManager.getMigration(migrationId);
    }

    for (int i = 0; i < 10; i++) {
        validateValues(tableId1, requestNum, iteration);
        iteration += 1;
        writeValues(tableId1, requestNum, iteration);
    }

    targetService->migrationTargetManager.finishMigration(migrationId, true);

    for (int i = 0; i < 10; i++) {
        validateValues(tableId1, requestNum, iteration);
        iteration += 1;
        writeValues(tableId1, requestNum, iteration);
    }

    master1Service->transactionManager.cleaner.stop();
    master1Service->migrationSourceManager.stop();

    master2Service->transactionManager.cleaner.stop();
    master2Service->migrationSourceManager.stop();

    targetService->transactionManager.cleaner.stop();
    targetService->migrationSourceManager.stop();
}

}
