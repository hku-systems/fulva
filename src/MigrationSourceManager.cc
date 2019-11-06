

#include "MigrationSourceManager.h"
#include "MasterService.h"

namespace RAMCloud {

MigrationSourceManager::MigrationSourceManager(
    MasterService *masterService)
    : WorkerTimer(masterService->context->dispatch),
      masterService(masterService),
      transactionManager(&masterService->transactionManager),
      operatingBuffer(new std::queue<Item>[2]), operatingIndex(0),
      bufferLock("migrationBufferLock"), listLock(), migrations(), prepQueue(),
      activeQueue(), epoch(0), startNotifier(new RealStartNotifier())
{
}

MigrationSourceManager::~MigrationSourceManager()
{
}

void MigrationSourceManager::handleTimerEvent()
{
    std::queue<Item> *buffer;
    {
        SpinLock::Guard guard(bufferLock);
        buffer = &operatingBuffer[operatingIndex];
        operatingIndex = static_cast<uint8_t>(operatingIndex == 1 ? 0 : 1);
    }

    std::lock_guard<std::mutex> guard(listLock);
    while (!buffer->empty()) {
        Item &item = buffer->front();

        Migration *migration = migrations[item.migrationId];
        if (migration->timestamp < item.timestamp) {
            switch (item.type) {
                case LOCK:
                    migration->lock(item.keyHash);
                    break;
                case UNLOCK:
                    migration->unlock(item.keyHash);
                    break;
            }
        }
        buffer->pop();
    }
    epoch++;

    uint64_t minTimestamp = transactionManager->getMinTimestamp();
    if (!prepQueue.empty())
        start(0);
    std::list<Migration *>::iterator iterator = prepQueue.begin();
    while (iterator != prepQueue.end()) {
        Migration *migration = *iterator;
        if (minTimestamp > migration->timestamp) {
            RAMCLOUD_LOG(NOTICE, "Min timestamp: %lu. migration %lu start",
                         minTimestamp, migration->migrationId);
            migration->start();
            iterator = prepQueue.erase(iterator);
        } else {
            iterator++;
        }
    }

}

void MigrationSourceManager::startMigration(
    uint64_t migrationId, uint64_t tableId, uint64_t firstKeyHash,
    uint64_t lastKeyHash, uint64_t sourceId, uint64_t targetId)
{
    {
        Migration *migration = new Migration(this, migrationId, tableId,
                                             firstKeyHash,
                                             lastKeyHash, sourceId,
                                             targetId);
        migrations[migrationId] = migration;
        prepQueue.push_back(migration);
    }

    start(0);
}

void MigrationSourceManager::finishMigration(uint64_t migrationId)
{
    std::lock_guard<std::mutex> guard(listLock);
    std::unordered_map<uint64_t, Migration *>::iterator migrationPair =
        migrations.find(migrationId);
    if (migrationPair == migrations.end())
        return;
    Migration *migration = migrationPair->second;
    migration->finish();
    delete migration;
    migrations.erase(migrationPair);
}

MigrationSourceManager::Migration *
MigrationSourceManager::getMigration(uint64_t migrationId)
{
//    std::lock_guard<std::mutex> guard(listLock);
    std::unordered_map<uint64_t, Migration *>::iterator migrationPair =
        migrations.find(migrationId);
    if (migrationPair == migrations.end())
        return NULL;
    return migrationPair->second;
}

void
MigrationSourceManager::lock(uint64_t migrationId, Key &key, uint64_t timestamp)
{
    SpinLock::Guard guard(bufferLock);
    operatingBuffer[operatingIndex].emplace(
        Item{LOCK, migrationId, key.getHash(), timestamp});
}

void MigrationSourceManager::unlock(uint64_t migrationId, Key &key,
                                    uint64_t timestamp)
{
    SpinLock::Guard guard(bufferLock);
    operatingBuffer[operatingIndex].emplace(
        Item{UNLOCK, migrationId, key.getHash(), timestamp});
}

Status
MigrationSourceManager::isLocked(
    uint64_t migrationId, Key &key, bool *isLocked,
    vector<WireFormat::MigrationIsLocked::Range> &ranges)
{
    auto migration = migrations.find(migrationId);
    if (migration == migrations.end())
        return STATUS_OBJECT_DOESNT_EXIST;
    if (migration->second->startEpoch >= epoch)
        throw RetryException(HERE, 10, 100,
                             "wait at least one epoch to clear buffer");
    if (isLocked)
        *isLocked = masterService->objectManager.isLocked(key);
    migration->second->rangeList.getRanges(ranges);

    return STATUS_OK;
}

MigrationSourceManager::Migration::Migration(
    MigrationSourceManager *manager,
    uint64_t migrationId,
    uint64_t tableId,
    uint64_t firstKeyHash,
    uint64_t lastKeyHash,
    uint64_t sourceId,
    uint64_t targetId)
    : manager(manager), rangeList(firstKeyHash, lastKeyHash),
      migrationId(migrationId), tableId(tableId),
      firstKeyHash(firstKeyHash), lastKeyHash(lastKeyHash), sourceId(sourceId),
      targetId(targetId), timestamp(Cycles::rdtsc()), startEpoch(0),
      active(false)
{
}

void MigrationSourceManager::Migration::lock(uint64_t keyHash)
{
    rangeList.lock(keyHash);
}

void MigrationSourceManager::Migration::unlock(uint64_t keyHash)
{
    rangeList.unlock(keyHash);
}

void MigrationSourceManager::Migration::start()
{
    active = true;
    manager->masterService->tabletManager.migrateTablet(
        tableId, firstKeyHash, lastKeyHash, migrationId, sourceId, targetId,
        TabletManager::MIGRATION_SOURCE);

    auto replicas = manager->masterService->objectManager.getReplicas();

    ServerId sourceServerId(sourceId);
    ServerId targetServerId(targetId);
    startEpoch = manager->epoch;

    if (manager->startNotifier)
        manager->startNotifier->notify(migrationId);

    MasterClient::migrationTargetStart(
        manager->masterService->context, targetServerId, migrationId,
        sourceServerId, targetServerId, tableId, firstKeyHash,
        lastKeyHash, manager->masterService->tabletManager.getSafeVersion(),
        replicas.data(), downCast<uint32_t>(replicas.size()));
}

void MigrationSourceManager::Migration::finish()
{
    RAMCLOUD_LOG(NOTICE, "migration %lu finished", migrationId);
    manager->masterService->tabletManager.deleteTablet(tableId, firstKeyHash,
                                                       lastKeyHash);
    manager->masterService->objectManager.removeOrphanedObjects();

}

void MigrationSourceManager::RealStartNotifier::notify(uint64_t migrationId)
{
}

}
