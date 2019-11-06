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
#include "Cycles.h"
#include "CycleCounter.h"
#include "Common.h"
#include "MasterClient.h"
#include "Context.h"
#include "RamCloud.h"
#include "ShortMacros.h"

using namespace RAMCloud;

int
main(int argc, char *argv[])
try
{
    Context context(true);
    int clientIndex;
    int numClients;
    string tableName;
    uint64_t firstKey;
    uint64_t lastKey;
    uint32_t newOwnerMasterId;

    uint32_t objectCount = 0;
    uint32_t objectSize = 0;
    uint32_t otherObjectCount = 0;

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
             default_value(~0ul),
         "Last key of the tablet range to migrate")
        ("recipient,r",
         ProgramOptions::value<uint32_t>(&newOwnerMasterId)->
             default_value(2),
         "ServerId of the master to migrate to")
        ("objectCount",
         ProgramOptions::value<uint32_t>(&objectCount)->
             default_value(1000000),
         "Number of objects to pre-populate in the table to be migrated")
        ("objectSize",
         ProgramOptions::value<uint32_t>(&objectSize)->
             default_value(1000),
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

    string coordinatorLocator = optionParser.options.getCoordinatorLocator();
    RAMCLOUD_LOG(NOTICE, "client: Connecting to coordinator %s",
                 coordinatorLocator.c_str());

    RamCloud client(&context, coordinatorLocator.c_str());

    uint64_t tableId = client.createTable(tableName.c_str());
//    client.splitTablet(tableName.c_str(), lastKey + 1);
    client.testingFill(tableId, "", 0, objectCount, objectSize);
    const uint64_t totalBytes = objectCount * objectSize;

    RAMCLOUD_LOG(NOTICE, "Issuing migration request:");
    RAMCLOUD_LOG(NOTICE, "  table \"%s\" (%lu)", tableName.c_str(), tableId);
    RAMCLOUD_LOG(NOTICE, "  first key %lu", firstKey);
    RAMCLOUD_LOG(NOTICE, "  last key  %lu", lastKey);
    RAMCLOUD_LOG(NOTICE, "  recipient master id %u", newOwnerMasterId);

    {
        CycleCounter<> counter{};
        client.backupMigrate(tableId, firstKey, lastKey,
                             ServerId(newOwnerMasterId, 0), false);
        usleep(100000);
        while (!client.migrationQuery(1)) {
            usleep(100000);
        }
        double seconds = Cycles::toSeconds(counter.stop());
        RAMCLOUD_LOG(NOTICE, "Migration took %0.2f seconds", seconds);
        RAMCLOUD_LOG(NOTICE, "Migration took %0.2f MB/s",
                     double(totalBytes) / seconds / double(1 << 20));


    }

    return 0;
} catch (ClientException &e) {
    fprintf(stderr, "RAMCloud Client exception: %s\n", e.str().c_str());
    return 1;
} catch (RAMCloud::Exception &e) {
    fprintf(stderr, "RAMCloud exception: %s\n", e.str().c_str());
    return 1;
}
