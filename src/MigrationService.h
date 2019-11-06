#ifndef RAMCLOUD_MIGRATIONSERVICE_H
#define RAMCLOUD_MIGRATIONSERVICE_H

#include "Service.h"
#include "ServerConfig.h"

namespace RAMCloud {

class MigrationService : public Service {
  PUBLIC:

    MigrationService(Context *context, const ServerConfig *config);

  PRIVATE:

};

}

#endif //RAMCLOUD_MIGRATIONSERVICE_H
