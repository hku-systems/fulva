#ifndef RAMCLOUD_AUXILIARYMANAGER_H
#define RAMCLOUD_AUXILIARYMANAGER_H

#include <boost/lockfree/spsc_queue.hpp>

#include "Dispatch.h"
#include "Transport.h"
#include "DpdkDriver.h"
#include "AuxiliaryTransport.h"
#include "CommQueue.h"

namespace RAMCloud {

class AuxiliaryManager : Dispatch::Poller {

  public:
    AuxiliaryManager(Context *context);

    ~AuxiliaryManager();

    int poll();

    void sendReply(Transport::ServerRpc *rpc);

    string getAuxiliaryLocator();

    Transport::SessionRef openSession(string locator);

  private:
    CommQueue<Transport::ServerRpc *> replyQueue;
    DpdkDriver *driver;
    AuxiliaryTransport *transport;

    DISALLOW_COPY_AND_ASSIGN(AuxiliaryManager)
};

}

#endif //RAMCLOUD_AUXILIARYMANAGER_H
