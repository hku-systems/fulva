#include "AuxiliaryManager.h"
#include "Fence.h"

namespace RAMCloud {

AuxiliaryManager::AuxiliaryManager(Context *context)
    : Dispatch::Poller(context->auxDispatch, "AuxiliaryManager"),
      replyQueue(100), driver(NULL), transport(NULL)
{
    driver = new DpdkDriver(context, 1);
    transport = new AuxiliaryTransport(context, NULL, driver, false,
                                       generateRandom());
}

AuxiliaryManager::~AuxiliaryManager()
{

}

int AuxiliaryManager::poll()
{
    int workDone = 0;
    while (!replyQueue.empty()) {
        replyQueue.front()->sendReply();
        replyQueue.pop();
        workDone = 1;
    }
    return workDone;
}

void AuxiliaryManager::sendReply(Transport::ServerRpc *rpc)
{
    replyQueue.push(rpc);
}

string AuxiliaryManager::getAuxiliaryLocator()
{
    return transport->getServiceLocator();
}

Transport::SessionRef AuxiliaryManager::openSession(string locator)
{
    vector<ServiceLocator> locators = ServiceLocator::parseServiceLocators(
        locator);
    return transport->getSession(&locators[0]);
}

}
