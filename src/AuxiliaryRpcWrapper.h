#ifndef RAMCLOUD_AUXILIARYRPCWRAPPER_H
#define RAMCLOUD_AUXILIARYRPCWRAPPER_H

#include "RpcWrapper.h"
#include "ServiceLocator.h"

namespace RAMCloud {

class AuxiliaryRpcWrapper : public RpcWrapper {
  public:

    explicit AuxiliaryRpcWrapper(
        Context *context, Transport::SessionRef session,
        uint32_t responseHeaderLength,
        Buffer *response = NULL);

    virtual ~AuxiliaryRpcWrapper()
    {}

  protected:
    virtual bool checkStatus();

    virtual bool handleTransportError();

    virtual void send();

    void waitAndCheckErrors();

    Context *context;
    Transport::SessionRef session;

    DISALLOW_COPY_AND_ASSIGN(AuxiliaryRpcWrapper);
};

}

#endif //RAMCLOUD_AUXILIARYRPCWRAPPER_H
