#ifndef RAMCLOUD_CLIENTSERVERIDRPCWRAPPER_H
#define RAMCLOUD_CLIENTSERVERIDRPCWRAPPER_H


#include "RpcWrapper.h"

namespace RAMCloud {

class ClientServerIdRpcWrapper : public RpcWrapper {
  PUBLIC:

    explicit ClientServerIdRpcWrapper(
        Context *context, string locator, uint32_t responseHeaderLength,
        Buffer *response = NULL);

    virtual ~ClientServerIdRpcWrapper()
    {}

    DISALLOW_COPY_AND_ASSIGN(ClientServerIdRpcWrapper)

  PROTECTED:

    void waitAndCheckErrors();

    Context *context;

    string locator;

    int transportErrors;

    bool serverCrashed;

};

}

#endif //RAMCLOUD_CLIENTSERVERIDRPCWRAPPER_H
