#include "ClientServerIdRpcWrapper.h"
#include "Context.h"
#include "ServerList.h"
#include "ClientException.h"
#include "TransportManager.h"
#include "FailSession.h"

namespace RAMCloud {

ClientServerIdRpcWrapper::ClientServerIdRpcWrapper(
    Context *context, string locator, uint32_t responseHeaderLength,
    Buffer *response) :
    RpcWrapper(responseHeaderLength, response),
    context(context), locator(locator), transportErrors(0),
    serverCrashed(false)
{
    try {
        session = context->transportManager->getSession(locator);
    } catch (const TransportException &e) {
        session = FailSession::get();
    }
}

void ClientServerIdRpcWrapper::waitAndCheckErrors()
{
    // Note: this method is a generic shared version for RPCs that don't
    // return results and don't need to do any processing of the response
    // packet except checking for errors.
    waitInternal(context->dispatch);
    if (serverCrashed) {
        throw ServerNotUpException(HERE);
    }
    if (responseHeader->status != STATUS_OK)
        ClientException::throwException(HERE, responseHeader->status);
}
}
