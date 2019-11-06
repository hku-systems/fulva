#include "AuxiliaryRpcWrapper.h"
#include "ClientException.h"

namespace RAMCloud {

AuxiliaryRpcWrapper::AuxiliaryRpcWrapper(Context *context,
                                         Transport::SessionRef session,
                                         uint32_t responseHeaderLength,
                                         Buffer *response)
    : RpcWrapper(responseHeaderLength, response), context(context),
      session(session)
{

}

bool AuxiliaryRpcWrapper::checkStatus()
{
    return true;
}

bool AuxiliaryRpcWrapper::handleTransportError()
{
    return true;
}

void AuxiliaryRpcWrapper::send()
{
    state = IN_PROGRESS;
    session->sendRequest(&request, response, this);
}

void AuxiliaryRpcWrapper::waitAndCheckErrors()
{

    waitInternal(context->auxDispatch);
    if (responseHeader->status != STATUS_OK)
        ClientException::throwException(HERE, responseHeader->status);
}

}
