
#include "DumbRdp.h"

#include "../Rdp.h"

namespace inet {

namespace rdp {

Register_Class(DumbRdp);

DumbRdp::DumbRdp() :
        RdpAlgorithm(), state((DumbRdpStateVariables*&) RdpAlgorithm::state)
{

}

DumbRdp::~DumbRdp()
{

}

void DumbRdp::initialize()
{

}

void DumbRdp::connectionClosed()
{

}

void DumbRdp::processTimer(cMessage *timer, RdpEventCode &event)
{

}

void DumbRdp::dataSent(uint32_t fromseq)
{

}

void DumbRdp::ackSent()
{

}

void DumbRdp::receivedHeader(unsigned int seqNum)
{

}

void DumbRdp::receivedData(unsigned int seqNum, bool isMarked)
{

}

} // namespace RDP

} // namespace inet

