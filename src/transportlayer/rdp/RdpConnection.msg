

import inet.common.INETDefs;
import inet.common.packet.ChunkBuffer;
import inet.common.packet.ChunkQueue;
import inet.common.packet.Packet;
import inet.networklayer.common.L3Address;

import RdpConnectionState;

cplusplus {{
#include "Rdp.h"
#include "RdpAlgorithm.h"
#include "RdpConnection.h"
#include "RdpSendQueueOptimisation.h"
#include "RdpReceiveQueue.h"
#include "rdp_common/RdpHeader.h"
}}

namespace inet::rdp;

class RdpAlgorithm extends cObject { @existingClass; }

class RdpSendQueueOptimisation extends cObject
{
    @existingClass;
    @descriptor(readonly);
    //std::vector<int> seqNumToSendQueue;
}

class RdpReceiveQueue extends cObject
{
    @existingClass;
    @descriptor(readonly);
    cPacketQueue receiveBuffer;      // dataBuffer
}

class RdpConnection extends cObject
{
    @existingClass;
    @descriptor(readonly);

    int socketId;    // identifies connection within the app
    int listeningSocketId;    // identifies listening connection within the app

    // socket pair
    L3Address localAddr;
    L3Address remoteAddr;
    int localPort;
    int remotePort;

    RdpStateVariables *state;

    // NDP queues
    RdpSendQueueOptimisation *sendQueue;
	RdpReceiveQueue *receiveQueue;
	
    RdpAlgorithm *rdpAlgorithm;
    int fsmState;
};

