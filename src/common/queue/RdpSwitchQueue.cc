//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program.  If not, see http://www.gnu.org/licenses/.
// 

#include <inet/common/INETDefs.h>
#include <inet/common/ModuleAccess.h>
#include "inet/common/PacketEventTag.h"
#include <inet/common/Simsignals.h>
#include "inet/common/TimeTag.h"
#include <inet/queueing/function/PacketComparatorFunction.h>
#include <inet/queueing/function/PacketDropperFunction.h>
#include <inet/networklayer/ipv4/Ipv4Header_m.h>
#include <inet/queueing/base/PacketQueueBase.h>

#include "../../application/rdpapp/GenericAppMsgRdp_m.h"
#include "../../transportlayer/rdp/rdp_common/RdpHeader.h"
#include "RdpSwitchQueue.h"

namespace inet {
namespace queueing {
Define_Module(RdpSwitchQueue);

simsignal_t RdpSwitchQueue::dataQueueingTimeSignal = registerSignal("dataQueueingTime");
simsignal_t RdpSwitchQueue::headerQueueingTimeSignal = registerSignal("headerQueueingTime");
simsignal_t RdpSwitchQueue::dataQueueLengthSignal = registerSignal("dataQueueLength");
simsignal_t RdpSwitchQueue::headersQueueLengthSignal = registerSignal("headersQueueLength");
simsignal_t RdpSwitchQueue::numTrimmedPktSig = registerSignal("numTrimmedPkt");
simsignal_t RdpSwitchQueue::headerPacketPulledSignal = registerSignal("headerPacketPulled");
simsignal_t RdpSwitchQueue::headerPacketDroppedSignal = registerSignal("headerPacketDropped");
simsignal_t RdpSwitchQueue::headerPacketRemovedSignal = registerSignal("headerPacketRemoved");



void RdpSwitchQueue::emit(simsignal_t signal, cObject *object, cObject *details)
{
    if (signal == packetPushedSignal)
        numPushedPackets++;
    else if (signal == packetPulledSignal)
        numPulledPackets++;
    else if (signal == packetRemovedSignal)
        numRemovedPackets++;
    else if (signal == packetDroppedSignal)
        numDroppedPackets++;
    else if (signal == headerPacketPulledSignal)
        numPulledHeaderPackets++;
    else if (signal == headerPacketRemovedSignal)
        numRemovedHeaderPackets++;
    else if (signal == headerPacketDroppedSignal)
        numDroppedHeaderPackets++;
    cSimpleModule::emit(signal, object, details);
}


void RdpSwitchQueue::initialize(int stage)
{
    PacketQueueBase::initialize(stage);
    if (stage == INITSTAGE_LOCAL) {
        numTrimmedPacketsVec.setName("numTrimmedPacketsVec");
        weight=10;
        dataQueueLength = 0 ;
        headersQueueLength = 0;
        synAckQueueLength=0;
        numTrimmedPkt=0;

        numPushedHeaderPackets = 0;
        numPulledHeaderPackets = 0;
        numRemovedHeaderPackets = 0;
        numDroppedHeaderPackets = 0;
        numCreatedHeaderPackets = 0;
        WATCH(numPushedHeaderPackets);
        WATCH(numPulledHeaderPackets);
        WATCH(numRemovedHeaderPackets);
        WATCH(numDroppedHeaderPackets);

        producer = findConnectedModule<IActivePacketSource>(inputGate);
        collector = findConnectedModule<IActivePacketSink>(outputGate);
        WATCH(dataQueueLength);
        WATCH(headersQueueLength);
        WATCH(synAckQueueLength);
        WATCH(numTrimmedPkt);

        dataQueue.setName("dataQueue");
        headersQueue.setName("headerQueue");
        synAckQueue.setName("synAckQueue");
        // configuration
        packetCapacity = par("packetCapacity");
        // moh added
        recordScalar("packetCapacity= ", packetCapacity);
    }
    else if (stage == INITSTAGE_QUEUEING) {
            checkPacketOperationSupport(inputGate);
            checkPacketOperationSupport(outputGate);
            if (producer != nullptr)
                producer->handleCanPushPacketChanged(inputGate->getPathStartGate());
        }
    else if (stage == INITSTAGE_LAST)
        updateDisplayString();
    //emit()
    //statistics
    cSimpleModule::emit(dataQueueLengthSignal, dataQueue.getLength());
    cSimpleModule::emit(headersQueueLengthSignal, headersQueue.getLength());
    cSimpleModule::emit(numTrimmedPktSig, numTrimmedPkt);
}

bool RdpSwitchQueue::isOverloaded() const
{
    return dataQueue.getLength() >= packetCapacity;
}

int RdpSwitchQueue::getNumPackets() const
{
    return dataQueue.getLength();
}

int RdpSwitchQueue::getSynAckQueueNumPackets() const
{
    return synAckQueue.getLength();
}

int RdpSwitchQueue::getHeaderQueueNumPackets() const
{
    return headersQueue.getLength();
}

Packet *RdpSwitchQueue::getPacket(int index) const
{
    if (index < 0 || index >= dataQueue.getLength())
        throw cRuntimeError("index %i out of range", index);
    return check_and_cast<Packet *>(dataQueue.get(index));
}

void RdpSwitchQueue::pushPacket(Packet *packet, cGate *gate)
{
    Enter_Method("pushPacket");
    take(packet);
    cNamedObject packetPushStartedDetails("atomicOperationStarted");
    emit(packetPushStartedSignal, packet, &packetPushStartedDetails);
    EV_INFO << "PACKET STRING" << packet->str() << endl;
    EV_INFO << "Pushing packet " << packet->getName() << " into the queue." << endl;
    const auto& ipv4Datagram = packet->peekAtFront<Ipv4Header>();
    const auto& rdpHeaderPeek = packet->peekDataAt<rdp::RdpHeader>(ipv4Datagram->getChunkLength());
    if ( rdpHeaderPeek->getAckBit()==true || rdpHeaderPeek->getSynBit()==true || rdpHeaderPeek->getNackBit()==true) {
       synAckQueue.insert(packet);
       synAckQueueLength=synAckQueue.getLength();
       goto pushEnded;
    }
    else if (rdpHeaderPeek->isHeader() == true ) {
        headersQueue.insert(packet);
        headersQueueLength = headersQueue.getLength();
        goto pushEnded;
    }
    else if (rdpHeaderPeek->isPullPacket() == true ) {
        headersQueue.insert(packet);
        headersQueueLength = headersQueue.getLength();
        goto pushEnded;
    }
    else if (isOverloaded()) {
        std::string header="Header-";
        auto ipv4Header = packet->removeAtFront<Ipv4Header>();
        ASSERT(B(ipv4Header->getTotalLengthField()) >= ipv4Header->getChunkLength());
        if (ipv4Header->getTotalLengthField() < packet->getDataLength())
            packet->setBackOffset(B(ipv4Header->getTotalLengthField()) - ipv4Header->getChunkLength());
        auto rdpHeader = packet->removeAtFront<rdp::RdpHeader>();
        packet->removeAtFront<GenericAppMsgRdp>();
        if (rdpHeader != nullptr) {
            std::string name=packet->getName();
            std::string rename=header+name;
            packet->setName(rename.c_str());
            rdpHeader->setIsHeader(true);
            rdpHeader->setIsDataPacket(false);

            unsigned short srcPort = rdpHeader->getSrcPort();
            unsigned short destPort = rdpHeader->getDestPort();
            EV << "RdpSwitchQueue srcPort:" << srcPort << endl;
            EV << "RdpSwitchQueue destPort:" << destPort << endl;
            EV << "RdpSwitchQueue Header Full Name:" << rdpHeader->getFullName() << endl;
        }
        packet->insertAtFront(rdpHeader);
        ipv4Header->setTotalLengthField(ipv4Header->getChunkLength() + packet->getDataLength());
        packet->insertAtFront(ipv4Header);
        headersQueue.insert(packet);
        headersQueueLength=headersQueue.getLength();
        //emit(packetPushedHeadersQueueSignal, packet);
        ++numTrimmedPkt;
        numTrimmedPacketsVec.record(numTrimmedPkt);
        cSimpleModule::emit(numTrimmedPktSig, numTrimmedPkt);
        goto pushEnded;
    }
    else {
        dataQueue.insert(packet);
        dataQueueLength=dataQueue.getLength();
        goto pushEnded;
    }
    pushEnded:
        if (collector != nullptr && !isEmpty())
                collector->handleCanPullPacketChanged(outputGate->getPathEndGate());
    cNamedObject packetPushEndedDetails("atomicOperationEnded");
    emit(packetPushEndedSignal, nullptr, &packetPushEndedDetails);
    updateDisplayString();
    return;
}


Packet *RdpSwitchQueue::pullPacket(cGate *gate) {
    Enter_Method("pullPacket");
    if (dataQueue.isEmpty() && headersQueue.isEmpty() && synAckQueue.isEmpty()){
        return nullptr;
    }
    else if (synAckQueue.getLength()!=0){  //syn/ack pop
        auto packet = check_and_cast<Packet *>(synAckQueue.pop());
        take(packet);
        auto queueingTime = simTime() - packet->getArrivalTime();
        auto packetEvent = new PacketQueuedEvent();
        packetEvent->setQueuePacketLength(getSynAckQueueNumPackets());
        packetEvent->setQueueDataLength(getSynAckQueueTotalLength());
        insertPacketEvent(this, packet, PEK_QUEUED, queueingTime, packetEvent);
        increaseTimeTag<QueueingTimeTag>(packet, queueingTime, queueingTime);
        PacketProcessorBase::emit(headerPacketPulledSignal, packet);
        animatePullPacket(packet, outputGate);
        updateDisplayString();
        return packet;
    }
    else if (headersQueue.getLength() == 0 && dataQueue.getLength() != 0) { //dataQueue pop
        auto packet = check_and_cast<Packet *>(dataQueue.pop());
        take(packet);
        auto queueingTime = simTime() - packet->getArrivalTime();
        auto packetEvent = new PacketQueuedEvent();
        packetEvent->setQueuePacketLength(getNumPackets());
        packetEvent->setQueueDataLength(getTotalLength());
        insertPacketEvent(this, packet, PEK_QUEUED, queueingTime, packetEvent);
        increaseTimeTag<QueueingTimeTag>(packet, queueingTime, queueingTime);
        PacketProcessorBase::emit(packetPulledSignal, packet);
        cSimpleModule::emit(dataQueueLengthSignal, dataQueue.getLength());
        animatePullPacket(packet, outputGate);
        updateDisplayString();
        return packet;
    }
    else if (headersQueue.getLength() != 0 && dataQueue.getLength() == 0) { //header/pull pop
        auto packet = check_and_cast<Packet *>(headersQueue.pop());
        take(packet);
        auto queueingTime = simTime() - packet->getArrivalTime();
        auto packetEvent = new PacketQueuedEvent();
        packetEvent->setQueuePacketLength(getHeaderQueueNumPackets());
        packetEvent->setQueueDataLength(getHeaderQueueTotalLength());
        insertPacketEvent(this, packet, PEK_QUEUED, queueingTime, packetEvent);
        increaseTimeTag<QueueingTimeTag>(packet, queueingTime, queueingTime);
        PacketProcessorBase::emit(headerPacketPulledSignal, packet);
        cSimpleModule::emit(headersQueueLengthSignal, headersQueue.getLength());
        animatePullPacket(packet, outputGate);
        updateDisplayString();
        return packet;
    }
    else if ( headersQueue.getLength() != 0 && dataQueue.getLength() != 0 && weight%10 == 0) { //round robin dataQueue pop
        auto packet = check_and_cast<Packet *>(dataQueue.pop());
        take(packet);
        auto queueingTime = simTime() - packet->getArrivalTime();
        auto packetEvent = new PacketQueuedEvent();
        packetEvent->setQueuePacketLength(getNumPackets());
        packetEvent->setQueueDataLength(getTotalLength());
        insertPacketEvent(this, packet, PEK_QUEUED, queueingTime, packetEvent);
        increaseTimeTag<QueueingTimeTag>(packet, queueingTime, queueingTime);
        PacketProcessorBase::emit(packetPulledSignal, packet);
        cSimpleModule::emit(dataQueueLengthSignal, dataQueue.getLength());
        animatePullPacket(packet, outputGate);
        updateDisplayString();
        ++weight;
        return packet;
    }
    else if (headersQueue.getLength() != 0 && dataQueue.getLength() != 0 ) {
        auto packet = check_and_cast<Packet *>(headersQueue.pop());
        take(packet);
        auto queueingTime = simTime() - packet->getArrivalTime();
        auto packetEvent = new PacketQueuedEvent();
        packetEvent->setQueuePacketLength(getHeaderQueueNumPackets());
        packetEvent->setQueueDataLength(getHeaderQueueTotalLength());
        insertPacketEvent(this, packet, PEK_QUEUED, queueingTime, packetEvent);
        increaseTimeTag<QueueingTimeTag>(packet, queueingTime, queueingTime);
        PacketProcessorBase::emit(headerPacketPulledSignal, packet);
        cSimpleModule::emit(headersQueueLengthSignal, headersQueue.getLength());
        animatePullPacket(packet, outputGate);
        EV_INFO << " get from header queue- size = " << packet->getByteLength() << endl;
        updateDisplayString();
        ++weight;
        return packet;
    }
    return nullptr;
}

void RdpSwitchQueue::removePacket(Packet *packet)
{
    Enter_Method("removePacket");
    EV_INFO << "Removing packet " << packet->getName() << " from the queue." << endl;
    dataQueue.remove(packet);
    PacketProcessorBase::emit(packetRemovedSignal, packet);
    updateDisplayString();
}

bool RdpSwitchQueue::isEmpty() const
{
    bool a = headersQueue.isEmpty();
    bool b = synAckQueue.isEmpty();
    bool c = dataQueue.isEmpty();
    bool d;
    if (a == true && b == true && c == true) {
        d = true;
    }
    else {
        d = false;
    }

    return d;
}

void RdpSwitchQueue::removeAllPackets()
{
    Enter_Method("removeAllPacket");
    updateDisplayString();
}


void RdpSwitchQueue::finish(){
    recordScalar("numTrimmedPkt ",numTrimmedPkt );
    cSimpleModule::emit(numTrimmedPktSig, numTrimmedPkt);
}

}
} // namespace inet