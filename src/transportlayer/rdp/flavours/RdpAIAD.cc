
#include "RdpAIAD.h"
#include "../Rdp.h"

namespace inet {
namespace rdp {

Register_Class(RdpAIAD);

simsignal_t RdpAIAD::cwndSignal = cComponent::registerSignal("cwnd");    // will record changes to snd_cwnd
simsignal_t RdpAIAD::ssthreshSignal = cComponent::registerSignal("ssthresh");    // will record changes to ssthresh

RdpAIADStateVariables::RdpAIADStateVariables()
{
    internal_request_id = 0;
    request_id = 0;  // source block number (8-bit unsigned integer)

    pacingTime = 1200/1000000;
    lastPullTime = 0;
    numPacketsToGet = 0;
    numPacketsToSend = 0;
    congestionInWindow = false;

    numRcvTrimmedHeader = 0;
    numberReceivedPackets = 0;
    numberSentPackets = 0;
    IW = 0; // send the initial window (12 Packets as in RDP) IWWWWWWWWWWWW
    receivedPacketsInWindow = 0;
    sentPullsInWindow = 0;
    additiveIncreasePackets = 1;
    slowStartState = true;
    outOfWindowPackets = 0;
    waitToStart = false;
    ssthresh = 0;
    numRcvdPkt = 0;
    delayedNackNo = 0;
    connNotAddedYet = true;
    cwnd = 0;
    active = false;
}

RdpAIAD::RdpAIAD() :
        RdpAlgorithm(), state((RdpAIADStateVariables*&) RdpAlgorithm::state)
{

}

RdpAIAD::~RdpAIAD()
{

}

void RdpAIAD::initialize()
{

}

void RdpAIAD::connectionClosed()
{

}

void RdpAIAD::processTimer(cMessage *timer, RdpEventCode &event)
{

}

void RdpAIAD::dataSent(uint32 fromseq)
{

}

void RdpAIAD::ackSent()
{

}

void RdpAIAD::receivedHeader(unsigned int seqNum)
{
    EV_INFO << "Header arrived at the receiver" << endl;
    conn->sendNackRdp(seqNum);
    bool noPacketsInFlight = false;
    if(state->outOfWindowPackets > 0){
        state->outOfWindowPackets--;
    }
    else{
        state->sentPullsInWindow--;              //SEND PULLS FOR ALL CWND IF TWO HEADERS IN A ROW (FOR LOOP)
        state->receivedPacketsInWindow++;
    }

    if(state->outOfWindowPackets == 0){
        state->congestionInWindow = true;  //may have to change so certain amount of headers before multiplicative decrease
        state->ssthresh = state->cwnd/2;  //ssthresh, half of cwnd at loss event
        conn->emit(ssthreshSignal, state->ssthresh);
        state->cwnd = state->cwnd - 1;
        if(state->cwnd == 0) state->cwnd = 1;
        //if(state->receivedPacketsInWindow >= state->cwnd){
            state->outOfWindowPackets = state->sentPullsInWindow - state->cwnd;
            state->receivedPacketsInWindow = 0;
            state->sentPullsInWindow = state->cwnd;
            state->waitToStart = true;
        //}
    }

    if (state->numberReceivedPackets == 0 && state->connNotAddedYet == true) { //TODO never going to be called - IW is prioritised REMOVE
        conn->prepareInitialRequest();
        conn->addRequestToPullsQueue();
    }
    if(state->outOfWindowPackets < 0){
        state->congestionInWindow = false;
        int packetsMissing = state->outOfWindowPackets * -1;
        state->sentPullsInWindow = state->cwnd - packetsMissing;
        for(int i = 0; i < state->outOfWindowPackets * -1; i++){
            conn->addRequestToPullsQueue();

        }
        state->outOfWindowPackets = 0;
        //conn->paceChanged(state->sRtt.dbl()/(double(state->cwnd)));
        state->pacingTime = state->sRtt.dbl()/(double(state->cwnd));
        if(conn->getPullsQueueLength() > 0){
            conn->schedulePullTimer(); //should check if timer is scheduled, if is do nothing. Otherwise, schedule new timer based on previous time step.
        }
    }
    conn->emit(cwndSignal, state->cwnd);
}

void RdpAIAD::receivedData(unsigned int seqNum)
{
    int pullPacketsToSend = 0;
    bool windowIncreased = false;
    //If there are some packets still in the network following a trimmed header arrival
    if(state->outOfWindowPackets > 0){
        state->outOfWindowPackets--;
    }
    else{
        state->receivedPacketsInWindow++;
        state->sentPullsInWindow--;
    }
    EV_INFO << "Data packet arrived at the receiver - seq num " << seqNum << endl;
    unsigned int arrivedPktSeqNo = seqNum;
    conn->sendAckRdp(arrivedPktSeqNo); //TODO rename method to sendAck
    unsigned int seqNo = seqNum;
    state->numberReceivedPackets++;

    if (state->numberReceivedPackets >= state->numPacketsToGet) {
        EV_INFO << "All packets received - finish all connections!" << endl;
        conn->emit(cwndSignal, state->cwnd);
        EV_INFO << "Total Received Packets: " << state->numberReceivedPackets << endl;
        EV_INFO << "Total Wanted Packets: " << state->numPacketsToGet << endl;
        conn->sendPacketToApp(seqNum);
        conn->closeConnection();
        return;
    }

    if(state->cwnd < state->ssthresh) { //Slow-Start - Exponential Increase
        state->slowStartState = true;
        pullPacketsToSend++;
        state->cwnd++;
    }
    else{
        state->slowStartState = false;
    }

    if(state->outOfWindowPackets <= 0){
        if(((state->receivedPacketsInWindow % state->cwnd) == 0)){ //Congestion Avoidance - Linear Increase
            if(state->slowStartState){
                //pullPacketsToSend = state->cwnd;
                state->receivedPacketsInWindow = 0;
                windowIncreased = true;
                //state->cwnd = state->cwnd + state->cwnd;
                state->congestionInWindow = false;
            }
            else{
                windowIncreased = true;
                state->cwnd = state->cwnd + state->additiveIncreasePackets;
                pullPacketsToSend = pullPacketsToSend + state->additiveIncreasePackets;
                state->receivedPacketsInWindow = 0;
                state->congestionInWindow = false;
            }
        }
    }

    if (state->numberReceivedPackets == 1 && state->connNotAddedYet == true && state->outOfWindowPackets <= 0) {
        conn->prepareInitialRequest();
        conn->addRequestToPullsQueue();
        for(int pullNum = 0; pullNum < pullPacketsToSend; pullNum++){
            conn->addRequestToPullsQueue();
        }
    }
    else if((state->outOfWindowPackets <= 0) && (state->sentPullsInWindow < state->cwnd)){
        if (state->numberReceivedPackets <= state->numPacketsToGet) {
            //for loop sending pull for each pullPacketToSend;
            pullPacketsToSend++; //Pull Request to maintain current flow

            if(pullPacketsToSend > 0 && ((state->numberReceivedPackets + state->sentPullsInWindow + pullPacketsToSend) >= state->numPacketsToGet)){
                int newPullPacketsToSend = (state->numPacketsToGet - (state->numberReceivedPackets + state->sentPullsInWindow));
                if(newPullPacketsToSend < pullPacketsToSend) pullPacketsToSend = newPullPacketsToSend;
            } //maybe add this if statement to first window instance (see previous if block)

            for(int pullNum = 0; pullNum < pullPacketsToSend; pullNum++){
                conn->addRequestToPullsQueue();
            }
        }
    }

    EV_INFO << "Sending Data Packet to Application" << endl;
    conn->sendPacketToApp(seqNum);

    //conn->paceChanged(state->sRtt.dbl()/(double(state->cwnd)));
    state->pacingTime = state->sRtt.dbl()/(double(state->cwnd));
    if(conn->getPullsQueueLength() > 0){
        conn->schedulePullTimer(); //should check if timer is scheduled, if is do nothing. Otherwise, schedule new timer based on previous time step.
    }
    conn->emit(cwndSignal, state->cwnd);
}

} // namespace RDP

} // namespace inet

