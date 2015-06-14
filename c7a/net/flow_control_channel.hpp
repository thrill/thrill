/*******************************************************************************
 * c7a/net/flow_control_channel.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_FLOW_CONTROL_CHANNEL_HEADER
#define C7A_NET_FLOW_CONTROL_CHANNEL_HEADER

#include <string>
#include <vector>
#include <c7a/net/group.hpp>

namespace c7a {
namespace net {

/**
 * @brief Provides a blocking collection for communication.
 * @details This should be used for flow control.
 *
 */
class FlowControlChannel
{
protected:
    net::Group& group;
    int id;
    int count;

public:
    explicit FlowControlChannel(net::Group& group) : group(group), id(group.MyRank()), count(group.Size()) { }

    //base methods
    template <typename T>
    void SendTo(ClientId destination, T value)  {
        group.connection(destination).Send(value);
    }
    //void SendToAny(T message);  // No, we won't do that :P
    template <typename T>
    void ReceiveFrom(ClientId source, T* value) {
        group.connection(source).Receive(value);
    }

    //Concrete methods - signatures stolen from Robert. 
    template <typename T, typename BinarySumOp = common::SumOp<T> >
    T PrefixSum(T& value, BinarySumOp sumOp = BinarySumOp()) {
        if(id == 0) { //I am first
            if(count > 1) {
                SendTo(id + 1, value);
            } 
            return value;
        } else {
            if(count > 1) {
                T res;
                ReceiveFrom(id - 1, &res);
                res = sumOp(res, value);
                if(id < count - 1) { //I am not last
                    SendTo(id - 1, res);
                }
                return res;
            } else {
                return value;
            }
        } 
    } 

    template <typename T>
    T Broadcast(T& value) {

        T res;

        if(id == 0) { //I am the master
            for(int i = 1; i < count; i++) {
                SendTo(i, value);
            }
            res = value;
        } else { //I am someone else
            ReceiveFrom(0, &res);
        }

        return res;
    }

    template <typename T, typename BinarySumOp = common::SumOp<T> >
    T AllReduce(T& value, BinarySumOp sumOp = BinarySumOp()) {
        T res = value;
        
        if(id == 0) { //I am the master
            T msg;
            for(int i = 1; i < count; i++) {
                ReceiveFrom(i, &msg);
                res = sumOp(res, msg);
            }
        } else {
            SendTo(0, res);
        }  
        res = Broadcast(0, res);

        return res;
    }
};

} // namespace net
} // namespace c7a

#endif //! C7A_NET_FLOW_CONTROL_CHANNEL_HEADER