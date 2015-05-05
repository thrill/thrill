#pragma once

#include <memory> //std::shared_ptr
#include <c7a/net/net-dispatcher.hpp>
#include <c7a/net/channel.hpp>

namespace c7a {
namespace net {

//! Multiplexes virtual Connections on NetDispatcher
class ChannelMultiplexer
{
public:
    ChannelMultiplexer(NetDispatcher& dispatcher, int num_connections);

    void AddSocket(Socket& s);
    void ExpectHeaderFrom(Socket& s);
    bool HasChannel(int id);
    std::shared_ptr<Channel> PickupChannel(int id);

private:
    static const bool debug = true;
    typedef std::shared_ptr<Channel> ChannelPtr;

    //! Channels have an ID in block headers
    std::map<int, ChannelPtr> channels_;

    NetDispatcher& dispatcher_;
    int num_connections_;

    void ReadFirstHeaderPartFrom(Socket& s, const std::string& buffer);

};
}
}
