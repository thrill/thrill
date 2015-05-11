/*******************************************************************************
 * c7a/net/channel-multiplexer.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "channel-multiplexer.hpp"
#include <c7a/common/logger.hpp>
#include <cassert>

namespace c7a {

ChannelMultiplexer::ChannelMultiplexer(NetDispatcher& dispatcher, int num_connections)
    : dispatcher_(dispatcher),
      num_connections_(num_connections) { }

void ChannelMultiplexer::AddSocket(Socket& s)
{
    LOG << "add socket" << s.GetFileDescriptor() << "to channel multiplexer";
    ExpectHeaderFrom(s);
}

void ChannelMultiplexer::ExpectHeaderFrom(Socket& s)
{
    LOG << "expect header on socket" << s.GetFileDescriptor();
    auto expected_size = sizeof(StreamBlockHeader::num_elements) + sizeof(StreamBlockHeader::channel_id);
    auto callback = std::bind(&ChannelMultiplexer::ReadFirstHeaderPartFrom, this, std::placeholders::_1, std::placeholders::_2);
    dispatcher_.AsyncRead(s, expected_size, callback);
}

bool ChannelMultiplexer::HasChannel(int id)
{
    return channels_.find(id) != channels_.end();
}

std::shared_ptr<Channel> ChannelMultiplexer::PickupChannel(int id)
{
    return channels_[id];
}

void ChannelMultiplexer::ReadFirstHeaderPartFrom(Socket& s, const std::string& buffer)
{
    struct StreamBlockHeader header;
    header.ParseIdAndNumElem(buffer);

    ChannelPtr channel;
    if (!HasChannel(header.channel_id)) {
        auto callback = std::bind(&ChannelMultiplexer::ExpectHeaderFrom, this, std::placeholders::_1);
        channel = std::make_shared<Channel>(dispatcher_, callback, header.channel_id, num_connections_);
        channels_.insert(std::make_pair(header.channel_id, channel));
    }
    else {
        channel = channels_[header.channel_id];
    }

    channel->PickupStream(s, header);
}

} // namespace c7a

/******************************************************************************/
