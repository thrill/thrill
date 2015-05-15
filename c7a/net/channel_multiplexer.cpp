/*******************************************************************************
 * c7a/net/channel_multiplexer.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "channel_multiplexer.hpp"
#include <c7a/common/logger.hpp>
#include <cassert>

namespace c7a {
namespace net {
ChannelMultiplexer::ChannelMultiplexer(NetDispatcher& dispatcher)
    : dispatcher_(dispatcher),
      group_(nullptr) { }

void ChannelMultiplexer::Connect(std::shared_ptr<NetGroup> s) {
    group_ = s;
    for (size_t id = 0; id < group_->Size(); id++) {
        if (id == group_->MyRank()) continue;
        ExpectHeaderFrom(group_->Connection(id));
    }
}

void ChannelMultiplexer::ExpectHeaderFrom(NetConnection& s) {
    sLOG << "expect header on" << s;
    auto expected_size = sizeof(StreamBlockHeader::expected_bytes) + sizeof(StreamBlockHeader::channel_id);
    auto callback = std::bind(&ChannelMultiplexer::ReadFirstHeaderPartFrom, this, std::placeholders::_1, std::placeholders::_2);
    dispatcher_.AsyncRead(s, expected_size, callback);
}

bool ChannelMultiplexer::HasChannel(int id) {
    return channels_.find(id) != channels_.end();
}

std::shared_ptr<Channel> ChannelMultiplexer::PickupChannel(int id) {
    return channels_[id];
}

void ChannelMultiplexer::ReadFirstHeaderPartFrom(
    NetConnection& s, const Buffer& buffer) {
    struct StreamBlockHeader header;
    header.ParseHeader(buffer.ToString());

    ChannelPtr channel;
    if (!HasChannel(header.channel_id)) {
        auto callback = std::bind(&ChannelMultiplexer::ExpectHeaderFrom, this, std::placeholders::_1);
        channel = std::make_shared<Channel>(dispatcher_, callback, header.channel_id, group_->Size());
        channels_.insert(std::make_pair(header.channel_id, channel));
    }
    else {
        channel = channels_[header.channel_id];
    }

    channel->PickupStream(s, header);
}
} // namespace net
} // namespace c7a

/******************************************************************************/
