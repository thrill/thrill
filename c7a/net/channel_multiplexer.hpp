/*******************************************************************************
 * c7a/net/channel_multiplexer.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_CHANNEL_MULTIPLEXER_HEADER
#define C7A_NET_CHANNEL_MULTIPLEXER_HEADER

#include <memory> //std::shared_ptr
#include <map>
#include <c7a/net/net_dispatcher.hpp>
#include <c7a/net/net_group.hpp>
#include <c7a/net/channel.hpp>
#include <c7a/data/block_emitter.hpp>
#include <c7a/data/socket_target.hpp>

namespace c7a {
namespace data {
struct BufferChain;
}
namespace net {
//! \ingroup net
//! \{

//! Multiplexes virtual Connections on NetDispatcher
//!
//! A worker as a TCP conneciton to each other worker to exchange large amounts
//! of data. Since multiple exchanges can occur at the same time on this single
//! connection we use multiplexing. The slices are called Blocks and are
//! indicated by a \ref StreamBlockHeader. Multiple Blocks form a Stream on a
//! single TCP connection. The multi plexer multiplexes all streams on all
//! sockets.
//!
//! All sockets are polled for headers. As soon as the a header arrives it is
//! either attached to an existing channel or a new channel instance is
//! created.
class ChannelMultiplexer {
public:
    ChannelMultiplexer(NetDispatcher& dispatcher)
    : dispatcher_(dispatcher),
      group_(nullptr) { }

    void Connect(std::shared_ptr<NetGroup> s) {
        group_ = s;
        for (size_t id = 0; id < group_->Size(); id++) {
            if (id == group_->MyRank()) continue;
            ExpectHeaderFrom(group_->Connection(id));
        }
    }

    std::vector<NetConnection*> GetSockets() {
        sLOG << "expect header on" << s;
        auto expected_size = sizeof(StreamBlockHeader::expected_bytes) + sizeof(StreamBlockHeader::channel_id);
        auto callback = std::bind(&ChannelMultiplexer::ReadFirstHeaderPartFrom, this, std::placeholders::_1, std::placeholders::_2);
        dispatcher_.AsyncRead(s, expected_size, callback);
    }

    //! Indicates if a channel exists with the given id
    bool HasChannel(int id) {
        return channels_.find(id) != channels_.end();
    }

    //! Returns the channel with the given ID or an onvalid pointer
    //! if the channel does not exist
    std::shared_ptr<Channel> PickupChannel(int id) {
        return channels_[id];
    }

    template <class T>
    std::vector<data::BlockEmitter<T> > OpenChannel(size_t id, std::shared_ptr<c7a::data::BufferChain> loopback) {
        std::vector<data::BlockEmitter<T> > result;
        for (size_t worker_id = 0; worker_id < group_->Size(); worker_id++) {
            if (worker_id == group_->MyRank()) {
                auto target = std::make_shared<data::LoopbackTarget>(loopback);
                result.emplace_back(data::BlockEmitter<T>(target));
            }
            else {
                auto target = std::make_shared<data::SocketTarget>(
                    &dispatcher_,
                    &(group_->Connection(worker_id)),
                    id);

                result.emplace_back(data::BlockEmitter<T>(target));
            }
        }
        return result;
    }

private:
    static const bool debug = true;
    typedef std::shared_ptr<Channel> ChannelPtr;

    //! Channels have an ID in block headers
    std::map<int, ChannelPtr> channels_;

    //Hols NetConnections for outgoing Channels
    std::shared_ptr<NetGroup> group_;

    NetDispatcher& dispatcher_;

    //! expects the next header from a socket
    void ExpectHeaderFrom(NetConnection& s);

    //! parses the channel id from a header and passes it to an existing
    //! channel or creates a new channel
    void ReadFirstHeaderPartFrom(
        NetConnection& s, const Buffer& buffer) {
        struct StreamBlockHeader header;
        header.ParseHeader(buffer.ToString());

        ChannelPtr channel;
        sLOG << "reading head for channel" << header.channel_id;
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
};
} // namespace net
} // namespace c7a

#endif // !C7A_NET_CHANNEL_MULTIPLEXER_HEADER

/******************************************************************************/
