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
#include <c7a/data/buffer_chain_manager.hpp>
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
        : group_(nullptr),
          dispatcher_(dispatcher) { }

    void Connect(std::shared_ptr<NetGroup> s) {
        group_ = s;
        for (size_t id = 0; id < group_->Size(); id++) {
            if (id == group_->MyRank()) continue;
            ExpectHeaderFrom(group_->Connection(id));
        }
    }

    //! Indicates if a channel exists with the given id
    bool HasChannel(int id) {
        return chains_.Contains(id);
    }

    std::shared_ptr<data::BufferChain> AccessData(size_t id) {
        return chains_.Chain(id);
    }

    size_t AllocateNext() {
        return chains_.AllocateNext();
    }

    template <class T>
    std::vector<data::BlockEmitter<T> > OpenChannel(size_t id) {
        std::vector<data::BlockEmitter<T> > result;
        for (size_t worker_id = 0; worker_id < group_->Size(); worker_id++) {
            if (worker_id == group_->MyRank()) {
                auto target = std::make_shared<data::LoopbackTarget>(chains_.Chain(id));
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
    data::BufferChainManager chains_;

    //Hols NetConnections for outgoing Channels
    std::shared_ptr<NetGroup> group_;

    NetDispatcher& dispatcher_;

    //! expects the next header from a socket
    void ExpectHeaderFrom(NetConnection& s) {
        auto expected_size = sizeof(StreamBlockHeader::expected_bytes) + sizeof(StreamBlockHeader::channel_id);
        auto callback = std::bind(&ChannelMultiplexer::ReadFirstHeaderPartFrom, this, std::placeholders::_1, std::placeholders::_2);
        dispatcher_.AsyncRead(s, expected_size, callback);
    }

    //! parses the channel id from a header and passes it to an existing
    //! channel or creates a new channel
    void ReadFirstHeaderPartFrom(
        NetConnection& s, const Buffer& buffer) {
        struct StreamBlockHeader header;
        header.ParseHeader(buffer.ToString());

        ChannelPtr channel;
        sLOG << "reading head for channel" << header.channel_id;
        if (!HasChannel(header.channel_id)) {
            auto id = header.channel_id;

            //create buffer chain target if it does not exist
            if (!chains_.Contains(id))
                chains_.Allocate(id);
            auto targetChain = chains_.Chain(id);

            //build params for Channel ctor
            auto callback = std::bind(&ChannelMultiplexer::ExpectHeaderFrom, this, std::placeholders::_1);
            auto expected_peers = group_->Size();
            channel = std::make_shared<Channel>(dispatcher_, callback, id, expected_peers, targetChain);
            channels_.insert(std::make_pair(id, channel));
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
