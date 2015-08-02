/*******************************************************************************
 * c7a/data/channel_multiplexer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_MULTIPLEXER_HEADER
#define C7A_DATA_CHANNEL_MULTIPLEXER_HEADER

#include <c7a/data/block_writer.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/net/group.hpp>
#include <c7a/common/atomic_movable.hpp>

#include <algorithm>
#include <map>
#include <memory>
#include <vector>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * Multiplexes virtual Connections on Dispatcher.
 *
 * A worker as a TCP conneciton to each other worker to exchange large amounts
 * of data. Since multiple exchanges can occur at the same time on this single
 * connection we use multiplexing. The slices are called Blocks and are
 * indicated by a \ref StreamBlockHeader. Multiple Blocks form a Stream on a
 * single TCP connection. The multiplexer multiplexes all streams on all
 * sockets.
 *
 * All sockets are polled for headers. As soon as the a header arrives it is
 * either attached to an existing channel or a new channel instance is
 * created.
 */
class ChannelMultiplexer
{
public:
    explicit ChannelMultiplexer(net::DispatcherThread& dispatcher)
        : dispatcher_(dispatcher), next_id_(0) { }

    //! non-copyable: delete copy-constructor
    ChannelMultiplexer(const ChannelMultiplexer&) = delete;
    //! non-copyable: delete assignment operator
    ChannelMultiplexer& operator = (const ChannelMultiplexer&) = delete;
    //! default move constructor
    ChannelMultiplexer(ChannelMultiplexer&&) = default;

    //! Closes all client connections
    ~ChannelMultiplexer() {
        if (group_ != nullptr) {
            // close all still open Channels
            for (auto& ch : channels_)
                ch.second->Close();
        }

        // terminate dispatcher, this waits for unfinished AsyncWrites.
        dispatcher_.Terminate();

        if (group_ != nullptr)
            group_->Close();
    }

    void Connect(net::Group* group) {
        group_ = group;
        for (size_t id = 0; id < group_->Size(); id++) {
            if (id == group_->MyRank()) continue;
            AsyncReadStreamBlockHeader(group_->connection(id));
        }
    }

    //! Indicates if a channel exists with the given id
    //! Channels exist if they have been allocated before
    bool HasChannel(ChannelId id) {
        return channels_.find(id) != channels_.end();
    }

    //TODO Method to access channel via queue -> requires vec<Queue> or MultiQueue
    //TODO Method to access channel via callbacks

    //! Allocate the next channel
    ChannelId AllocateNext() {
        return next_id_++;
    }

    //! Get channel with given id, if it does not exist, create it.
    ChannelPtr GetOrCreateChannel(ChannelId id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return _GetOrCreateChannel(id);
    }

private:
    static const bool debug = false;

    net::DispatcherThread& dispatcher_;

    //! Channels have an ID in block headers
    std::map<ChannelId, ChannelPtr> channels_;

    // Holds NetConnections for outgoing Channels
    net::Group* group_ = nullptr;

    //protects critical sections
    common::MutexMovable mutex_;

    //! Next ID to generate
    ChannelId next_id_;

    //! Get channel with given id, if it does not exist, create it.
    ChannelPtr _GetOrCreateChannel(ChannelId id) {
        assert(group_ != nullptr);
        auto it = channels_.find(id);

        if (it != channels_.end())
            return it->second;

        // build params for Channel ctor
        ChannelPtr channel = std::make_shared<Channel>(id, *group_, dispatcher_);
        channels_.insert(std::make_pair(id, channel));
        return channel;
    }

    /**************************************************************************/

    using Connection = net::Connection;

    //! expects the next StreamBlockHeader from a socket and passes to
    //! OnStreamBlockHeader
    void AsyncReadStreamBlockHeader(Connection& s) {
        dispatcher_.AsyncRead(
            s, sizeof(StreamBlockHeader),
            [this](Connection& s, net::Buffer&& buffer) {
                OnStreamBlockHeader(s, std::move(buffer));
            });
    }

    void OnStreamBlockHeader(Connection& s, net::Buffer&& buffer) {

        // received invalid Buffer: the connection has closed?
        if (!buffer.IsValid()) return;

        StreamBlockHeader header;
        header.ParseHeader(buffer);

        // received channel id
        auto id = header.channel_id;
        ChannelPtr channel = GetOrCreateChannel(id);

        if (header.IsStreamEnd()) {
            sLOG << "end of stream on" << s << "in channel" << id;
            channel->OnCloseStream(header.sender_rank);

            AsyncReadStreamBlockHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on channel" << id
                 << "from" << header.sender_rank;

            dispatcher_.AsyncRead(
                s, header.size,
                [this, header, channel](Connection& s, net::Buffer&& buffer) {
                    OnStreamBlock(s, header, channel, std::move(buffer));
                });
        }
    }

    void OnStreamBlock(
        Connection& s, const StreamBlockHeader& header, const ChannelPtr& channel,
        net::Buffer&& buffer) {

        sLOG << "got block on" << s << "in channel" << header.channel_id;

        die_unless(header.size == buffer.size());

        // TODO(tb): don't copy data!
        BlockPtr block = Block::Allocate(buffer.size());
        std::copy(buffer.data(), buffer.data() + buffer.size(), block->begin());

        channel->OnStreamBlock(
            header.sender_rank,
            VirtualBlock(block, 0, header.size,
                         header.first_item, header.nitems));

        AsyncReadStreamBlockHeader(s);
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHANNEL_MULTIPLEXER_HEADER

/******************************************************************************/
