/*******************************************************************************
 * c7a/data/multiplexer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_MULTIPLEXER_HEADER
#define C7A_DATA_MULTIPLEXER_HEADER

#include <c7a/common/atomic_movable.hpp>
#include <c7a/data/block_writer.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/repository.hpp>
#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/net/group.hpp>

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
class Multiplexer
{
public:
    explicit Multiplexer(size_t num_workers_per_node)
        : dispatcher_("multiplexer"),
          num_workers_per_node_(num_workers_per_node),
          channels_(num_workers_per_node){ }

    //! non-copyable: delete copy-constructor
    Multiplexer(const Multiplexer&) = delete;
    //! non-copyable: delete assignment operator
    Multiplexer& operator = (const Multiplexer&) = delete;
    //! default move constructor
    Multiplexer(Multiplexer&&) = default;

    //! Closes all client connections
    ~Multiplexer() {
        if (group_ != nullptr) {
            // close all still open Channels
            for (auto& ch : channels_.map())
                ch.second->Close();
        }

        // terminate dispatcher, this waits for unfinished AsyncWrites.
        dispatcher_.Terminate();

        if (group_ != nullptr)
            group_->Close();
    }

    void Connect(net::Group* group) {
        group_ = group;
        for (size_t id = 0; id < group_->num_connections(); id++) {
            if (id == group_->my_connection_id()) continue;
            AsyncReadStreamBlockHeader(group_->connection(id));
        }
    }

    //! Allocate the next channel
    size_t AllocateChannelId(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return channels_.AllocateId(local_worker_id);
    }

    //! Get channel with given id, if it does not exist, create it.
    ChannelPtr GetOrCreateChannel(size_t id, size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::move(_GetOrCreateChannel(id, local_worker_id));
    }

    //! Request next channel.
    ChannelPtr GetNewChannel(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::move(
            _GetOrCreateChannel(
                channels_.AllocateId(local_worker_id), local_worker_id));
    }

private:
    static const bool debug = false;

    //! dispatcher used for all communication by data::Multiplexer, the thread
    //! never leaves the data components!
    net::DispatcherThread dispatcher_;

    // Holds NetConnections for outgoing Channels
    net::Group* group_ = nullptr;

    //! Number of workers per node
    size_t num_workers_per_node_;

    //! protects critical sections
    common::MutexMovable mutex_;

    //! Channels have an ID in block headers. (worker id, channel id)
    Repository<Channel> channels_;

    ChannelPtr _GetOrCreateChannel(size_t id, size_t local_worker_id) {
        assert(group_ != nullptr);
        return std::move(
            channels_.GetOrCreate(
                id, local_worker_id,
                // initializers for Channels
                id, *group_, dispatcher_, local_worker_id, num_workers_per_node_));
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
        auto local_worker = header.receiver_local_worker_id;
        ChannelPtr channel = GetOrCreateChannel(id, local_worker);

        size_t sender_worker_rank = header.sender_rank * num_workers_per_node_ + header.sender_local_worker_id;
        if (header.IsStreamEnd()) {
            sLOG << "end of stream on" << s << "in channel" << id << "from worker" << sender_worker_rank;
            channel->OnCloseStream(sender_worker_rank);

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

        die_unless(header.size == buffer.size());

        // TODO(tb): don't copy data!
        ByteBlockPtr bytes = ByteBlock::Allocate(buffer.size());
        std::copy(buffer.data(), buffer.data() + buffer.size(), bytes->begin());

        size_t sender_worker_rank = header.sender_rank * num_workers_per_node_ + header.sender_local_worker_id;
        sLOG << "got block on" << s << "in channel" << header.channel_id << "from worker" << sender_worker_rank;
        channel->OnStreamBlock(
            sender_worker_rank,
            Block(bytes, 0, header.size, header.first_item, header.nitems));

        AsyncReadStreamBlockHeader(s);
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_MULTIPLEXER_HEADER

/******************************************************************************/