/*******************************************************************************
 * c7a/data/channel_multiplexer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm  <Tobias.Sturm@student.kit.edu>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_MULTIPLEXER_HEADER
#define C7A_DATA_CHANNEL_MULTIPLEXER_HEADER

#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/net/group.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/channel_sink.hpp>
#include <c7a/data/block_writer.hpp>
#include <c7a/data/repository.hpp>

#include <memory>
#include <map>
#include <functional>

namespace c7a {
namespace data {

//! \ingroup data
//! \{

//! Multiplexes virtual Connections on Dispatcher
//!
//! A worker as a TCP conneciton to each other worker to exchange large amounts
//! of data. Since multiple exchanges can occur at the same time on this single
//! connection we use multiplexing. The slices are called Blocks and are
//! indicated by a \ref StreamBlockHeader. Multiple Blocks form a Stream on a
//! single TCP connection. The multiplexer multiplexes all streams on all
//! sockets.
//!
//! All sockets are polled for headers. As soon as the a header arrives it is
//! either attached to an existing channel or a new channel instance is
//! created.
//!
//! OpenChannel returns a set of emitters that can be used to emitt data to other workers.
class ChannelMultiplexer
{
public:
    using ChannelPtr = std::shared_ptr<Channel>;
    using ChannelId = Channel::ChannelId;

    static const size_t block_size = default_block_size;

    using BlockWriter = data::BlockWriter<block_size>;
    using BlockQueueReader = BlockReader<BlockQueueSource<block_size> >;

    ChannelMultiplexer(net::DispatcherThread& dispatcher)
        : dispatcher_(dispatcher), next_id_(0) { }

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

#if FIXUP_LATER
    //! Scatters the BufferChain to all workers
    //!
    //! elements from 0..offset[0] are sent to the first worker,
    //! elements from (offset[0] + 1)..offset[1] are sent to the second worker.
    //! elements from (offset[my_rank - 1] + 1)..(offset[my_rank]) are copied
    //! The offset values range from 0..Manager::GetNumElements()
    //! The number of given offsets must be equal to the net::Group::Size()
    //!/param source BufferChain containing the data to be scattered
    //!/param target id of the channel that will hold the resulting data. This
    //               channel must be created with CreateOrderPreservingChannel.
    //               Make sure *all* workers allocated this channel *before* any
    //               worker sends data
    //!/param offsets - as described above. offsets.size must be equal to group.size
    template <class T>
    void Scatter(const std::shared_ptr<BufferChain>& source,
                 const ChannelId target, std::vector<size_t> offsets) {
        //potential problem: channel was created by reception of packets,
        //which would cause the channel to be not order-preserving.
        assert(HasChannel(target));
        assert(offsets.size() == group_->Size());

        size_t sent_elements = 0;
        size_t elements_to_send = 0;
        Iterator<T> source_it(*source);
        for (size_t worker_id = 0; worker_id < offsets.size(); worker_id++) {
            elements_to_send = offsets[worker_id] - sent_elements;
            if (worker_id == group_->MyRank()) {
                auto channel = channels_[target];
                sLOG << "sending" << elements_to_send << "elements via channel" << target << "to self";
                MoveFromItToTarget<T>(
                    source_it,
                    [&channel, worker_id](const void* base, size_t length, size_t elements) {
                        // -tb removed for now.
                        //channel->ReceiveLocalData(base, length, elements, worker_id);
                    }, elements_to_send);
                channel->CloseLoopback();
            }
            else {
                SocketTarget sink(
                    &dispatcher_,
                    &(group_->connection(worker_id)),
                    target,
                    group_->MyRank());
                sLOG << "sending" << elements_to_send << "elements via channel" << target << "to worker" << worker_id;
                MoveFromItToTarget<T>(source_it, [&sink](const void* base, size_t length, size_t elements) { sink.Pipe(base, length, elements); }, elements_to_send);
                sink.Close();
            }
            sent_elements += elements_to_send;
        }
    }
#endif      // FIXUP_LATER

    //! Closes all client connections
    //!
    //! Requires new call to Connect() afterwards
    void Close() {
        group_->Close();
    }

private:
    static const bool debug = true;

    net::DispatcherThread& dispatcher_;

    //! Channels have an ID in block headers
    std::map<ChannelId, ChannelPtr> channels_;

    // Holds NetConnections for outgoing Channels
    net::Group* group_;

    //protects critical sections
    std::mutex mutex_;

    //! Next ID to generate
    ChannelId next_id_;

    //! Get channel with given id, if it does not exist, create it.
    ChannelPtr _GetOrCreateChannel(ChannelId id) {
        assert(group_ != nullptr);
        auto it = channels_.find(id);

        if (it != channels_.end())
            return it->second;

        // build params for Channel ctor
        ChannelPtr channel = std::make_shared<Channel>(id, group_->Size(), *group_, dispatcher_);
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
                s, header.expected_bytes,
                [this, header, channel](Connection& s, net::Buffer&& buffer) {
                    OnStreamData(s, header, channel, std::move(buffer));
                });
        }
    }

    void OnStreamData(
        Connection& s, const StreamBlockHeader& header, const ChannelPtr& channel,
        net::Buffer&& buffer) {
        sLOG << "got data on" << s << "in channel" << header.channel_id;

        using Block = data::Block<block_size>;
        using BlockPtr = std::shared_ptr<Block>;
        using VirtualBlock = data::VirtualBlock<block_size>;

        assert(header.expected_bytes == buffer.size());

        // TODO(tb): don't copy data!
        BlockPtr block = std::make_shared<Block>();
        std::copy(buffer.data(), buffer.data() + buffer.size(), block->begin());

        channel->OnStreamBlock(
            header.sender_rank,
            VirtualBlock(block,
                         header.expected_bytes, header.expected_elements, 0));

        AsyncReadStreamBlockHeader(s);
    }
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHANNEL_MULTIPLEXER_HEADER

/******************************************************************************/
