/*******************************************************************************
 * c7a/net/channel_multiplexer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_CHANNEL_MULTIPLEXER_HEADER
#define C7A_NET_CHANNEL_MULTIPLEXER_HEADER

#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/net/group.hpp>
#include <c7a/net/channel.hpp>
#include <c7a/data/emitter.hpp>
#include <c7a/data/buffer_chain_manager.hpp>
#include <c7a/data/socket_target.hpp>

#include <memory>
#include <map>
#include <functional>

namespace c7a {
namespace data {

struct BufferChain;

} // namespace data

namespace net {

//! \ingroup net
//! \{

typedef c7a::data::ChainId ChannelId;

//! Multiplexes virtual Connections on Dispatcher
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
//!
//! OpenChannel returns a set of emitters that can be used to emitt data to other workers.
class ChannelMultiplexer
{
public:
    using ChannelPtr = std::shared_ptr<Channel>;
    using ChannelId = data::ChannelId;

    ChannelMultiplexer(DispatcherThread& dispatcher)
        : dispatcher_(dispatcher), chains_(data::NETWORK) { }

    void Connect(Group* group) {
        group_ = group;
        for (size_t id = 0; id < group_->Size(); id++) {
            if (id == group_->MyRank()) continue;
            AsyncReadStreamBlockHeader(group_->connection(id));
        }
    }

    //! Indicates if a channel exists with the given id
    //! Channels exist if they have been allocated before
    bool HasChannel(ChannelId id) {
        assert(id.type == data::NETWORK);
        return channels_.find(id.identifier) != channels_.end();
    }

    //! Indicates if there is data for a certain channel
    //! Data exists as soon as either a channel has been allocated or data arrived
    //! on this worker with the given id
    bool HasDataOn(ChannelId id) {
        assert(id.type == data::NETWORK);
        return chains_.Contains(id);
    }

    //! Returns the buffer chain that contains the data for the channel with the given id
    std::shared_ptr<data::BufferChain> AccessData(ChannelId id) {
        assert(id.type == data::NETWORK);
        return chains_.Chain(id);
    }

    //! Allocate the next channel
    ChannelId AllocateNext() {
        return chains_.AllocateNext();
    }

    //! Get channel with given id, if it does not exist, create it.
    ChannelPtr GetOrCreateChannel(ChannelId id) {
        assert(id.type == data::NETWORK);

        std::lock_guard<std::mutex> lock(mutex_);
        auto it = channels_.find(id.identifier);

        if (it != channels_.end())
            return it->second;

        // create buffer chain target if it does not exist
        auto targetChain = chains_.GetOrAllocate(id);

        // build params for Channel ctor
        ChannelPtr channel = std::make_shared<Channel>(
            id, group_->Size(), targetChain);
        channels_.insert(std::make_pair(id.identifier, channel));
        return channel;
    }

    //! Creates emitters for each worker. Uses the given ChannelId
    //! Channels can be opened only once.
    //! Behaviour on multiple calls to OpenChannel is undefined.
    //! \param id the channel to use
    template <class T>
    std::vector<data::Emitter<T> > OpenChannel(const ChannelId& id) {
        assert(group_ != nullptr);
        assert(id.type == data::NETWORK);
        std::vector<data::Emitter<T> > result;

        //rest of method is critical section
        std::lock_guard<std::mutex> lock(mutex_);

        for (size_t worker_id = 0; worker_id < group_->Size(); worker_id++) {
            if (worker_id == group_->MyRank()) {
                auto target = std::make_shared<data::LoopbackTarget>(chains_.Chain(id), [=]() { sLOG << "loopback closes" << id; GetOrCreateChannel(id)->CloseLoopback(); });
                result.emplace_back(data::Emitter<T>(target));
            }
            else {
                auto target = std::make_shared<data::SocketTarget>(
                    &dispatcher_,
                    &(group_->connection(worker_id)),
                    id.identifier,
                    group_->MyRank());

                result.emplace_back(data::Emitter<T>(target));
            }
        }
        assert(result.size() == group_->Size());
        return result;
    }

    //! Scatters the BufferChain to all workers
    //!
    //! elements from 0..offset[0] are sent to the first worker,
    //! elements from (offset[0] + 1)..offset[1] are sent to the second worker.
    //! elements from (offset[my_rank - 1] + 1)..(offset[my_rank]) are copied
    //! The offset values range from 0..data::Manager::GetNumElements()
    //! The number of given offsets must be equal to the net::Group::Size()
    //!/param source BufferChain containing the data to be scattered
    //!/param target id of the channel that will hold the resulting data. This
    //               channel must be created with CreateOrderPreservingChannel.
    //               Make sure *all* workers allocated this channel *before* any
    //               worker sends data
    //!/param offsets - as described above. offsets.size must be equal to group.size
    template <class T>
    void Scatter(const std::shared_ptr<data::BufferChain>& source, const ChannelId target, std::vector<size_t> offsets) {
        //potential problem: channel was created by reception of packets,
        //which would cause the channel to be not order-preserving.
        assert(HasChannel(target));
        assert(offsets.size() == group_->Size());

        size_t sent_elements = 0;
        size_t elements_to_send = 0;
        data::Iterator<T> source_it(*source);
        for (size_t worker_id = 0; worker_id < offsets.size(); worker_id++) {
            elements_to_send = offsets[worker_id] - sent_elements;
            if (worker_id == group_->MyRank()) {
                auto channel = channels_[target.identifier];
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
                data::SocketTarget sink(
                    &dispatcher_,
                    &(group_->connection(worker_id)),
                    target.identifier,
                    group_->MyRank());
                sLOG << "sending" << elements_to_send << "elements via channel" << target << "to worker" << worker_id;
                MoveFromItToTarget<T>(source_it, [&sink](const void* base, size_t length, size_t elements) { sink.Pipe(base, length, elements); }, elements_to_send);
                sink.Close();
            }
            sent_elements += elements_to_send;
        }
    }

    //! Closes all client connections
    //!
    //! Requires new call to Connect() afterwards
    void Close() {
        group_->Close();
    }

private:
    static const bool debug = false;

    DispatcherThread& dispatcher_;

    //! Channels have an ID in block headers
    std::map<size_t, ChannelPtr> channels_;
    data::BufferChainManager chains_;

    // Holds NetConnections for outgoing Channels
    Group* group_;

    //protects critical sections
    std::mutex mutex_;

    template <typename T>
    void MoveFromItToTarget(data::Iterator<T>& source, std::function<void(const void*, size_t, size_t)> target, size_t num_elements) {
        while (num_elements > 0) {
            assert(source.HasNext());
            void* data;
            size_t length;
            size_t seeked_elements = source.Seek(num_elements, &data, &length);
            target(data, length, seeked_elements);
            data::BinaryBufferReader reader(data::BinaryBuffer(data, length));
            while (!reader.empty())
                sLOG << "sending" << reader.GetString();
            num_elements -= seeked_elements;
        }
    }

    /**************************************************************************/

    //! expects the next StreamBlockHeader from a socket and passes to
    //! OnStreamBlockHeader
    void AsyncReadStreamBlockHeader(Connection& s) {
        dispatcher_.AsyncRead(
            s, sizeof(StreamBlockHeader),
            [this](Connection& s, Buffer&& buffer) {
                OnStreamBlockHeader(s, std::move(buffer));
            });
    }

    void OnStreamBlockHeader(Connection& s, Buffer&& buffer) {

        StreamBlockHeader header;
        header.ParseHeader(buffer.ToString());

        // received channel id
        auto id = ChannelId(data::NETWORK, header.channel_id);
        ChannelPtr channel = GetOrCreateChannel(id);

        if (header.IsStreamEnd()) {
            sLOG << "end of stream on" << s << "in channel" << id;
            channel->OnCloseStream();

            AsyncReadStreamBlockHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on channel" << id
                 << "from" << header.sender_rank;

            dispatcher_.AsyncRead(
                s, header.expected_bytes,
                [this, header, channel](Connection& s, Buffer&& buffer) {
                    OnStreamData(s, header, channel, std::move(buffer));
                });
        }
    }

    void OnStreamData(
        Connection& s, const StreamBlockHeader& header, const ChannelPtr& channel,
        Buffer&& buffer) {
        sLOG << "got data on" << s << "in channel" << header.channel_id;

        data::BinaryBufferBuilder bb(buffer.data(), buffer.size());
        channel->OnStreamData(bb);

        AsyncReadStreamBlockHeader(s);
    }
};

} // namespace net
} // namespace c7a

#endif // !C7A_NET_CHANNEL_MULTIPLEXER_HEADER

/******************************************************************************/
