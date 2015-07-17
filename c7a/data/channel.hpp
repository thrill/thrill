/*******************************************************************************
 * c7a/data/channel.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm  <Tobias.Sturm@student.kit.edu>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_HEADER
#define C7A_DATA_CHANNEL_HEADER

#include <c7a/net/connection.hpp>
#include <c7a/data/stream_block_header.hpp>
#include <c7a/data/block_queue.hpp>
#include <c7a/data/channel_sink.hpp>
#include <c7a/net/group.hpp>

#include <vector>
#include <string>
#include <sstream>

namespace c7a {
namespace data {

//! \ingroup data
//! \{

using ChannelId = size_t;

//! A Channel is a collection of \ref Stream instances and bundles them to a
//! logical communication channel.
//!
//! There exists only one stream per socket at a time.
//! The channel keeps track of all active channels and counts the closed ones.
//!
//! As soon as the number of expected streams is reached, the channel is marked
//! as finished and no more data will arrive.
//!
//! Block headers are put into streams that poll more data from the socket.
//! As soon as the block is exhausted, the socket polling responsibility
//! is transfered back to the channel multiplexer.
template <size_t BlockSize = default_block_size>
class Channel
{
    using BlockQueue = data::BlockQueue<BlockSize>;
    using BlockQueueReader = BlockReader<BlockQueueSource<BlockSize> >;
    using BlockWriter = data::BlockWriterBase<BlockSize>;
    using VirtualBlock = data::VirtualBlock<BlockSize>;
    using ChannelSink = data::ChannelSink<BlockSize>;
    using File = data::FileBase<BlockSize>;

public:
    //! Creates a new channel instance
    Channel(const ChannelId& id, net::Group& group, net::DispatcherThread& dispatcher)
        : id_(id),
          queues_(group.Size()),
          group_(group),
          dispatcher_(dispatcher) {
        // construct ChannelSink array
        for (size_t i = 0; i != group_.Size(); ++i) {
            if (i == group_.MyRank()) {
                sinks_.emplace_back();
            }
            else {
                sinks_.emplace_back(
                    &dispatcher, &group_.connection(i), id, group_.MyRank());
            }
        }
    }

    void CloseLoopback() {
        OnCloseStream(group_.MyRank());
    }

    //! non-copyable: delete copy-constructor
    Channel(const Channel&) = delete;
    //! non-copyable: delete assignment operator
    Channel& operator = (const Channel&) = delete;

    //! move-constructor
    Channel(Channel&&) = default;

    //! Indicates whether all streams are finished
    bool Finished() const {
        return finished_streams_ == queues_.size();
    }

    const ChannelId & id() const {
        return id_;
    }

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<BlockWriter> OpenWriters() {
        std::vector<BlockWriter> result;

        for (size_t worker_id = 0; worker_id < group_.Size(); ++worker_id) {
            if (worker_id == group_.MyRank()) {
                result.emplace_back(&queues_[worker_id]);
            }
            else {
                result.emplace_back(&sinks_[worker_id]);
            }
        }

        assert(result.size() == group_.Size());
        return result;
    }

    //! Creates a BlockReader for each worker. The BlockReaders are attached to
    //! the BlockQueues in the Channel and wait for further Blocks to arrive or
    //! the Channel's remote close.
    std::vector<BlockQueueReader> OpenReaders() {
        std::vector<BlockQueueReader> result;

        for (size_t worker_id = 0; worker_id < group_.Size(); ++worker_id) {

            result.emplace_back(BlockQueueSource<BlockSize>(queues_[worker_id]));
        }

        assert(result.size() == group_.Size());
        return result;
    }

    //! Reads add data from this Channel (blocking)
    //! The resulting blocks in the file will be ordered by their sender ascending.
    //! Blocks from the same sender are ordered the way they were received/sent
    File ReadCompleteChannel() {
        FileBase<BlockSize> result;
        for (auto& q : queues_) {
            while (!q.empty() || !q.closed()) {
                auto vblock = q.Pop(); //this is blocking
                result.Append(q.Pop());
            }
        }
        return result;
    }

    void Close() {
        // close all sinks, this should emit sentinel to all other workers.
        for (size_t i = 0; i != sinks_.size(); ++i) {
            if (sinks_[i].closed()) continue;
            sinks_[i].Close();
        }

        // close self-loop queues
        if (!queues_[group_.MyRank()].closed())
            queues_[group_.MyRank()].Close();

        // wait for close packets to arrive (this is a busy waiting loop, try to
        // do it better -tb)
        for (size_t i = 0; i != queues_.size(); ++i) {
            while (!queues_[i].closed()) {
                LOG << "wait for close from worker" << i;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

protected:
    static const bool debug = false;

    ChannelId id_;
    size_t finished_streams_ = 0;

    //! ChannelSink objects are receivers of Blocks outbound for other workers.
    std::vector<ChannelSink> sinks_;

    //! BlockQueues to store incoming Blocks with no attached destination.
    std::vector<BlockQueue> queues_;

    net::Group& group_;
    net::DispatcherThread& dispatcher_;

    template <size_t block_size>
    friend class ChannelMultiplexer;

    //! called from ChannelMultiplexer when there is a new Block on a
    //! Stream.
    void OnStreamBlock(size_t from, VirtualBlock&& vb) {
        assert(from < queues_.size());

        if (debug) {
            sLOG << "channel" << id_ << "receive from" << from << ":"
                 << common::hexdump(vb.block->data(), vb.bytes_used);
        }

        queues_[from].Append(std::move(vb));
    }

    //! called from ChannelMultiplexer when a Stream closed notification was
    //! received.
    void OnCloseStream(size_t from) {
        assert(from < queues_.size());
        assert(!queues_[from].closed());
        queues_[from].Close();

        finished_streams_++;
        if (finished_streams_ == queues_.size()) {
            sLOG << "channel" << id_ << " is closed";
        }
        else {
            sLOG << "channel" << id_ << " is not closed yet "
                 << "(expect:" << queues_.size() << "actual:" << finished_streams_ << ")";
        }
    }

    // void ReceiveLocalData(const void* base, size_t len, size_t elements, size_t own_rank) {
    //     assert(finished_streams_ < queues_.size());
    //     LOG << "channel " << id_ << " receives local data @" << base << " (" << len << " bytes / " << elements << " elements)";
    //     //TODO(ts) this is a copy
    //     BinaryBufferBuilder bb(base, len, elements);
    //     buffer_sorter_.Append(own_rank, bb);
    // }
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHANNEL_HEADER

/******************************************************************************/
