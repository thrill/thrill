/*******************************************************************************
 * c7a/data/channel.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm  <Tobias.Sturm@student.kit.edu>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_HEADER
#define C7A_DATA_CHANNEL_HEADER

#include <c7a/net/connection.hpp>
#include <c7a/data/stream_block_header.hpp>
#include <c7a/data/block_queue.hpp>
#include <c7a/data/dyn_block_writer.hpp>
#include <c7a/net/group.hpp>

#include <vector>
#include <string>
#include <sstream>

namespace c7a {
namespace data {

//! \ingroup data
//! \{

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
class Channel
{
public:

    using ChannelId = size_t;
    //! Creates a new channel instance
    Channel(const ChannelId& id, int expected_streams, size_t own_rank)
        : id_(id),
          queues_(expected_streams),
          own_rank_(own_rank)
    { }

    void CloseLoopback() {
        OnCloseStream(own_rank_);
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

    using BlockQueue = data::BlockQueue<default_block_size>;
    using VirtualBlock = data::VirtualBlock<default_block_size>;

protected:
    static const bool debug = false;

    ChannelId id_;
    size_t finished_streams_ = 0;

    //! BlockQueues to store incoming Blocks with no attached destination.
    std::vector<BlockQueue> queues_;

    //! Own rank (sender id)
    size_t own_rank_;

    //! for calling protected methods to deliver blocks.
    friend class ChannelMultiplexer;

    //! called from ChannelMultiplexer when there is a new Block on a
    //! Stream. TODO(tb): pass sender id when needed.
    void OnStreamBlock(size_t from, VirtualBlock&& vb) {
        assert(from < queues_.size());

        sLOG1 << "channel receive " << id_ << "from" << from << ":"
              << common::hexdump(vb.block->data(), vb.bytes_used);

        queues_[from].Append(std::move(vb));
    }

    //! called from ChannelMultiplexer when a Stream closed notification was
    //! received.
    void OnCloseStream(size_t from) {
        finished_streams_++;
        if (finished_streams_ == queues_.size()) {
            assert(from < queues_.size());

            sLOG << "channel" << id_ << " is closed";
            queues_[from].Close();
        }
        else {
            sLOG << "channel" << id_ << " is not closed yet (expect:" << queues_.size() << "actual:" << finished_streams_ << ")";
        }
    }

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    template <size_t BlockSize>
    std::vector<data::DynBlockWriter<BlockSize>> OpenWriters(std::shared_ptr<Channel> channel, net::Dispatcher& dispatcher, net::Group* group) {
        assert(group != nullptr);

        std::vector<data::DynBlockWriter<BlockSize>> result;

        for (size_t worker_id = 0; worker_id < group->Size(); ++worker_id) {
            if (worker_id == group->MyRank()) {
                result.emplace_back(
                    DynBlockSink<BlockSize>(&channel->queues_[worker_id]));
            }
            else {
                result.emplace_back(
                    DynBlockSink<BlockSize>(
                        ChannelSink<BlockSize>(dispatcher,
                                                &group->connection(worker_id),
                                                id,
                                                group->MyRank())));
            }
        }

        assert(result.size() == group->Size());
        return result;
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
