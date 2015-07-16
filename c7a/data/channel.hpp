/*******************************************************************************
 * c7a/data/channel.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_HEADER
#define C7A_DATA_CHANNEL_HEADER

#include <c7a/net/connection.hpp>
#include <c7a/data/stream_block_header.hpp>
#include <c7a/data/block_queue.hpp>

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
    Channel(const ChannelId& id, int expected_streams)
        : id_(id),
          queues_(expected_streams)
    { }

    void CloseLoopback() {
        OnCloseStream(0);
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
