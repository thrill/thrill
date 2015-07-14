/*******************************************************************************
 * c7a/net/channel.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_CHANNEL_HEADER
#define C7A_NET_CHANNEL_HEADER

#include <c7a/net/connection.hpp>
#include <c7a/net/stream.hpp>
#include <c7a/data/binary_buffer_builder.hpp>
#include <c7a/data/binary_buffer.hpp>
#include <c7a/data/buffer_chain.hpp>
#include <c7a/data/chain_id.hpp>

#include <vector>
#include <string>
#include <sstream>

namespace c7a {
namespace net {

//! \ingroup net
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
    using ChannelId = data::ChannelId;

    //! Creates a new channel instance
    Channel(const ChannelId& id, int expected_streams,
            std::shared_ptr<data::BufferChain> target)
        : id_(id),
          expected_streams_(expected_streams),
          finished_streams_(0),
          target_(target)
    { }

    void CloseLoopback() {
        OnCloseStream();
    }

    //! Indicates whether all streams are finished
    bool Finished() const {
        return finished_streams_ == expected_streams_;
    }

    const ChannelId & id() const {
        return id_;
    }

protected:
    static const bool debug = false;

    ChannelId id_;
    int expected_streams_;
    int finished_streams_;

    data::OrderedBufferChain buffer_sorter_;
    std::shared_ptr<data::BufferChain> target_;

    //! for calling protected methods to deliver blocks.
    friend class ChannelMultiplexer;

    //! called from ChannelMultiplexer when there is a new Block on a
    //! Stream. TODO(tb): pass sender id when needed.
    void OnStreamData(data::BinaryBufferBuilder& bb) {
        target_->Append(bb);
    }

    //! called from ChannelMultiplexer when a Stream closed notification was
    //! received.
    void OnCloseStream() {
        finished_streams_++;
        if (finished_streams_ == expected_streams_) {
            sLOG << "channel" << id_ << " is closed";
            target_->Close();
        }
        else {
            sLOG << "channel" << id_ << " is not closed yet (expect:" << expected_streams_ << "actual:" << finished_streams_ << ")";
        }
    }

    // void ReceiveLocalData(const void* base, size_t len, size_t elements, size_t own_rank) {
    //     assert(finished_streams_ < expected_streams_);
    //     LOG << "channel " << id_ << " receives local data @" << base << " (" << len << " bytes / " << elements << " elements)";
    //     //TODO(ts) this is a copy
    //     data::BinaryBufferBuilder bb(base, len, elements);
    //     buffer_sorter_.Append(own_rank, bb);
    // }
};

} // namespace net
} // namespace c7a

#endif // !C7A_NET_CHANNEL_HEADER

/******************************************************************************/
