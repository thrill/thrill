/*******************************************************************************
 * thrill/data/stream_sink.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_STREAM_SINK_HEADER
#define THRILL_DATA_STREAM_SINK_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/multiplexer_header.hpp>
#include <thrill/net/buffer.hpp>
#include <thrill/net/dispatcher_thread.hpp>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

// forward declarations
class Stream;

/*!
 * StreamSink is an BlockSink that sends data via a network socket to the
 * Stream object on a different worker.
 */
class StreamSink final : public BlockSink
{
public:
    using StreamId = size_t;

    //! Construct invalid StreamSink, needed for placeholders in sinks arrays
    //! where Blocks are directly sent to local workers.
    StreamSink(Stream& stream, BlockPool& block_pool, size_t local_worker_id)
        : BlockSink(block_pool, local_worker_id),
          stream_(stream), closed_(true) { }

    //! StreamSink sending out to network.
    StreamSink(Stream& stream, BlockPool& block_pool,
               net::Connection* connection,
               MagicByte magic, StreamId stream_id,
               size_t host_rank, size_t local_worker_id,
               size_t peer_rank, size_t peer_local_worker_id);

    StreamSink(StreamSink&&) = default;
    StreamSink& operator = (StreamSink&&) = default;

    //! Appends data to the StreamSink.  Data may be sent but may be delayed.
    void AppendBlock(const PinnedBlock& block) final;

    //! Closes the connection
    void Close() final;

    //! return close flag
    bool closed() const { return closed_; }

    //! boolean flag whether to check if AllocateByteBlock can fail in any
    //! subclass (if false: accelerate BlockWriter to not be able to cope with
    //! nullptr).
    static constexpr bool allocate_can_fail_ = false;

private:
    static constexpr bool debug = false;

    Stream& stream_;
    net::Connection* connection_ = nullptr;

    MagicByte magic_ = MagicByte::Invalid;
    StreamId id_ = size_t(-1);
    size_t host_rank_ = size_t(-1);
    using BlockSink::local_worker_id_;
    size_t peer_rank_ = size_t(-1);
    size_t peer_local_worker_id_ = size_t(-1);
    bool closed_ = false;

    size_t byte_counter_ = 0;
    size_t block_counter_ = 0;
    common::StatsTimerStart timespan_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_STREAM_SINK_HEADER

/******************************************************************************/
