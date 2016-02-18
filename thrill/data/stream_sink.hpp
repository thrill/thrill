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

/*!
 * StreamSink is an BlockSink that sends data via a network socket to the
 * Stream object on a different worker.
 */
class StreamSink final : public BlockSink
{
public:
    using StreamId = size_t;
    // use ptr because the default ctor cannot leave references unitialized
    using StatsCounterPtr = common::StatsCounter<size_t, common::g_enable_stats>*;
    using StatsTimerPtr = common::StatsTimer<common::g_enable_stats>*;

    //! Construct invalid StreamSink, needed for placeholders in sinks arrays
    //! where Blocks are directly sent to local workers.
    explicit StreamSink(BlockPool& block_pool, size_t local_worker_id)
        : BlockSink(block_pool, local_worker_id), closed_(true) { }

    /*!
     * StreamSink sending out to network.
     */
    StreamSink(BlockPool& block_pool,
               net::DispatcherThread* dispatcher,
               net::Connection* connection,
               MagicByte magic,
               StreamId stream_id, size_t host_rank,
               size_t my_local_worker_id,
               size_t peer_rank,
               size_t peer_local_worker_id)
        : BlockSink(block_pool, my_local_worker_id),
          dispatcher_(dispatcher),
          connection_(connection),
          magic_(magic),
          id_(stream_id),
          host_rank_(host_rank),
          my_local_worker_id_(my_local_worker_id),
          peer_rank_(peer_rank),
          peer_local_worker_id_(peer_local_worker_id) {
        logger()
            << "class" << "StreamSink"
            << "event" << "open"
            << "stream" << id_
            << "src_worker" << (host_rank_ * workers_per_host()) + my_local_worker_id_
            << "peer_host" << peer_rank_
            << "tgt_worker" << (peer_rank_ * workers_per_host()) + peer_local_worker_id_;
    }

    StreamSink(StreamSink&&) = default;

    //! Appends data to the StreamSink.  Data may be sent but may be delayed.
    void AppendBlock(const PinnedBlock& block) final {
        if (block.size() == 0) return;

        sLOG << "StreamSink::AppendBlock" << block;

        StreamBlockHeader header(magic_, block);
        header.stream_id = id_;
        header.sender_rank = host_rank_;
        header.sender_local_worker_id = my_local_worker_id_;
        header.receiver_local_worker_id = peer_local_worker_id_;

        if (debug) {
            sLOG << "sending block" << common::Hexdump(block.ToString());
        }

        net::BufferBuilder bb;
        header.Serialize(bb);

        net::Buffer buffer = bb.ToBuffer();
        assert(buffer.size() == BlockHeader::total_size);

        byte_counter_ += buffer.size() + block.size();
        ++block_counter_;

        dispatcher_->AsyncWrite(
            *connection_,
            // send out Buffer and Block, guaranteed to be successive
            std::move(buffer), block);
    }

    //! Closes the connection
    void Close() final {
        assert(!closed_);
        closed_ = true;

        sLOG << "sending 'close stream' from host_rank" << host_rank_
             << "worker" << my_local_worker_id_
             << "to" << peer_rank_
             << "worker" << peer_local_worker_id_
             << "stream" << id_;

        StreamBlockHeader header;
        header.magic = magic_;
        header.stream_id = id_;
        header.sender_rank = host_rank_;
        header.sender_local_worker_id = my_local_worker_id_;
        header.receiver_local_worker_id = peer_local_worker_id_;

        net::BufferBuilder bb;
        header.Serialize(bb);

        net::Buffer buffer = bb.ToBuffer();
        assert(buffer.size() == BlockHeader::total_size);

        byte_counter_ += buffer.size();
        ++block_counter_;

        dispatcher_->AsyncWrite(*connection_, std::move(buffer));

        logger()
            << "class" << "StreamSink"
            << "event" << "close"
            << "stream" << id_
            << "src_worker" << (host_rank_ * workers_per_host()) + my_local_worker_id_
            << "peer_host" << peer_rank_
            << "tgt_worker" << (peer_rank_ * workers_per_host()) + peer_local_worker_id_
            << "bytes" << byte_counter_
            << "blocks" << block_counter_
            << "timespan" << tx_timespan_;
    }

    //! return close flag
    bool closed() const { return closed_; }

    //! boolean flag whether to check if AllocateByteBlock can fail in any
    //! subclass (if false: accelerate BlockWriter to not be able to cope with
    //! nullptr).
    enum { allocate_can_fail_ = false };

private:
    static const bool debug = false;

    net::DispatcherThread* dispatcher_ = nullptr;
    net::Connection* connection_ = nullptr;

    MagicByte magic_ = MagicByte::Invalid;
    size_t id_ = size_t(-1);
    size_t host_rank_ = size_t(-1);
    size_t my_local_worker_id_ = size_t(-1);
    size_t peer_rank_ = size_t(-1);
    size_t peer_local_worker_id_ = size_t(-1);
    bool closed_ = false;

    size_t byte_counter_ = 0;
    size_t block_counter_ = 0;
    common::StatsTimer<true> tx_timespan_ { true };
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_STREAM_SINK_HEADER

/******************************************************************************/
