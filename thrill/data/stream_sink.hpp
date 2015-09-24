/*******************************************************************************
 * thrill/data/stream_sink.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
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
    explicit StreamSink(BlockPool& block_pool)
        : BlockSink(block_pool), closed_(true) { }

    /*!
     * StreamSink sending out to network.
     *
     * \param dispatcher used for sending data via a socket
     * \param connection the socket (aka conneciton) used for the stream
     * \param stream_id the ID that identifies the stream
     * \param my_rank the ID that identifies this computing node globally
     * \param my_local_worker_id the id that identifies the worker locally
     * \param partners_local_worker_id the id that identifies the partner worker locally
     */
    StreamSink(BlockPool& block_pool,
               net::DispatcherThread* dispatcher,
               net::Connection* connection,
               MagicByte magic,
               StreamId stream_id, size_t my_rank, size_t my_local_worker_id, size_t partners_local_worker_id, StatsCounterPtr byte_counter, StatsCounterPtr block_counter, StatsTimerPtr tx_timespan)
        : BlockSink(block_pool),
          dispatcher_(dispatcher),
          connection_(connection),
          magic_(magic),
          id_(stream_id),
          my_rank_(my_rank),
          my_local_worker_id_(my_local_worker_id),
          partners_local_worker_id_(partners_local_worker_id),
          byte_counter_(byte_counter),
          block_counter_(block_counter),
          tx_timespan_(tx_timespan)
    { }

    StreamSink(StreamSink&&) = default;

    //! Appends data to the StreamSink.  Data may be sent but may be delayed.
    void AppendBlock(const Block& block) final {
        if (block.size() == 0) return;

        tx_timespan_->StartEventually();
        sLOG << "StreamSink::AppendBlock" << block;

        StreamBlockHeader header(magic_, block);
        header.stream_id = id_;
        header.sender_rank = my_rank_;
        header.sender_local_worker_id = my_local_worker_id_;
        header.receiver_local_worker_id = partners_local_worker_id_;

        if (debug) {
            sLOG << "sending block" << common::Hexdump(block.ToString());
        }

        net::BufferBuilder bb;
        header.Serialize(bb);

        net::Buffer buffer = bb.ToBuffer();
        assert(buffer.size() == BlockHeader::total_size);

        (*byte_counter_) += buffer.size();
        (*byte_counter_) += block.size();
        (*block_counter_)++;

        dispatcher_->AsyncWrite(
            *connection_,
            // send out Buffer and Block, guaranteed to be successive
            std::move(buffer), block);
    }

    //! Closes the connection
    void Close() final {
        assert(!closed_);
        closed_ = true;

        tx_timespan_->StartEventually();

        sLOG << "sending 'close stream' from my_rank" << my_rank_
             << "worker" << my_local_worker_id_
             << "to worker" << partners_local_worker_id_
             << "stream" << id_;

        StreamBlockHeader header;
        header.magic = magic_;
        header.stream_id = id_;
        header.sender_rank = my_rank_;
        header.sender_local_worker_id = my_local_worker_id_;
        header.receiver_local_worker_id = partners_local_worker_id_;

        net::BufferBuilder bb;
        header.Serialize(bb);

        net::Buffer buffer = bb.ToBuffer();
        assert(buffer.size() == BlockHeader::total_size);

        (*byte_counter_) += buffer.size();

        dispatcher_->AsyncWrite(*connection_, std::move(buffer));
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
    size_t my_rank_ = size_t(-1);
    size_t my_local_worker_id_ = size_t(-1);
    size_t partners_local_worker_id_ = size_t(-1);
    bool closed_ = false;

    StatsCounterPtr byte_counter_;
    StatsCounterPtr block_counter_;
    StatsTimerPtr tx_timespan_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_STREAM_SINK_HEADER

/******************************************************************************/
