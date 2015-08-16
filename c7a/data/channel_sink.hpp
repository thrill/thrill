/*******************************************************************************
 * c7a/data/channel_sink.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_SINK_HEADER
#define C7A_DATA_CHANNEL_SINK_HEADER

#include <c7a/common/logger.hpp>
#include <c7a/common/stats_counter.hpp>
#include <c7a/common/stats_timer.hpp>
#include <c7a/data/block.hpp>
#include <c7a/data/block_sink.hpp>
#include <c7a/data/multiplexer_header.hpp>
#include <c7a/net/buffer.hpp>
#include <c7a/net/dispatcher_thread.hpp>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * ChannelSink is an BlockSink that sends data via a network socket to the
 * Channel object on a different worker.
 */
class ChannelSink : public BlockSink
{
public:
    using ChannelId = size_t;
    // use ptr because the default ctor cannot leave references unitialized
    using StatsCounterPtr = common::StatsCounter<size_t, common::g_enable_stats>*;
    using StatsTimerPtr = common::StatsTimer<common::g_enable_stats>*;

    //! Construct invalid ChannelSink, needed for placeholders in sinks arrays
    //! where Blocks are directly sent to local workers.
    ChannelSink(BlockPool& block_pool)
        : BlockSink(block_pool), closed_(true) { }

    /*! ChannelSink sending out to network.
     * \param dispatcher used for sending data via a socket
     * \param connection the socket (aka conneciton) used for the channel
     * \param channel_id the ID that identifies the channel
     * \param my_rank the ID that identifies this computing node globally
     * \param my_local_worker_id the id that identifies the worker locally
     * \param partners_local_worker_id the id that identifies the partner worker locally
     */
    ChannelSink(BlockPool& block_pool,
                net::DispatcherThread* dispatcher,
                net::Connection* connection,
                ChannelId channel_id, size_t my_rank, size_t my_local_worker_id, size_t partners_local_worker_id, StatsCounterPtr byte_counter, StatsCounterPtr block_counter, StatsTimerPtr tx_timespan)
        : BlockSink(block_pool),
          dispatcher_(dispatcher),
          connection_(connection),
          id_(channel_id),
          my_rank_(my_rank),
          my_local_worker_id_(my_local_worker_id),
          partners_local_worker_id_(partners_local_worker_id),
          byte_counter_(byte_counter),
          block_counter_(block_counter),
          tx_timespan_(tx_timespan)
    { }

    ChannelSink(ChannelSink&&) = default;

    //! Appends data to the ChannelSink.  Data may be sent but may be delayed.
    void AppendBlock(const Block& block) final {
        if (block.size() == 0) return;

        tx_timespan_->StartEventually();
        sLOG << "ChannelSink::AppendBlock" << block;

        ChannelBlockHeader header(block);
        header.channel_id = id_;
        header.sender_rank = my_rank_;
        header.sender_local_worker_id = my_local_worker_id_;
        header.receiver_local_worker_id = partners_local_worker_id_;

        if (debug) {
            sLOG << "sending block" << common::hexdump(block.ToString());
        }

        net::BufferBuilder bb;
        // bb.Put(MagicByte::CHANNEL_BLOCK);
        header.Serialize(bb);

        net::Buffer buffer = bb.ToBuffer();

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

        sLOG << "sending 'close channel' from my_rank" << my_rank_
             << "worker" << my_local_worker_id_
             << "to worker" << partners_local_worker_id_
             << "channel" << id_;

        ChannelBlockHeader header;
        header.channel_id = id_;
        header.sender_rank = my_rank_;
        header.sender_local_worker_id = my_local_worker_id_;
        header.receiver_local_worker_id = partners_local_worker_id_;

        net::BufferBuilder bb;
        // bb.Put(MagicByte::CHANNEL_BLOCK);
        header.Serialize(bb);

        net::Buffer buffer = bb.ToBuffer();

        (*byte_counter_) += buffer.size();

        dispatcher_->AsyncWrite(*connection_, std::move(buffer));
    }

    //! return close flag
    bool closed() const { return closed_; }

protected:
    static const bool debug = false;

    net::DispatcherThread* dispatcher_ = nullptr;
    net::Connection* connection_ = nullptr;

    size_t id_ = -1;
    size_t my_rank_ = -1;
    size_t my_local_worker_id_ = -1;
    size_t partners_local_worker_id_ = -1;
    bool closed_ = false;

    StatsCounterPtr byte_counter_;
    StatsCounterPtr block_counter_;
    StatsTimerPtr tx_timespan_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHANNEL_SINK_HEADER

/******************************************************************************/
