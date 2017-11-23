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
#include <thrill/common/semaphore.hpp>
#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/stream_data.hpp>
#include <thrill/net/buffer.hpp>
#include <thrill/net/dispatcher_thread.hpp>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

// forward declarations
class StreamData;
using StreamDataPtr = tlx::CountingPtr<StreamData>;

/*!
 * StreamSink is an BlockSink that sends data via a network socket to the
 * StreamData object on a different worker.
 */
class StreamSink final : public BlockSink
{
    static constexpr bool debug = false;

public:
    using StreamId = size_t;

    //! Construct invalid StreamSink, needed for placeholders in sinks arrays
    //! where Blocks are directly sent to local workers.
    StreamSink();

    //! StreamSink sending out to network.
    StreamSink(StreamDataPtr stream, BlockPool& block_pool,
               net::Connection* connection,
               MagicByte magic, StreamId stream_id,
               size_t host_rank, size_t host_local_worker,
               size_t peer_rank, size_t peer_local_worker);

    //! StreamSink sending out local BlockQueue.
    StreamSink(StreamDataPtr stream, BlockPool& block_pool,
               BlockQueue* block_queue,
               StreamId stream_id,
               size_t host_rank, size_t host_local_worker,
               size_t peer_rank, size_t peer_local_worker);

    //! StreamSink sending out local MixBlockQueue.
    StreamSink(StreamDataPtr stream, BlockPool& block_pool,
               MixStreamDataPtr target,
               StreamId stream_id,
               size_t host_rank, size_t host_local_worker,
               size_t peer_rank, size_t peer_local_worker);

    StreamSink(StreamSink&&) = default;
    StreamSink& operator = (StreamSink&&) = default;

    //! Appends data to the StreamSink.  Data may be sent but may be delayed.
    void AppendBlock(const Block& block, bool is_last_block) final;

    //! Appends data to the StreamSink.  Data may be sent but may be delayed.
    void AppendBlock(Block&& block, bool is_last_block) final;

    //! Appends data to the StreamSink.  Data may be sent but may be delayed.
    void AppendPinnedBlock(PinnedBlock&& block, bool is_last_block) final;

    //! Closes the connection
    void Close() final;

    //! Finalize structure after sending the piggybacked or explicit close
    void Finalize();

    //! return close flag
    bool closed() const { return closed_; }

    //! is valid?
    bool IsValid() const { return stream_ != nullptr; }

    //! boolean flag whether to check if AllocateByteBlock can fail in any
    //! subclass (if false: accelerate BlockWriter to not be able to cope with
    //! nullptr).
    static constexpr bool allocate_can_fail_ = false;

    //! return local worker rank
    size_t my_worker_rank() const;

    //! return remote worker rank
    size_t peer_worker_rank() const;

private:
    StreamDataPtr stream_;

    //! \name StreamSink To Network
    //! \{

    net::Connection* connection_ = nullptr;
    MagicByte magic_ = MagicByte::Invalid;

    //! \}

    //! \name StreamSink To BlockQueue (CatStream Loopback)
    //! \{

    BlockQueue* block_queue_ = nullptr;

    //! \}

    //! \name StreamSink To MixBlockQueue (MixStream Loopback)
    //! \{

    //! destination mix stream
    MixStreamDataPtr target_mix_stream_;

    //! \}

    StreamId id_ = size_t(-1);
    size_t host_rank_ = size_t(-1);
    using BlockSink::local_worker_id_;
    size_t peer_rank_ = size_t(-1);
    size_t peer_local_worker_ = size_t(-1);
    bool closed_ = false;

    //! number of PinnedBlocks to queue in the network layer
    static constexpr size_t num_queue_ = 8;

    //! semaphore to stall the amount of PinnedBlocks passed to the network
    //! layer for transmission.
    common::Semaphore sem_ { num_queue_ };

    size_t item_counter_ = 0;
    size_t byte_counter_ = 0;
    size_t block_counter_ = 0;
    common::StatsTimerStart timespan_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_STREAM_SINK_HEADER

/******************************************************************************/
