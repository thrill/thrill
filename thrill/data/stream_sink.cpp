/*******************************************************************************
 * thrill/data/stream_sink.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/stream_sink.hpp>

#include <thrill/data/cat_stream.hpp>
#include <thrill/data/mix_stream.hpp>
#include <thrill/data/multiplexer_header.hpp>
#include <thrill/data/stream.hpp>

#include <tlx/string/hexdump.hpp>

namespace thrill {
namespace data {

StreamSink::StreamSink()
    : BlockSink(nullptr, -1), closed_(true) { }

StreamSink::StreamSink(StreamDataPtr stream, BlockPool& block_pool,
                       net::Connection* connection,
                       MagicByte magic, StreamId stream_id,
                       size_t host_rank, size_t host_local_worker,
                       size_t peer_rank, size_t peer_local_worker)
    : BlockSink(block_pool, host_local_worker),
      stream_(std::move(stream)),
      connection_(connection),
      magic_(magic),
      id_(stream_id),
      host_rank_(host_rank),
      peer_rank_(peer_rank),
      peer_local_worker_(peer_local_worker) {
    logger()
        << "class" << "StreamSink"
        << "event" << "open"
        << "id" << id_
        << "peer_host" << peer_rank_
        << "src_worker" << my_worker_rank()
        << "tgt_worker" << peer_worker_rank();
}

StreamSink::StreamSink(StreamDataPtr stream, BlockPool& block_pool,
                       BlockQueue* block_queue,
                       StreamId stream_id,
                       size_t host_rank, size_t host_local_worker,
                       size_t peer_rank, size_t peer_local_worker)
    : BlockSink(block_pool, host_local_worker),
      stream_(std::move(stream)),
      block_queue_(block_queue),
      id_(stream_id),
      host_rank_(host_rank),
      peer_rank_(peer_rank),
      peer_local_worker_(peer_local_worker) {
    logger()
        << "class" << "StreamSink"
        << "event" << "open"
        << "id" << id_
        << "peer_host" << peer_rank_
        << "src_worker" << my_worker_rank()
        << "tgt_worker" << peer_worker_rank();
}

StreamSink::StreamSink(StreamDataPtr stream, BlockPool& block_pool,
                       MixStreamDataPtr target,
                       StreamId stream_id,
                       size_t host_rank, size_t host_local_worker,
                       size_t peer_rank, size_t peer_local_worker)
    : BlockSink(block_pool, host_local_worker),
      stream_(std::move(stream)),
      target_mix_stream_(target),
      id_(stream_id),
      host_rank_(host_rank),
      peer_rank_(peer_rank),
      peer_local_worker_(peer_local_worker) {
    logger()
        << "class" << "StreamSink"
        << "event" << "open"
        << "id" << id_
        << "peer_host" << peer_rank_
        << "src_worker" << my_worker_rank()
        << "tgt_worker" << peer_worker_rank();
}

size_t StreamSink::my_worker_rank() const {
    return host_rank_ * workers_per_host() + local_worker_id_;
}

size_t StreamSink::peer_worker_rank() const {
    return peer_rank_ * workers_per_host() + peer_local_worker_;
}

void StreamSink::AppendBlock(const Block& block, bool is_last_block) {
    return AppendPinnedBlock(block.PinWait(local_worker_id()), is_last_block);
}

void StreamSink::AppendBlock(Block&& block, bool is_last_block) {
    return AppendPinnedBlock(block.PinWait(local_worker_id()), is_last_block);
}

void StreamSink::AppendPinnedBlock(PinnedBlock&& block, bool is_last_block) {
    if (block.size() == 0) return;

    LOG << "StreamSink::AppendPinnedBlock()"
        << " block=" << block
        << " is_last_block=" << is_last_block;

    // StreamSink statistics
    item_counter_ += block.num_items();
    byte_counter_ += block.size();
    block_counter_++;

    if (block_queue_) {
        // StreamData statistics for internal transfer
        stream_->tx_int_items_ += block.num_items();
        stream_->tx_int_bytes_ += block.size();
        stream_->tx_int_blocks_++;

        return block_queue_->AppendPinnedBlock(std::move(block), is_last_block);
    }
    if (target_mix_stream_) {
        // StreamData statistics for internal transfer
        stream_->tx_int_items_ += block.num_items();
        stream_->tx_int_bytes_ += block.size();
        stream_->tx_int_blocks_++;

        return target_mix_stream_->OnStreamBlock(my_worker_rank(), std::move(block));
    }

    sem_.wait();

    sLOG0 << "sending block" << tlx::hexdump(block.ToString());

    StreamMultiplexerHeader header(magic_, block);
    header.stream_id = id_;
    header.sender_worker = my_worker_rank();
    header.receiver_local_worker = peer_local_worker_;
    header.is_last_block = is_last_block;

    net::BufferBuilder bb;
    header.Serialize(bb);

    net::Buffer buffer = bb.ToBuffer();
    assert(buffer.size() == MultiplexerHeader::total_size);

    // StreamData statistics for network transfer
    stream_->tx_net_items_ += block.num_items();
    stream_->tx_net_bytes_ += buffer.size() + block.size();
    stream_->tx_net_blocks_++;
    byte_counter_ += buffer.size();

    stream_->multiplexer_.dispatcher_.AsyncWrite(
        *connection_,
        // send out Buffer and Block, guaranteed to be successive
        std::move(buffer), std::move(block),
        [this](net::Connection&) { sem_.signal(); });

    if (is_last_block) {
        assert(!closed_);
        closed_ = true;

        // wait for the last Blocks to be transmitted (take away semaphore
        // tokens)
        for (size_t i = 0; i < num_queue_; ++i)
            sem_.wait();

        LOG << "StreamSink::AppendPinnedBlock()"
            << " sent 'piggy-backed close stream' id=" << id_
            << " from=" << my_worker_rank()
            << " (host=" << host_rank_ << ")"
            << " to=" << peer_worker_rank()
            << " (host=" << peer_rank_ << ")";

        Finalize();
    }
}

void StreamSink::Close() {
    if (closed_) return;
    closed_ = true;

    LOG << "StreamSink::Close() sending 'close stream' id=" << id_
        << " from=" << my_worker_rank()
        << " (host=" << host_rank_ << ")"
        << " to=" << peer_worker_rank()
        << " (host=" << peer_rank_ << ")";

    block_counter_++;

    if (block_queue_) {
        // StreamData statistics for internal transfer
        stream_->tx_int_blocks_++;
        return block_queue_->Close();
    }
    if (target_mix_stream_) {
        // StreamData statistics for internal transfer
        stream_->tx_int_blocks_++;
        return target_mix_stream_->OnCloseStream(my_worker_rank());
    }

    // wait for the last Blocks to be transmitted (take away semaphore tokens)
    for (size_t i = 0; i < num_queue_; ++i)
        sem_.wait();

    StreamMultiplexerHeader header;
    header.magic = magic_;
    header.stream_id = id_;
    header.sender_worker = (host_rank_ * workers_per_host()) + local_worker_id_;
    header.receiver_local_worker = peer_local_worker_;

    net::BufferBuilder bb;
    header.Serialize(bb);

    net::Buffer buffer = bb.ToBuffer();
    assert(buffer.size() == MultiplexerHeader::total_size);

    // StreamData statistics for network transfer
    stream_->tx_net_bytes_ += buffer.size();
    stream_->tx_net_blocks_++;
    byte_counter_ += buffer.size();

    stream_->multiplexer_.dispatcher_.AsyncWrite(
        *connection_, std::move(buffer));

    Finalize();
}

void StreamSink::Finalize() {
    logger()
        << "class" << "StreamSink"
        << "event" << "close"
        << "id" << id_
        << "peer_host" << peer_rank_
        << "src_worker" << my_worker_rank()
        << "tgt_worker" << peer_worker_rank()
        << "items" << item_counter_
        << "bytes" << byte_counter_
        << "blocks" << block_counter_
        << "timespan" << timespan_;
}

} // namespace data
} // namespace thrill

/******************************************************************************/
