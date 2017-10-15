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

StreamSink::StreamSink(const StreamDataPtr& stream, BlockPool& block_pool,
                       size_t local_worker_id)
    : BlockSink(block_pool, local_worker_id),
      stream_(stream), closed_(true) { }

StreamSink::StreamSink(const StreamDataPtr& stream, BlockPool& block_pool,
                       net::Connection* connection,
                       MagicByte magic, StreamId stream_id,
                       size_t host_rank, size_t host_local_worker,
                       size_t peer_rank, size_t peer_local_worker)
    : BlockSink(block_pool, host_local_worker),
      stream_(stream),
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

void StreamSink::AppendPinnedBlock(const PinnedBlock& block, bool is_last_block) {
    if (block.size() == 0) return;

    sem_.wait();

    LOG << "StreamSink::AppendPinnedBlock()"
        << " block=" << block
        << " is_last_block=" << is_last_block;

    sLOG << "sending block" << tlx::hexdump(block.ToString());

    StreamMultiplexerHeader header(magic_, block);
    header.stream_id = id_;
    header.sender_worker = (host_rank_ * workers_per_host()) + local_worker_id_;
    header.receiver_local_worker = peer_local_worker_;
    header.is_last_block = is_last_block;

    net::BufferBuilder bb;
    header.Serialize(bb);

    net::Buffer buffer = bb.ToBuffer();
    assert(buffer.size() == MultiplexerHeader::total_size);

    item_counter_ += block.num_items();
    byte_counter_ += buffer.size() + block.size();
    ++block_counter_;

    stream_->multiplexer_.dispatcher_.AsyncWrite(
        *connection_,
        // send out Buffer and Block, guaranteed to be successive
        std::move(buffer), PinnedBlock(block),
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

void StreamSink::AppendPinnedBlock(PinnedBlock&& block, bool is_last_block) {
    return AppendPinnedBlock(block, is_last_block);
}

void StreamSink::Close() {
    if (closed_) return;
    closed_ = true;

    // wait for the last Blocks to be transmitted (take away semaphore tokens)
    for (size_t i = 0; i < num_queue_; ++i)
        sem_.wait();

    LOG << "StreamSink::Close() sending 'close stream' id=" << id_
        << " from=" << my_worker_rank()
        << " (host=" << host_rank_ << ")"
        << " to=" << peer_worker_rank()
        << " (host=" << peer_rank_ << ")";

    StreamMultiplexerHeader header;
    header.magic = magic_;
    header.stream_id = id_;
    header.sender_worker = (host_rank_ * workers_per_host()) + local_worker_id_;
    header.receiver_local_worker = peer_local_worker_;

    net::BufferBuilder bb;
    header.Serialize(bb);

    net::Buffer buffer = bb.ToBuffer();
    assert(buffer.size() == MultiplexerHeader::total_size);

    byte_counter_ += buffer.size();
    ++block_counter_;

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

    stream_->tx_net_items_ += item_counter_;
    stream_->tx_net_bytes_ += byte_counter_;
    stream_->tx_net_blocks_ += block_counter_;
}

} // namespace data
} // namespace thrill

/******************************************************************************/
