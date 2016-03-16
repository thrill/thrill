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

#include <thrill/data/stream.hpp>

namespace thrill {
namespace data {

StreamSink::StreamSink(Stream& stream,
                       BlockPool& block_pool,
                       net::Connection* connection,
                       MagicByte magic,
                       StreamId stream_id, size_t host_rank,
                       size_t local_worker_id,
                       size_t peer_rank,
                       size_t peer_local_worker_id)
    : BlockSink(block_pool, local_worker_id),
      stream_(stream),
      connection_(connection),
      magic_(magic),
      id_(stream_id),
      host_rank_(host_rank),
      peer_rank_(peer_rank),
      peer_local_worker_id_(peer_local_worker_id) {
    logger()
        << "class" << "StreamSink"
        << "event" << "open"
        << "stream" << id_
        << "peer_host" << peer_rank_
        << "src_worker" << (host_rank_ * workers_per_host()) + local_worker_id_
        << "tgt_worker" << (peer_rank_ * workers_per_host()) + peer_local_worker_id_;
}

void StreamSink::AppendBlock(const Block& block) {
    return AppendPinnedBlock(block.PinWait(local_worker_id()));
}

void StreamSink::AppendBlock(Block&& block) {
    return AppendPinnedBlock(block.PinWait(local_worker_id()));
}

void StreamSink::AppendPinnedBlock(const PinnedBlock& block) {
    if (block.size() == 0) return;

    sLOG << "StreamSink::AppendBlock" << block;

    StreamBlockHeader header(magic_, block);
    header.stream_id = id_;
    header.sender_rank = host_rank_;
    header.sender_local_worker_id = local_worker_id_;
    header.receiver_local_worker_id = peer_local_worker_id_;

    sLOG << "sending block" << common::Hexdump(block.ToString());

    net::BufferBuilder bb;
    header.Serialize(bb);

    net::Buffer buffer = bb.ToBuffer();
    assert(buffer.size() == BlockHeader::total_size);

    byte_counter_ += buffer.size() + block.size();
    ++block_counter_;

    stream_.multiplexer_.dispatcher_.AsyncWrite(
        *connection_,
        // send out Buffer and Block, guaranteed to be successive
        std::move(buffer), block);
}

void StreamSink::AppendPinnedBlock(PinnedBlock&& block) {
    return AppendPinnedBlock(block);
}

void StreamSink::Close() {
    assert(!closed_);
    closed_ = true;

    sLOG << "sending 'close stream' from host_rank" << host_rank_
         << "worker" << local_worker_id_
         << "to" << peer_rank_
         << "worker" << peer_local_worker_id_
         << "stream" << id_;

    StreamBlockHeader header;
    header.magic = magic_;
    header.stream_id = id_;
    header.sender_rank = host_rank_;
    header.sender_local_worker_id = local_worker_id_;
    header.receiver_local_worker_id = peer_local_worker_id_;

    net::BufferBuilder bb;
    header.Serialize(bb);

    net::Buffer buffer = bb.ToBuffer();
    assert(buffer.size() == BlockHeader::total_size);

    byte_counter_ += buffer.size();
    ++block_counter_;

    stream_.multiplexer_.dispatcher_.AsyncWrite(
        *connection_, std::move(buffer));

    logger()
        << "class" << "StreamSink"
        << "event" << "close"
        << "stream" << id_
        << "peer_host" << peer_rank_
        << "src_worker" << (host_rank_ * workers_per_host()) + local_worker_id_
        << "tgt_worker" << (peer_rank_ * workers_per_host()) + peer_local_worker_id_
        << "bytes" << byte_counter_
        << "blocks" << block_counter_
        << "timespan" << timespan_;

    stream_.outgoing_bytes_ += byte_counter_;
    stream_.outgoing_blocks_ += block_counter_;
}

} // namespace data
} // namespace thrill

/******************************************************************************/
