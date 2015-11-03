/*******************************************************************************
 * thrill/data/multiplexer.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/cat_stream.hpp>
#include <thrill/data/mix_stream.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/stream.hpp>

#include <algorithm>

namespace thrill {
namespace data {

Multiplexer::~Multiplexer() {
    // close all still open Streams
    for (auto& ch : stream_sets_.map())
        ch.second->Close();

    // terminate dispatcher, this waits for unfinished AsyncWrites.
    dispatcher_.Terminate();

    group_.Close();
}

CatStreamPtr
Multiplexer::_GetOrCreateCatStream(size_t id, size_t local_worker_id) {
    CatStreamSetPtr set =
        stream_sets_.GetOrCreate<CatStreamSet>(
            id, *this, id, num_workers_per_host_);
    return set->peer(local_worker_id);
}

MixStreamPtr
Multiplexer::_GetOrCreateMixStream(size_t id, size_t local_worker_id) {
    MixStreamSetPtr set =
        stream_sets_.GetOrCreate<MixStreamSet>(
            id, *this, id, num_workers_per_host_);
    return set->peer(local_worker_id);
}

/******************************************************************************/

//! expects the next StreamBlockHeader from a socket and passes to
//! OnStreamBlockHeader
void Multiplexer::AsyncReadBlockHeader(Connection& s) {
    dispatcher_.AsyncRead(
        s, BlockHeader::total_size,
        net::AsyncReadCallback::from<
            Multiplexer, & Multiplexer::OnBlockHeader>(this));
}

void Multiplexer::OnBlockHeader(Connection& s, net::Buffer&& buffer) {

    // received invalid Buffer: the connection has closed?
    if (!buffer.IsValid()) return;

    StreamBlockHeader header;
    net::BufferReader br(buffer);
    header.ParseHeader(br);

    // received stream id
    StreamId id = header.stream_id;
    size_t local_worker = header.receiver_local_worker_id;
    size_t sender_worker_rank =
        header.sender_rank * num_workers_per_host_ + header.sender_local_worker_id;

    if (header.magic == MagicByte::CatStreamBlock)
    {
        CatStreamPtr stream = GetOrCreateCatStream(id, local_worker);

        if (header.IsEnd()) {
            sLOG << "end of stream on" << s << "in CatStream" << id
                 << "from worker" << sender_worker_rank;

            stream->OnCloseStream(sender_worker_rank);

            AsyncReadBlockHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on CatStream" << id
                 << "from worker" << sender_worker_rank;

            ByteBlockPtr bytes = ByteBlock::Allocate(header.size, block_pool_);

            dispatcher_.AsyncRead(
                s, bytes,
                [this, header, stream, bytes](Connection& s) {
                    OnCatStreamBlock(s, header, stream, bytes);
                });
        }
    }
    else if (header.magic == MagicByte::MixStreamBlock)
    {
        MixStreamPtr stream = GetOrCreateMixStream(id, local_worker);

        if (header.IsEnd()) {
            sLOG << "end of stream on" << s << "in MixStream" << id
                 << "from worker" << sender_worker_rank;

            stream->OnCloseStream(sender_worker_rank);

            AsyncReadBlockHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on MixStream" << id
                 << "from worker" << sender_worker_rank;

            ByteBlockPtr bytes = ByteBlock::Allocate(header.size, block_pool_);

            dispatcher_.AsyncRead(
                s, bytes,
                [this, header, stream, bytes](Connection& s) {
                    OnMixStreamBlock(s, header, stream, bytes);
                });
        }
    }
    else {
        die("Invalid magic byte in BlockHeader");
    }
}

void Multiplexer::OnCatStreamBlock(
    Connection& s, const StreamBlockHeader& header,
    const CatStreamPtr& stream, const ByteBlockPtr& bytes) {

    size_t sender_worker_rank = header.sender_rank * num_workers_per_host_ + header.sender_local_worker_id;
    sLOG << "got block on" << s << "in CatStream" << header.stream_id << "from worker" << sender_worker_rank;

    stream->OnStreamBlock(
        sender_worker_rank,
        Block(bytes, 0, header.size, header.first_item, header.num_items));

    AsyncReadBlockHeader(s);
}

void Multiplexer::OnMixStreamBlock(
    Connection& s, const StreamBlockHeader& header,
    const MixStreamPtr& stream, const ByteBlockPtr& bytes) {

    size_t sender_worker_rank = header.sender_rank * num_workers_per_host_ + header.sender_local_worker_id;
    sLOG << "got block on" << s << "in MixStream" << header.stream_id << "from worker" << sender_worker_rank;

    stream->OnStreamBlock(
        sender_worker_rank,
        Block(bytes, 0, header.size, header.first_item, header.num_items));

    AsyncReadBlockHeader(s);
}

BlockQueue* Multiplexer::CatLoopback(
    size_t stream_id, size_t from_worker_id, size_t to_worker_id) {
    return stream_sets_.GetOrDie<CatStreamSet>(stream_id)
           ->peer(to_worker_id)->loopback_queue(from_worker_id);
}

MixBlockQueueSink* Multiplexer::MixLoopback(
    size_t stream_id, size_t from_worker_id, size_t to_worker_id) {
    return stream_sets_.GetOrDie<MixStreamSet>(stream_id)
           ->peer(to_worker_id)->loopback_queue(from_worker_id);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
