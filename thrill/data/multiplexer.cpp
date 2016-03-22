/*******************************************************************************
 * thrill/data/multiplexer.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/multiplexer.hpp>

#include <thrill/data/cat_stream.hpp>
#include <thrill/data/mix_stream.hpp>
#include <thrill/data/stream.hpp>
#include <thrill/mem/aligned_allocator.hpp>

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

CatStreamPtr Multiplexer::IntGetOrCreateCatStream(
    size_t id, size_t local_worker_id, size_t dia_id) {
    CatStreamSetPtr set =
        stream_sets_.GetOrCreate<CatStreamSet>(
            id, *this, id, workers_per_host_, dia_id);
    CatStreamPtr ptr = set->peer(local_worker_id);
    // update dia_id: the stream may have been created before the DIANode
    // associated with it.
    if (!ptr->dia_id_)
        ptr->set_dia_id(dia_id);
    return ptr;
}

MixStreamPtr Multiplexer::IntGetOrCreateMixStream(
    size_t id, size_t local_worker_id, size_t dia_id) {
    MixStreamSetPtr set =
        stream_sets_.GetOrCreate<MixStreamSet>(
            id, *this, id, workers_per_host_, dia_id);
    MixStreamPtr ptr = set->peer(local_worker_id);
    // update dia_id: the stream may have been created before the DIANode
    // associated with it.
    if (!ptr->dia_id_)
        ptr->set_dia_id(dia_id);
    return ptr;
}

common::JsonLogger& Multiplexer::logger() {
    return block_pool_.logger();
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
        header.sender_rank * workers_per_host_ + header.sender_local_worker_id;

    // round of allocation size to next power of two
    size_t alloc_size = header.size;
    if (alloc_size < THRILL_DEFAULT_ALIGN) alloc_size = THRILL_DEFAULT_ALIGN;
    alloc_size = common::RoundUpToPowerOfTwo(alloc_size);

    if (header.magic == MagicByte::CatStreamBlock)
    {
        CatStreamPtr stream = GetOrCreateCatStream(
            id, local_worker, /* dia_id (unknown at this time) */ 0);
        stream->rx_bytes_ += buffer.size();

        if (header.IsEnd()) {
            sLOG << "end of stream on" << s << "in CatStream" << id
                 << "from worker" << sender_worker_rank;

            stream->OnCloseStream(sender_worker_rank);

            AsyncReadBlockHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on CatStream" << id
                 << "from worker" << sender_worker_rank
                 << "for local_worker" << local_worker;

            PinnedByteBlockPtr bytes = block_pool_.AllocateByteBlock(
                alloc_size, local_worker);

            dispatcher_.AsyncRead(
                s, header.size, std::move(bytes),
                [this, header, stream](Connection& s, PinnedByteBlockPtr&& bytes) {
                    OnCatStreamBlock(s, header, stream, std::move(bytes));
                });
        }
    }
    else if (header.magic == MagicByte::MixStreamBlock)
    {
        MixStreamPtr stream = GetOrCreateMixStream(
            id, local_worker, /* dia_id (unknown at this time) */ 0);
        stream->rx_bytes_ += buffer.size();

        if (header.IsEnd()) {
            sLOG << "end of stream on" << s << "in MixStream" << id
                 << "from worker" << sender_worker_rank;

            stream->OnCloseStream(sender_worker_rank);

            AsyncReadBlockHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on MixStream" << id
                 << "from worker" << sender_worker_rank
                 << "for local_worker" << local_worker;

            PinnedByteBlockPtr bytes = block_pool_.AllocateByteBlock(
                alloc_size, local_worker);

            dispatcher_.AsyncRead(
                s, header.size, std::move(bytes),
                [this, header, stream](Connection& s, PinnedByteBlockPtr&& bytes) mutable {
                    OnMixStreamBlock(s, header, stream, std::move(bytes));
                });
        }
    }
    else {
        die("Invalid magic byte in BlockHeader");
    }
}

void Multiplexer::OnCatStreamBlock(
    Connection& s, const StreamBlockHeader& header,
    const CatStreamPtr& stream, PinnedByteBlockPtr&& bytes) {

    size_t sender_worker_rank =
        header.sender_rank * workers_per_host_ + header.sender_local_worker_id;

    sLOG << "Multiplexer::OnCatStreamBlock()"
         << "got block on" << s
         << "in CatStream" << header.stream_id
         << "from worker" << sender_worker_rank;

    stream->OnStreamBlock(
        sender_worker_rank,
        PinnedBlock(std::move(bytes), 0, header.size,
                    header.first_item, header.num_items));

    AsyncReadBlockHeader(s);
}

void Multiplexer::OnMixStreamBlock(
    Connection& s, const StreamBlockHeader& header,
    const MixStreamPtr& stream, PinnedByteBlockPtr&& bytes) {

    size_t sender_worker_rank =
        header.sender_rank * workers_per_host_ + header.sender_local_worker_id;

    sLOG << "Multiplexer::OnMixStreamBlock()"
         << "got block on" << s
         << "in MixStream" << header.stream_id
         << "from worker" << sender_worker_rank;

    stream->OnStreamBlock(
        sender_worker_rank,
        PinnedBlock(std::move(bytes), 0, header.size,
                    header.first_item, header.num_items));

    AsyncReadBlockHeader(s);
}

BlockQueue* Multiplexer::CatLoopback(
    size_t stream_id, size_t from_worker_id, size_t to_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return stream_sets_.GetOrDie<CatStreamSet>(stream_id)
           ->peer(to_worker_id)->loopback_queue(from_worker_id);
}

MixBlockQueueSink* Multiplexer::MixLoopback(
    size_t stream_id, size_t from_worker_id, size_t to_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return stream_sets_.GetOrDie<MixStreamSet>(stream_id)
           ->peer(to_worker_id)->loopback_queue(from_worker_id);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
