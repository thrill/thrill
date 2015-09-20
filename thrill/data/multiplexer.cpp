/*******************************************************************************
 * thrill/data/multiplexer.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/data/cat_channel.hpp>
#include <thrill/data/channel.hpp>
#include <thrill/data/mix_channel.hpp>
#include <thrill/data/multiplexer.hpp>

#include <algorithm>

namespace thrill {
namespace data {

Multiplexer::~Multiplexer() {
    // close all still open Channels
    for (auto& ch : channel_sets_.map())
        ch.second->Close();

    // terminate dispatcher, this waits for unfinished AsyncWrites.
    dispatcher_.Terminate();

    group_.Close();
}

CatChannelPtr
Multiplexer::_GetOrCreateCatChannel(size_t id, size_t local_worker_id) {
    CatChannelSetPtr set =
        channel_sets_.GetOrCreate<CatChannelSet>(
            id, *this, id, num_workers_per_host_);
    return set->peer(local_worker_id);
}

MixChannelPtr
Multiplexer::_GetOrCreateMixChannel(size_t id, size_t local_worker_id) {
    MixChannelSetPtr set =
        channel_sets_.GetOrCreate<MixChannelSet>(
            id, *this, id, num_workers_per_host_);
    return set->peer(local_worker_id);
}

/******************************************************************************/

//! expects the next ChannelBlockHeader from a socket and passes to
//! OnChannelBlockHeader
void Multiplexer::AsyncReadBlockHeader(Connection& s) {
    dispatcher_.AsyncRead(
        s, BlockHeader::total_size,
        net::AsyncReadCallback::from<
            Multiplexer, & Multiplexer::OnBlockHeader>(this));
}

void Multiplexer::OnBlockHeader(Connection& s, net::Buffer&& buffer) {

    // received invalid Buffer: the connection has closed?
    if (!buffer.IsValid()) return;

    ChannelBlockHeader header;
    net::BufferReader br(buffer);
    header.ParseHeader(br);

    // received channel id
    ChannelId id = header.channel_id;
    size_t local_worker = header.receiver_local_worker_id;
    size_t sender_worker_rank =
        header.sender_rank * num_workers_per_host_ + header.sender_local_worker_id;

    if (header.magic == MagicByte::CAT_CHANNEL_BLOCK)
    {
        CatChannelPtr channel = GetOrCreateCatChannel(id, local_worker);

        if (header.IsEnd()) {
            sLOG << "end of stream on" << s << "in CatChannel" << id
                 << "from worker" << sender_worker_rank;

            channel->OnCloseChannel(sender_worker_rank);

            AsyncReadBlockHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on CatChannel" << id
                 << "from worker" << sender_worker_rank;

            ByteBlockPtr bytes = ByteBlock::Allocate(header.size, block_pool_);

            dispatcher_.AsyncRead(
                s, bytes,
                [this, header, channel, bytes](Connection& s) {
                    OnCatChannelBlock(s, header, channel, bytes);
                });
        }
    }
    else if (header.magic == MagicByte::MIX_CHANNEL_BLOCK)
    {
        MixChannelPtr channel = GetOrCreateMixChannel(id, local_worker);

        if (header.IsEnd()) {
            sLOG << "end of stream on" << s << "in mix channel" << id
                 << "from worker" << sender_worker_rank;

            channel->OnCloseChannel(sender_worker_rank);

            AsyncReadBlockHeader(s);
        }
        else {
            sLOG << "stream header from" << s << "on mix channel" << id
                 << "from worker" << sender_worker_rank;

            ByteBlockPtr bytes = ByteBlock::Allocate(header.size, block_pool_);

            dispatcher_.AsyncRead(
                s, bytes,
                [this, header, channel, bytes](Connection& s) {
                    OnMixChannelBlock(s, header, channel, bytes);
                });
        }
    }
    else {
        die("Invalid magic byte in BlockHeader");
    }
}

void Multiplexer::OnCatChannelBlock(
    Connection& s, const ChannelBlockHeader& header,
    const CatChannelPtr& channel, const ByteBlockPtr& bytes) {

    size_t sender_worker_rank = header.sender_rank * num_workers_per_host_ + header.sender_local_worker_id;
    sLOG << "got block on" << s << "in CatChannel" << header.channel_id << "from worker" << sender_worker_rank;

    channel->OnChannelBlock(
        sender_worker_rank,
        Block(bytes, 0, header.size, header.first_item, header.num_items));

    AsyncReadBlockHeader(s);
}

void Multiplexer::OnMixChannelBlock(
    Connection& s, const ChannelBlockHeader& header,
    const MixChannelPtr& channel, const ByteBlockPtr& bytes) {

    size_t sender_worker_rank = header.sender_rank * num_workers_per_host_ + header.sender_local_worker_id;
    sLOG << "got block on" << s << "in mix channel" << header.channel_id << "from worker" << sender_worker_rank;

    channel->OnChannelBlock(
        sender_worker_rank,
        Block(bytes, 0, header.size, header.first_item, header.num_items));

    AsyncReadBlockHeader(s);
}

BlockQueue* Multiplexer::CatLoopback(
    size_t channel_id, size_t from_worker_id, size_t to_worker_id) {
    return channel_sets_.GetOrDie<CatChannelSet>(channel_id)
           ->peer(to_worker_id)->loopback_queue(from_worker_id);
}

MixBlockQueueSink* Multiplexer::MixLoopback(
    size_t channel_id, size_t from_worker_id, size_t to_worker_id) {
    return channel_sets_.GetOrDie<MixChannelSet>(channel_id)
           ->peer(to_worker_id)->loopback_queue(from_worker_id);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
