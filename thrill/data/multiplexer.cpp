/*******************************************************************************
 * thrill/data/multiplexer.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/data/channel.hpp>
#include <thrill/data/multiplexer.hpp>

#include <algorithm>

namespace thrill {
namespace data {

class ChannelSet;
using ChannelSetPtr = std::shared_ptr<ChannelSet>;

Multiplexer::~Multiplexer() {
    // close all still open Channels
    for (auto& ch : channel_sets_.map())
        ch.second->Close();

    // terminate dispatcher, this waits for unfinished AsyncWrites.
    dispatcher_.Terminate();

    group_.Close();
}

ChannelPtr Multiplexer::_GetOrCreateChannel(size_t id, size_t local_worker_id) {
    ChannelSetPtr set = channel_sets_.GetOrCreate(id, *this, id, num_workers_per_host_);
    return set->peer(local_worker_id);
}

/******************************************************************************/

//! expects the next ChannelBlockHeader from a socket and passes to
//! OnChannelBlockHeader
void Multiplexer::AsyncReadChannelBlockHeader(Connection& s) {
    dispatcher_.AsyncRead(
        s, sizeof(ChannelBlockHeader),
        net::AsyncReadCallback::from<
            Multiplexer, & Multiplexer::OnChannelBlockHeader>(this));
}

void Multiplexer::OnChannelBlockHeader(Connection& s, net::Buffer&& buffer) {

    // received invalid Buffer: the connection has closed?
    if (!buffer.IsValid()) return;

    ChannelBlockHeader header;
    net::BufferReader br(buffer);
    header.ParseHeader(br);

    // received channel id
    auto id = header.channel_id;
    auto local_worker = header.receiver_local_worker_id;
    ChannelPtr channel = GetOrCreateChannel(id, local_worker);

    size_t sender_worker_rank = header.sender_rank * num_workers_per_host_ + header.sender_local_worker_id;
    if (header.IsEnd()) {
        sLOG << "end of stream on" << s << "in channel" << id << "from worker" << sender_worker_rank;
        channel->OnCloseChannel(sender_worker_rank);

        AsyncReadChannelBlockHeader(s);
    }
    else {
        sLOG << "stream header from" << s << "on channel" << id
             << "from" << header.sender_rank;

        ByteBlockPtr bytes = ByteBlock::Allocate(header.size, block_pool_);

        dispatcher_.AsyncRead(
            s, bytes,
            [this, header, channel, bytes](Connection& s) {
                OnChannelBlock(s, header, channel, bytes);
            });
    }
}

void Multiplexer::OnChannelBlock(
    Connection& s, const ChannelBlockHeader& header, const ChannelPtr& channel,
    const ByteBlockPtr& bytes) {

    size_t sender_worker_rank = header.sender_rank * num_workers_per_host_ + header.sender_local_worker_id;
    sLOG << "got block on" << s << "in channel" << header.channel_id << "from worker" << sender_worker_rank;

    channel->OnChannelBlock(
        sender_worker_rank,
        Block(bytes, 0, header.size, header.first_item, header.num_items));

    AsyncReadChannelBlockHeader(s);
}


BlockQueue* Multiplexer::loopback(size_t channel_id, size_t from_worker_id, size_t to_worker_id) {
    return channel_sets_.GetOrDie(channel_id)->peer(to_worker_id)->loopback_queue(from_worker_id);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
