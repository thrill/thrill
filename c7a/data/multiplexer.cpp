/*******************************************************************************
 * c7a/data/multiplexer.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/channel.hpp>
#include <c7a/data/multiplexer.hpp>

#include <algorithm>

namespace c7a {
namespace data {

Multiplexer::~Multiplexer() {
    // close all still open Channels
    for (auto& ch : channels_.map())
        ch.second->Close();

    // terminate dispatcher, this waits for unfinished AsyncWrites.
    dispatcher_.Terminate();

    group_.Close();
}

ChannelPtr Multiplexer::_GetOrCreateChannel(size_t id, size_t local_worker_id) {
    return std::move(
        channels_.GetOrCreate(
            id, local_worker_id,
            // initializers for Channels
            *this, id, local_worker_id));
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

    net::BufferReader br(buffer);
    on_channel.header.ParseHeader(br);

    // received channel id
    auto id = on_channel.header.channel_id;
    auto local_worker = on_channel.header.receiver_local_worker_id;
    on_channel.channel = GetOrCreateChannel(id, local_worker);

    size_t sender_worker_rank =
        on_channel.header.sender_rank * num_workers_per_host_ +
        on_channel.header.sender_local_worker_id;

    if (on_channel.header.IsEnd()) {
        sLOG << "end of stream on" << s << "in channel" << id << "from worker" << sender_worker_rank;
        on_channel.channel->OnCloseChannel(sender_worker_rank);

        AsyncReadChannelBlockHeader(s);
    }
    else {
        sLOG << "stream header from" << s << "on channel" << id
             << "from" << on_channel.header.sender_rank;

        on_channel.bytes = ByteBlock::Allocate(on_channel.header.size, block_pool_);

        dispatcher_.AsyncRead(
            s, on_channel.bytes,
            net::AsyncReadByteBlockCallback::from<
                Multiplexer, & Multiplexer::OnChannelBlock>(this));
    }
}

void Multiplexer::OnChannelBlock(Connection& s) {

    size_t sender_worker_rank =
        on_channel.header.sender_rank * num_workers_per_host_ +
        on_channel.header.sender_local_worker_id;

    sLOG << "got block on" << s << "in channel"
         << on_channel.header.channel_id << "from worker" << sender_worker_rank;

    on_channel.channel->OnChannelBlock(
        sender_worker_rank,
        Block(on_channel.bytes, 0, on_channel.header.size,
              on_channel.header.first_item, on_channel.header.nitems));

    AsyncReadChannelBlockHeader(s);
}

} // namespace data
} // namespace c7a

/******************************************************************************/
