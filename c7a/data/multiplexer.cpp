/*******************************************************************************
 * c7a/data/multiplexer.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
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
        [this](Connection& s, net::Buffer&& buffer) {
            OnChannelBlockHeader(s, std::move(buffer));
        });
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

        ByteBlockPtr bytes = ByteBlock::Allocate(block_pool_, header.size);

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
        Block(bytes, 0, header.size, header.first_item, header.nitems));

    AsyncReadChannelBlockHeader(s);
}

} // namespace data
} // namespace c7a

/******************************************************************************/
