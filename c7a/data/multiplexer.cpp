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

namespace c7a {
namespace data {

Multiplexer::~Multiplexer() {
    if (group_ != nullptr) {
        // close all still open Channels
        for (auto& ch : channels_.map())
            ch.second->Close();
    }

    // terminate dispatcher, this waits for unfinished AsyncWrites.
    dispatcher_.Terminate();

    if (group_ != nullptr)
        group_->Close();
}

ChannelPtr Multiplexer::_GetOrCreateChannel(size_t id, size_t local_worker_id) {
    assert(group_ != nullptr);
    return std::move(
        channels_.GetOrCreate(
            id, local_worker_id,
            // initializers for Channels
            id, *group_, dispatcher_, local_worker_id, num_workers_per_node_));
}

/******************************************************************************/

//! expects the next StreamBlockHeader from a socket and passes to
//! OnStreamBlockHeader
void Multiplexer::AsyncReadStreamBlockHeader(Connection& s) {
    dispatcher_.AsyncRead(
        s, sizeof(StreamBlockHeader),
        [this](Connection& s, net::Buffer&& buffer) {
            OnStreamBlockHeader(s, std::move(buffer));
        });
}

void Multiplexer::OnStreamBlockHeader(Connection& s, net::Buffer&& buffer) {

    // received invalid Buffer: the connection has closed?
    if (!buffer.IsValid()) return;

    StreamBlockHeader header;
    header.ParseHeader(buffer);

    // received channel id
    auto id = header.channel_id;
    auto local_worker = header.receiver_local_worker_id;
    ChannelPtr channel = GetOrCreateChannel(id, local_worker);

    size_t sender_worker_rank = header.sender_rank * num_workers_per_node_ + header.sender_local_worker_id;
    if (header.IsStreamEnd()) {
        sLOG << "end of stream on" << s << "in channel" << id << "from worker" << sender_worker_rank;
        channel->OnCloseStream(sender_worker_rank);

        AsyncReadStreamBlockHeader(s);
    }
    else {
        sLOG << "stream header from" << s << "on channel" << id
             << "from" << header.sender_rank;

        dispatcher_.AsyncRead(
            s, header.size,
            [this, header, channel](Connection& s, net::Buffer&& buffer) {
                OnStreamBlock(s, header, channel, std::move(buffer));
            });
    }
}

void Multiplexer::OnStreamBlock(
    Connection& s, const StreamBlockHeader& header, const ChannelPtr& channel,
    net::Buffer&& buffer) {

    die_unless(header.size == buffer.size());

    // TODO(tb): don't copy data!
    ByteBlockPtr bytes = ByteBlock::Allocate(buffer.size());
    std::copy(buffer.data(), buffer.data() + buffer.size(), bytes->begin());

    size_t sender_worker_rank = header.sender_rank * num_workers_per_node_ + header.sender_local_worker_id;
    sLOG << "got block on" << s << "in channel" << header.channel_id << "from worker" << sender_worker_rank;
    channel->OnStreamBlock(
        sender_worker_rank,
        Block(bytes, 0, header.size, header.first_item, header.nitems));

    AsyncReadStreamBlockHeader(s);
}

} // namespace data
} // namespace c7a

/******************************************************************************/
