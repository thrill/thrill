/*******************************************************************************
 * thrill/data/stream_data.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2017 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/stream.hpp>

#include <thrill/data/cat_stream.hpp>
#include <thrill/data/mix_stream.hpp>
#include <thrill/data/multiplexer_header.hpp>

namespace thrill {
namespace data {

/******************************************************************************/
// StreamData

StreamData::StreamData(StreamSetBase* stream_set_base,
                       Multiplexer& multiplexer, size_t send_size_limit,
                       const StreamId& id,
                       size_t local_worker_id, size_t dia_id)
    : sem_queue_(send_size_limit),
      id_(id),
      stream_set_base_(stream_set_base),
      local_worker_id_(local_worker_id),
      dia_id_(dia_id),
      multiplexer_(multiplexer)
{ }

StreamData::~StreamData() = default;

void StreamData::OnWriterClosed(size_t peer_worker_rank, bool sent) {
    ++writers_closed_;

    LOG << "StreamData::OnWriterClosed()"
        << " my_worker_rank= " << my_worker_rank()
        << " peer_worker_rank=" << peer_worker_rank
        << " writers_closed_=" << writers_closed_;

    die_unless(writers_closed_ <= num_hosts() * workers_per_host());

    stream_set_base_->OnWriterClosed(peer_worker_rank, sent);

    if (writers_closed_ == num_hosts() * workers_per_host()) {
        LOG << "StreamData::OnWriterClosed() final close received";

        tx_lifetime_.StopEventually();
        tx_timespan_.StopEventually();

        OnAllWritersClosed();
        all_writers_closed_ = true;
    }
}

void StreamData::OnAllWritersClosed() {
    multiplexer_.logger()
        << "class" << "StreamData"
        << "event" << "close"
        << "id" << id_
        << "type" << stream_type()
        << "dia_id" << dia_id_
        << "worker_rank"
        << (my_host_rank() * multiplexer_.workers_per_host())
        + local_worker_id_
        << "rx_net_items" << rx_net_items_
        << "rx_net_bytes" << rx_net_bytes_
        << "rx_net_blocks" << rx_net_blocks_
        << "tx_net_items" << tx_net_items_
        << "tx_net_bytes" << tx_net_bytes_
        << "tx_net_blocks" << tx_net_blocks_
        << "rx_int_items" << rx_int_items_
        << "rx_int_bytes" << rx_int_bytes_
        << "rx_int_blocks" << rx_int_blocks_
        << "tx_int_items" << tx_int_items_
        << "tx_int_bytes" << tx_int_bytes_
        << "tx_int_blocks" << tx_int_blocks_;
}

/******************************************************************************/
// StreamData::Writers

StreamData::Writers::Writers(size_t my_worker_rank)
    : my_worker_rank_(my_worker_rank)
{ }

void StreamData::Writers::Close() {
    // close BlockWriters in a cyclic fashion
    size_t s = size();
    for (size_t i = 0; i < s; ++i) {
        operator [] ((i + my_worker_rank_) % s).Close();
    }
}

StreamData::Writers::~Writers() {
    Close();
}

/******************************************************************************/
// StreamSet

template <typename StreamData>
StreamSet<StreamData>::StreamSet(Multiplexer& multiplexer, size_t send_size_limit,
                                 StreamId id, size_t workers_per_host, size_t dia_id)
    : multiplexer_(multiplexer), id_(id) {
    for (size_t i = 0; i < workers_per_host; ++i) {
        streams_.emplace_back(
            tlx::make_counting<StreamData>(
                this, multiplexer, send_size_limit, id, i, dia_id));
    }
    remaining_ = workers_per_host;
    writers_closed_per_host_.resize(num_hosts());
    writers_closed_per_host_sent_.resize(num_hosts());
}

template <typename StreamData>
tlx::CountingPtr<StreamData> StreamSet<StreamData>::Peer(size_t local_worker_id) {
    assert(local_worker_id < streams_.size());
    return streams_[local_worker_id];
}

template <typename StreamData>
bool StreamSet<StreamData>::Release(size_t local_worker_id) {
    std::unique_lock<std::mutex> lock(mutex_);
    assert(local_worker_id < streams_.size());
    if (streams_[local_worker_id]) {
        assert(remaining_ > 0);
        streams_[local_worker_id].reset();
        --remaining_;
    }
    return (remaining_ == 0);
}

template <typename StreamData>
void StreamSet<StreamData>::Close() {
    for (StreamDataPtr& c : streams_)
        c->Close();
}

template <typename StreamData>
void StreamSet<StreamData>::OnWriterClosed(size_t peer_worker_rank, bool sent) {
    std::unique_lock<std::mutex> lock(mutex_);

    size_t peer_host_rank = peer_worker_rank / workers_per_host();
    die_unless(peer_host_rank < writers_closed_per_host_.size());

    writers_closed_per_host_[peer_host_rank]++;
    if (sent)
        writers_closed_per_host_sent_[peer_host_rank]++;

    LOG << "StreamSet::OnWriterClosed()"
        << " my_host_rank= " << my_host_rank()
        << " peer_host_rank=" << peer_host_rank
        << " peer_worker_rank=" << peer_worker_rank
        << " writers_closed_per_host_[]="
        << writers_closed_per_host_[peer_host_rank];

    die_unless(writers_closed_per_host_[peer_host_rank] <=
               workers_per_host() * workers_per_host());

    if (writers_closed_per_host_[peer_host_rank] ==
        workers_per_host() * workers_per_host())
    {
        if (peer_host_rank == my_host_rank())
            return;

        if (writers_closed_per_host_[peer_host_rank] ==
            writers_closed_per_host_sent_[peer_host_rank])
        {
            LOG << "StreamSet::OnWriterClosed() final close already-done"
                << " my_host_rank=" << my_host_rank()
                << " peer_host_rank=" << peer_host_rank
                << " writers_closed_per_host_[]="
                << writers_closed_per_host_[peer_host_rank];
            return;
        }

        LOG << "StreamSet::OnWriterClosed() final close "
            << " my_host_rank=" << my_host_rank()
            << " peer_host_rank=" << peer_host_rank
            << " writers_closed_per_host_[]="
            << writers_closed_per_host_[peer_host_rank];

        StreamMultiplexerHeader header;
        header.magic = magic_byte();
        header.stream_id = id_;
        header.sender_worker = my_host_rank() * workers_per_host();
        header.receiver_local_worker = StreamMultiplexerHeader::all_workers;
        header.seq = StreamMultiplexerHeader::final_seq;

        net::BufferBuilder bb;
        header.Serialize(bb);

        net::Buffer buffer = bb.ToBuffer();
        assert(buffer.size() == MultiplexerHeader::total_size);

        net::Connection& conn = multiplexer_.group().connection(peer_host_rank);

        multiplexer_.dispatcher().AsyncWrite(
            conn, 42 + (conn.tx_seq_.fetch_add(2) & 0xFFFF),
            std::move(buffer));
    }
}

template <>
MagicByte StreamSet<CatStreamData>::magic_byte() const {
    return MagicByte::CatStreamBlock;
}

template <>
MagicByte StreamSet<MixStreamData>::magic_byte() const {
    return MagicByte::MixStreamBlock;
}

template class StreamSet<CatStreamData>;
template class StreamSet<MixStreamData>;

} // namespace data
} // namespace thrill

/******************************************************************************/
