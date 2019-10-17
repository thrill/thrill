/*******************************************************************************
 * thrill/data/mix_stream.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/mix_stream.hpp>

#include <thrill/data/cat_stream.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>

#include <tlx/math/round_to_power_of_two.hpp>

#include <algorithm>
#include <map>
#include <vector>

namespace thrill {
namespace data {

MixStreamData::MixStreamData(StreamSetBase* stream_set_base,
                             Multiplexer& multiplexer, size_t send_size_limit,
                             const StreamId& id,
                             size_t local_worker_id, size_t dia_id)
    : StreamData(stream_set_base, multiplexer,
                 send_size_limit, id, local_worker_id, dia_id),
      seq_(num_workers()),
      queue_(multiplexer_.block_pool_, num_workers(),
             local_worker_id, dia_id) {
    remaining_closing_blocks_ = num_hosts() * workers_per_host();
}

MixStreamData::~MixStreamData() {
    LOG << "~MixStreamData() deleted";
}

void MixStreamData::set_dia_id(size_t dia_id) {
    dia_id_ = dia_id;
    queue_.set_dia_id(dia_id);
}

const char* MixStreamData::stream_type() {
    return "MixStream";
}

MixStreamData::Writers MixStreamData::GetWriters() {
    size_t hard_ram_limit = multiplexer_.block_pool_.hard_ram_limit();
    size_t block_size_base = hard_ram_limit / 4
                             / multiplexer_.num_workers() / multiplexer_.workers_per_host();
    size_t block_size = tlx::round_down_to_power_of_two(block_size_base);
    if (block_size == 0 || block_size > default_block_size)
        block_size = default_block_size;

    {
        std::unique_lock<std::mutex> lock(multiplexer_.mutex_);
        multiplexer_.active_streams_++;
        multiplexer_.max_active_streams_ =
            std::max(multiplexer_.max_active_streams_.load(),
                     multiplexer_.active_streams_.load());
    }

    LOGC(my_worker_rank() == 0 && 0)
        << "MixStreamData::GetWriters()"
        << " hard_ram_limit=" << hard_ram_limit
        << " block_size_base=" << block_size_base
        << " block_size=" << block_size
        << " active_streams=" << multiplexer_.active_streams_
        << " max_active_streams=" << multiplexer_.max_active_streams_;

    tx_timespan_.StartEventually();

    Writers result(my_worker_rank());
    result.reserve(num_workers());

    for (size_t host = 0; host < num_hosts(); ++host) {
        for (size_t worker = 0; worker < workers_per_host(); ++worker) {
            if (host == my_host_rank()) {
                // construct loopback queue writer
                auto target_stream_ptr = multiplexer_.MixLoopback(id_, worker);
                result.emplace_back(
                    StreamSink(
                        StreamDataPtr(this),
                        multiplexer_.block_pool_,
                        target_stream_ptr,
                        id_,
                        my_host_rank(), local_worker_id_,
                        host, worker),
                    block_size);
            }
            else {
                result.emplace_back(
                    StreamSink(
                        StreamDataPtr(this),
                        multiplexer_.block_pool_,
                        &multiplexer_.group_.connection(host),
                        MagicByte::MixStreamBlock,
                        id_,
                        my_host_rank(), local_worker_id_,
                        host, worker),
                    block_size);
            }
        }
    }

    assert(result.size() == num_workers());
    return result;
}

MixStreamData::MixReader MixStreamData::GetMixReader(bool consume) {
    rx_timespan_.StartEventually();
    return MixReader(queue_, consume, local_worker_id_);
}

MixStreamData::MixReader MixStreamData::GetReader(bool consume) {
    return GetMixReader(consume);
}

void MixStreamData::Close() {
    if (is_closed_) return;
    is_closed_ = true;

    // wait for all close packets to arrive.
    for (size_t i = 0; i < num_hosts() * workers_per_host(); ++i) {
        LOG << "MixStreamData::Close() wait for closing block"
            << " local_worker_id_=" << local_worker_id_
            << " remaining=" << sem_closing_blocks_.value();
        sem_closing_blocks_.wait();
    }

    die_unless(all_writers_closed_);

    {
        std::unique_lock<std::mutex> lock(multiplexer_.mutex_);
        multiplexer_.active_streams_--;
        multiplexer_.IntReleaseMixStream(id_, local_worker_id_);
    }

    LOG << "MixStreamData::Close() finished"
        << " id_=" << id_
        << " local_worker_id_=" << local_worker_id_;
}

bool MixStreamData::closed() const {
    if (is_closed_) return true;
    bool closed = true;
    closed = closed && queue_.write_closed();
    return closed;
}

bool MixStreamData::is_queue_closed(size_t from) {
    return queue_.is_queue_closed(from);
}

struct MixStreamData::SeqReordering {
    //! current top sequence number
    uint32_t                  seq_ = 0;

    //! queue of waiting Blocks, ordered by sequence number
    std::map<uint32_t, Block> waiting_;
};

void MixStreamData::OnStreamBlock(size_t from, uint32_t seq, Block&& b) {
    assert(from < num_workers());
    rx_timespan_.StartEventually();

    sLOG << "MixStreamData::OnStreamBlock" << b
         << "stream" << id_
         << "from" << from
         << "for worker" << my_worker_rank();

    if (TLX_UNLIKELY(seq != seq_[from].seq_ &&
                     seq != StreamMultiplexerHeader::final_seq)) {
        // sequence mismatch: put into queue
        die_unless(seq >= seq_[from].seq_);

        seq_[from].waiting_.insert(std::make_pair(seq, std::move(b)));

        return;
    }

    OnStreamBlockOrdered(from, std::move(b));

    // try to process additional queued blocks
    while (!seq_[from].waiting_.empty() &&
           (seq_[from].waiting_.begin()->first == seq_[from].seq_ ||
            seq_[from].waiting_.begin()->first == StreamMultiplexerHeader::final_seq))
    {
        sLOG << "MixStreamData::OnStreamBlock"
             << "processing delayed block with seq"
             << seq_[from].waiting_.begin()->first;

        OnStreamBlockOrdered(
            from, std::move(seq_[from].waiting_.begin()->second));

        seq_[from].waiting_.erase(
            seq_[from].waiting_.begin());
    }
}

void MixStreamData::OnStreamBlockOrdered(size_t from, Block&& b) {
    // sequence number matches
    if (b.IsValid()) {
        rx_net_items_ += b.num_items();
        rx_net_bytes_ += b.size();
        rx_net_blocks_++;

        queue_.AppendBlock(from, std::move(b));
    }
    else {
        sLOG << "MixStreamData::OnCloseStream"
             << "stream" << id_
             << "from" << from
             << "for worker" << my_worker_rank()
             << "remaining_closing_blocks_" << remaining_closing_blocks_;

        queue_.Close(from);

        die_unless(remaining_closing_blocks_ > 0);
        if (--remaining_closing_blocks_ == 0) {
            rx_lifetime_.StopEventually();
            rx_timespan_.StopEventually();
        }

        sem_closing_blocks_.signal();
    }

    seq_[from].seq_++;
}

/******************************************************************************/
// MixStream

MixStream::MixStream(const MixStreamDataPtr& ptr)
    : ptr_(ptr) { }

MixStream::~MixStream() {
    ptr_->Close();
}

const StreamId& MixStream::id() const {
    return ptr_->id();
}

StreamData& MixStream::data() {
    return *ptr_;
}

const StreamData& MixStream::data() const {
    return *ptr_;
}

MixStream::Writers MixStream::GetWriters() {
    return ptr_->GetWriters();
}

MixStream::MixReader MixStream::GetMixReader(bool consume) {
    return ptr_->GetMixReader(consume);
}

MixStream::MixReader MixStream::GetReader(bool consume) {
    return ptr_->GetReader(consume);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
