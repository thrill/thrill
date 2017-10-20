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
#include <vector>

namespace thrill {
namespace data {

MixStreamData::MixStreamData(Multiplexer& multiplexer, const StreamId& id,
                             size_t local_worker_id, size_t dia_id)
    : StreamData(multiplexer, id, local_worker_id, dia_id),
      queue_(multiplexer_.block_pool_, num_workers(),
             local_worker_id, dia_id)
{ }

MixStreamData::~MixStreamData() {
    LOG << "~MixStreamData() deleted";
}

void MixStreamData::set_dia_id(size_t dia_id) {
    dia_id_ = dia_id;
    queue_.set_dia_id(dia_id);
}

std::vector<MixStreamData::Writer> MixStreamData::GetWriters() {
    size_t hard_ram_limit = multiplexer_.block_pool_.hard_ram_limit();
    size_t block_size_base = hard_ram_limit / 16 / multiplexer_.num_workers();
    size_t block_size = tlx::round_down_to_power_of_two(block_size_base);
    if (block_size == 0 || block_size > default_block_size)
        block_size = default_block_size;

    {
        std::unique_lock<std::mutex> lock(multiplexer_.mutex_);
        multiplexer_.active_streams_++;
        multiplexer_.max_active_streams_ =
            std::max(multiplexer_.max_active_streams_,
                     multiplexer_.active_streams_.load());
    }

    LOG << "MixStreamData::GetWriters()"
        << " hard_ram_limit=" << hard_ram_limit
        << " block_size_base=" << block_size_base
        << " block_size=" << block_size
        << " active_streams=" << multiplexer_.active_streams_
        << " max_active_streams=" << multiplexer_.max_active_streams_;

    tx_timespan_.StartEventually();

    std::vector<Writer> result;
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
            << " local_worker_id_=" << local_worker_id_;
        sem_closing_blocks_.wait();
    }

    tx_lifetime_.StopEventually();
    tx_timespan_.StopEventually();
    OnAllClosed("MixStreamData");

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

void MixStreamData::OnStreamBlock(size_t from, PinnedBlock&& b) {
    assert(from < num_workers());
    rx_timespan_.StartEventually();

    rx_net_items_ += b.num_items();
    rx_net_bytes_ += b.size();
    rx_net_blocks_++;

    sLOG << "OnMixStreamBlock" << b;

    sLOG0 << "stream" << id_ << "receive from" << from << ":"
          << tlx::hexdump(b.ToString());

    queue_.AppendBlock(from, std::move(b).MoveToBlock());
}

void MixStreamData::OnCloseStream(size_t from) {
    assert(from < num_workers());
    queue_.Close(from);

    rx_net_blocks_++;

    sLOG << "OnMixCloseStream stream" << id_
         << "from" << from
         << "for worker" << my_worker_rank();

    if (--remaining_closing_blocks_ == 0) {
        rx_lifetime_.StopEventually();
        rx_timespan_.StopEventually();
    }

    sem_closing_blocks_.signal();
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

std::vector<MixStream::Writer> MixStream::GetWriters() {
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
