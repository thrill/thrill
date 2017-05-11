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

MixStream::MixStream(Multiplexer& multiplexer, const StreamId& id,
                     size_t local_worker_id, size_t dia_id)
    : Stream(multiplexer, id, local_worker_id, dia_id),
      queue_(multiplexer_.block_pool_, num_workers(),
             local_worker_id, dia_id) {

    sinks_.reserve(num_workers());
    loopback_.reserve(num_workers());

    // construct StreamSink array
    for (size_t host = 0; host < num_hosts(); ++host) {
        for (size_t worker = 0; worker < workers_per_host(); worker++) {
            if (host == my_host_rank()) {
                // dummy entries
                sinks_.emplace_back(*this, multiplexer_.block_pool_, worker);
            }
            else {
                // StreamSink which transmits MIX_STREAM_BLOCKs
                sinks_.emplace_back(
                    *this,
                    multiplexer_.block_pool_,
                    &multiplexer_.group_.connection(host),
                    MagicByte::MixStreamBlock,
                    id,
                    my_host_rank(), local_worker_id,
                    host, worker);
            }
        }
    }

    // construct MixBlockQueueSink for loopback writers
    for (size_t worker = 0; worker < workers_per_host(); worker++) {
        loopback_.emplace_back(
            *this,
            my_host_rank() * multiplexer_.workers_per_host() + worker,
            worker);
    }
}

MixStream::~MixStream() {
    Close();
}

void MixStream::set_dia_id(size_t dia_id) {
    dia_id_ = dia_id;
    queue_.set_dia_id(dia_id);
}

std::vector<MixStream::Writer> MixStream::GetWriters() {
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

    LOG << "MixStream::GetWriters()"
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
                auto target_queue_ptr =
                    multiplexer_.MixLoopback(id_, local_worker_id_, worker);
                result.emplace_back(target_queue_ptr, block_size);
                target_queue_ptr->set_src_mix_stream(this);
            }
            else {
                size_t worker_id = host * workers_per_host() + worker;
                result.emplace_back(&sinks_[worker_id], block_size);
            }
        }
    }

    assert(result.size() == num_workers());
    return result;
}

MixStream::MixReader MixStream::GetMixReader(bool consume) {
    rx_timespan_.StartEventually();
    return MixReader(queue_, consume, local_worker_id_);
}

MixStream::MixReader MixStream::GetReader(bool consume) {
    return GetMixReader(consume);
}

void MixStream::Close() {
    if (is_closed_) return;
    is_closed_ = true;

    // close all sinks, this should emit sentinel to all other worker.
    for (size_t i = 0; i != sinks_.size(); ++i) {
        if (sinks_[i].closed()) continue;
        sinks_[i].Close();
    }

    // close loop-back queue from this worker to all others on this host.
    for (size_t worker = 0;
         worker < multiplexer_.workers_per_host(); ++worker)
    {
        auto queue_ptr = multiplexer_.MixLoopback(
            id_, local_worker_id_, worker);

        if (!queue_ptr->write_closed())
            queue_ptr->Close();
    }

    // wait for all close packets to arrive.
    for (size_t i = 0; i < (num_hosts() - 1) * workers_per_host(); ++i) {
        LOG << "MixStream::Close() wait for closing block"
            << " local_worker_id_=" << local_worker_id_;
        sem_closing_blocks_.wait();
    }

    multiplexer_.active_streams_--;

    tx_lifetime_.StopEventually();
    tx_timespan_.StopEventually();
    OnAllClosed("MixStream");

    LOG << "MixStream::Close() finished"
        << " id_=" << id_
        << " local_worker_id_=" << local_worker_id_;
}

bool MixStream::closed() const {
    if (is_closed_) return true;
    bool closed = true;
    closed = closed && queue_.write_closed();
    return closed;
}

void MixStream::OnStreamBlock(size_t from, PinnedBlock&& b) {
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

void MixStream::OnCloseStream(size_t from) {
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

MixBlockQueueSink* MixStream::loopback_queue(size_t from_worker_id) {
    assert(from_worker_id < workers_per_host());
    assert(from_worker_id < loopback_.size());
    sLOG0 << "expose loopback queue for" << from_worker_id;
    return &(loopback_[from_worker_id]);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
