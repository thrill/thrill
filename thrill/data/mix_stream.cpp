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
            queue_,
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

std::vector<MixStream::Writer>
MixStream::GetWriters(size_t block_size) {
    tx_timespan_.StartEventually();

    std::vector<Writer> result;
    result.reserve(num_workers());

    for (size_t host = 0; host < num_hosts(); ++host) {
        for (size_t worker = 0; worker < workers_per_host(); ++worker) {
            if (host == my_host_rank()) {
                auto target_queue_ptr =
                    multiplexer_.MixLoopback(id_, local_worker_id_, worker);
                result.emplace_back(target_queue_ptr, block_size);
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

    // wait for close packets to arrive.
    while (!queue_.write_closed())
        sem_closing_blocks_.wait();

    tx_lifetime_.StopEventually();
    tx_timespan_.StopEventually();
    OnAllClosed();
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

    rx_items_ += b.num_items();
    rx_bytes_ += b.size();
    rx_blocks_++;

    sLOG << "OnMixStreamBlock" << b;

    sLOG << "stream" << id_ << "receive from" << from << ":"
         << common::Hexdump(b.ToString());

    queue_.AppendBlock(from, std::move(b).MoveToBlock());
}

void MixStream::OnCloseStream(size_t from) {
    assert(from < num_workers());
    queue_.Close(from);

    rx_blocks_++;

    sLOG << "OnMixCloseStream from=" << from;

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
