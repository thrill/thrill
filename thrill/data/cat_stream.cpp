/*******************************************************************************
 * thrill/data/cat_stream.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/cat_stream.hpp>

#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>

#include <vector>

namespace thrill {
namespace data {

CatStream::CatStream(Multiplexer& multiplexer, const StreamId& id,
                     size_t local_worker_id, size_t dia_id)
    : Stream(multiplexer, id, local_worker_id, dia_id) {

    sinks_.reserve(num_workers());
    queues_.reserve(num_workers());

    // construct StreamSink array
    for (size_t host = 0; host < num_hosts(); ++host) {
        for (size_t worker = 0; worker < workers_per_host(); worker++) {
            if (host == my_host_rank()) {
                // construct loopback queue

                // insert placeholder in sinks_ array
                sinks_.emplace_back(
                    *this, multiplexer_.block_pool_, worker);

                multiplexer_.logger()
                    << "class" << "StreamSink"
                    << "event" << "open"
                    << "stream" << id_
                    << "peer_host" << host
                    << "src_worker" << my_worker_rank()
                    << "tgt_worker" << (host * workers_per_host() + worker)
                    << "loopback" << true;

                queues_.emplace_back(
                    multiplexer_.block_pool_, local_worker_id, dia_id,
                    // OnClose callback to BlockQueue to deliver stats
                    [this, host, worker](BlockQueue& queue) {

                        multiplexer_.logger()
                        << "class" << "StreamSink"
                        << "event" << "close"
                        << "stream" << id_
                        << "peer_host" << host
                        << "src_worker" << my_worker_rank()
                        << "tgt_worker" << (host * workers_per_host() + worker)
                        << "loopback" << true
                        << "bytes" << queue.byte_counter()
                        << "blocks" << queue.block_counter()
                        << "timespan" << queue.timespan();

                        tx_bytes_ += queue.byte_counter();
                        tx_blocks_ += queue.block_counter();
                    });
            }
            else {
                // construct outbound StreamSink

                sinks_.emplace_back(
                    *this,
                    multiplexer_.block_pool_,
                    &multiplexer_.group_.connection(host),
                    MagicByte::CatStreamBlock,
                    id,
                    my_host_rank(), local_worker_id,
                    host, worker);

                // construct inbound BlockQueue
                queues_.emplace_back(
                    multiplexer_.block_pool_, local_worker_id, dia_id);
            }
        }
    }
}

CatStream::~CatStream() {
    Close();
}

void CatStream::set_dia_id(size_t dia_id) {
    dia_id_ = dia_id;
    for (size_t i = 0; i < queues_.size(); ++i) {
        queues_[i].set_dia_id(dia_id);
    }
}

std::vector<CatStream::Writer>
CatStream::GetWriters(size_t block_size) {
    tx_timespan_.StartEventually();

    std::vector<Writer> result;
    result.reserve(num_workers());

    for (size_t host = 0; host < num_hosts(); ++host) {
        for (size_t worker = 0; worker < workers_per_host(); ++worker) {
            if (host == my_host_rank()) {
                auto target_queue_ptr = multiplexer_.CatLoopback(
                    id_, local_worker_id_, worker);
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

std::vector<CatStream::BlockQueueReader> CatStream::GetReaders() {
    rx_timespan_.StartEventually();

    std::vector<BlockQueueReader> result;
    result.reserve(num_workers());

    for (size_t host = 0; host < num_hosts(); ++host) {
        for (size_t worker = 0; worker < workers_per_host(); ++worker) {
            size_t worker_id = host * workers_per_host() + worker;
            result.emplace_back(
                BlockQueueSource(queues_[worker_id], local_worker_id_));
        }
    }

    assert(result.size() == num_workers());
    return result;
}

CatStream::CatBlockSource CatStream::GetCatBlockSource(bool consume) {
    rx_timespan_.StartEventually();

    // construct vector of BlockSources to read from queues_.
    std::vector<DynBlockSource> result;
    result.reserve(num_workers());

    for (size_t worker = 0; worker < num_workers(); ++worker) {
        result.emplace_back(
            queues_[worker].GetBlockSource(consume, local_worker_id_));
    }

    // move BlockQueueSources into concatenation BlockSource, and to Reader.
    return CatBlockSource(std::move(result));
}

CatStream::CatReader CatStream::GetCatReader(bool consume) {
    return CatBlockReader(GetCatBlockSource(consume));
}

CatStream::CatReader CatStream::GetReader(bool consume) {
    return GetCatReader(consume);
}

void CatStream::Close() {
    if (is_closed_) return;
    is_closed_ = true;

    sLOG << "CatStream" << id() << "close"
         << "host" << my_host_rank()
         << "local_worker_id_" << local_worker_id_;

    // close all sinks, this should emit sentinel to all other worker.
    for (size_t i = 0; i < sinks_.size(); ++i) {
        if (sinks_[i].closed()) continue;
        sLOG << "CatStream" << id() << "close"
             << "unopened sink" << i;
        sinks_[i].Close();
    }

    // close loop-back queue from this worker to itself
    auto my_global_worker_id = my_worker_rank();
    if (!queues_[my_global_worker_id].write_closed())
        queues_[my_global_worker_id].Close();

    // wait for close packets to arrive
    for (size_t i = 0; i < queues_.size() - workers_per_host(); ++i)
        sem_closing_blocks_.wait();

    tx_lifetime_.StopEventually();
    tx_timespan_.StopEventually();
    OnAllClosed();
}

bool CatStream::closed() const {
    bool closed = true;
    for (auto& q : queues_) {
        closed = closed && q.write_closed();
    }
    return closed;
}

void CatStream::OnStreamBlock(size_t from, PinnedBlock&& b) {
    assert(from < queues_.size());
    rx_timespan_.StartEventually();

    rx_bytes_ += b.size();
    rx_blocks_++;

    sLOG << "OnCatStreamBlock" << b;

    if (debug_data) {
        sLOG << "stream" << id_ << "receive from" << from << ":"
             << common::Hexdump(b.ToString());
    }

    queues_[from].AppendPinnedBlock(std::move(b));
}

void CatStream::OnCloseStream(size_t from) {
    assert(from < queues_.size());
    queues_[from].Close();

    rx_blocks_++;

    sLOG << "OnCatCloseStream from=" << from;

    if (--remaining_closing_blocks_ == 0) {
        rx_lifetime_.StopEventually();
        rx_timespan_.StopEventually();
    }

    sem_closing_blocks_.signal();
}

BlockQueue* CatStream::loopback_queue(size_t from_worker_id) {
    assert(from_worker_id < workers_per_host());
    size_t global_worker_rank = workers_per_host() * my_host_rank() + from_worker_id;
    sLOG << "expose loopback queue for" << from_worker_id << "->" << local_worker_id_;
    return &(queues_[global_worker_rank]);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
