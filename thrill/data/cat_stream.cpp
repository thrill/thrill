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

#include <thrill/data/mix_stream.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>

#include <tlx/math/round_to_power_of_two.hpp>
#include <tlx/string/hexdump.hpp>

#include <algorithm>
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
                    << "id" << id_
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
                        << "id" << id_
                        << "peer_host" << host
                        << "src_worker" << my_worker_rank()
                        << "tgt_worker" << (host * workers_per_host() + worker)
                        << "loopback" << true
                        << "items" << queue.item_counter()
                        << "bytes" << queue.byte_counter()
                        << "blocks" << queue.block_counter()
                        << "timespan" << queue.timespan();

                        CatStream* source = reinterpret_cast<CatStream*>(queue.source());
                        if (source) {
                            source->tx_int_items_ += queue.item_counter();
                            source->tx_int_bytes_ += queue.byte_counter();
                            source->tx_int_blocks_ += queue.block_counter();
                        }

                        rx_int_items_ += queue.item_counter();
                        rx_int_bytes_ += queue.byte_counter();
                        rx_int_blocks_ += queue.block_counter();
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

std::vector<CatStream::Writer> CatStream::GetWriters() {
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

    LOG << "CatStream::GetWriters()"
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
                auto target_queue_ptr = multiplexer_.CatLoopback(
                    id_, local_worker_id_, worker);
                target_queue_ptr->set_source(this);
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

std::vector<CatStream::Reader> CatStream::GetReaders() {
    rx_timespan_.StartEventually();

    std::vector<BlockQueueReader> result;
    result.reserve(num_workers());

    for (size_t worker = 0; worker < num_workers(); ++worker) {
        result.emplace_back(
            BlockQueueSource(queues_[worker], local_worker_id_));
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

    {
        std::unique_lock<std::mutex> lock(multiplexer_.mutex_);
        multiplexer_.active_streams_--;
    }

    tx_lifetime_.StopEventually();
    tx_timespan_.StopEventually();
    OnAllClosed("CatStream");

    LOG << "CatStream::Close() finished"
        << " id_=" << id_
        << " local_worker_id_=" << local_worker_id_;
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

    rx_net_items_ += b.num_items();
    rx_net_bytes_ += b.size();
    rx_net_blocks_++;

    sLOG << "OnCatStreamBlock" << b;

    if (debug_data) {
        sLOG << "stream" << id_ << "receive from" << from << ":"
             << tlx::hexdump(b.ToString());
    }

    queues_[from].AppendPinnedBlock(std::move(b), /* is_last_block */ false);
}

void CatStream::OnCloseStream(size_t from) {
    assert(from < queues_.size());
    queues_[from].Close();

    rx_net_blocks_++;

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
