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

CatStreamData::CatStreamData(Multiplexer& multiplexer, const StreamId& id,
                             size_t local_worker_id, size_t dia_id)
    : StreamData(multiplexer, id, local_worker_id, dia_id) {

    queues_.reserve(num_workers());

    // construct StreamSink array
    for (size_t host = 0; host < num_hosts(); ++host) {
        for (size_t worker = 0; worker < workers_per_host(); worker++) {
            if (host == my_host_rank()) {
                // construct loopback queue

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
                    // OnClose callback to BlockQueue to deliver stats, keep a
                    // smart pointer reference to this
                    [p = CatStreamDataPtr(this)](BlockQueue& queue) {
                        p->rx_int_items_ += queue.item_counter();
                        p->rx_int_bytes_ += queue.byte_counter();
                        p->rx_int_blocks_ += queue.block_counter();
                    });
            }
            else {
                // construct inbound BlockQueues
                queues_.emplace_back(
                    multiplexer_.block_pool_, local_worker_id, dia_id);
            }
        }
    }
}

CatStreamData::~CatStreamData() {
    LOG << "~CatStreamData() deleted";
}

void CatStreamData::set_dia_id(size_t dia_id) {
    dia_id_ = dia_id;
    for (size_t i = 0; i < queues_.size(); ++i) {
        queues_[i].set_dia_id(dia_id);
    }
}

std::vector<CatStreamData::Writer> CatStreamData::GetWriters() {
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

    LOG << "CatStreamData::GetWriters()"
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
                auto target_stream_ptr = multiplexer_.CatLoopback(id_, worker);
                BlockQueue* sink_queue_ptr =
                    target_stream_ptr->loopback_queue(local_worker_id_);
                result.emplace_back(
                    StreamSink(
                        StreamDataPtr(this),
                        multiplexer_.block_pool_,
                        sink_queue_ptr,
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
                        MagicByte::CatStreamBlock,
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

std::vector<CatStreamData::Reader> CatStreamData::GetReaders() {
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

CatStreamData::CatBlockSource CatStreamData::GetCatBlockSource(bool consume) {
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

CatStreamData::CatReader CatStreamData::GetCatReader(bool consume) {
    return CatBlockReader(GetCatBlockSource(consume));
}

CatStreamData::CatReader CatStreamData::GetReader(bool consume) {
    return GetCatReader(consume);
}

void CatStreamData::Close() {
    if (is_closed_) return;
    is_closed_ = true;

    sLOG << "CatStreamData" << id() << "close"
         << "host" << my_host_rank()
         << "local_worker_id_" << local_worker_id_;

    // close loop-back queue from this worker to itself
    auto my_global_worker_id = my_worker_rank();
    if (!queues_[my_global_worker_id].write_closed())
        queues_[my_global_worker_id].Close();

    // wait for close packets to arrive
    for (size_t i = 0; i < queues_.size() - workers_per_host(); ++i)
        sem_closing_blocks_.wait();

    tx_lifetime_.StopEventually();
    tx_timespan_.StopEventually();
    OnAllClosed("CatStreamData");

    {
        std::unique_lock<std::mutex> lock(multiplexer_.mutex_);
        multiplexer_.active_streams_--;
        multiplexer_.IntReleaseCatStream(id_, local_worker_id_);
    }

    LOG << "CatStreamData::Close() finished"
        << " id_=" << id_
        << " local_worker_id_=" << local_worker_id_;
}

bool CatStreamData::closed() const {
    bool closed = true;
    for (auto& q : queues_) {
        closed = closed && q.write_closed();
    }
    return closed;
}

void CatStreamData::OnStreamBlock(size_t from, PinnedBlock&& b) {
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

void CatStreamData::OnCloseStream(size_t from) {
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

BlockQueue* CatStreamData::loopback_queue(size_t from_worker_id) {
    assert(from_worker_id < workers_per_host());
    size_t global_worker_rank = workers_per_host() * my_host_rank() + from_worker_id;
    sLOG << "expose loopback queue for" << from_worker_id << "->" << local_worker_id_;
    return &(queues_[global_worker_rank]);
}

/******************************************************************************/
// CatStream

CatStream::CatStream(const CatStreamDataPtr& ptr)
    : ptr_(ptr) { }

CatStream::~CatStream() {
    ptr_->Close();
}

const StreamId& CatStream::id() const {
    return ptr_->id();
}

StreamData& CatStream::data() {
    return *ptr_;
}

const StreamData& CatStream::data() const {
    return *ptr_;
}

std::vector<CatStream::Writer> CatStream::GetWriters() {
    return ptr_->GetWriters();
}

std::vector<CatStream::Reader> CatStream::GetReaders() {
    return ptr_->GetReaders();
}

CatStream::CatReader CatStream::GetCatReader(bool consume) {
    return ptr_->GetCatReader(consume);
}

CatStream::CatReader CatStream::GetReader(bool consume) {
    return ptr_->GetReader(consume);
}

} // namespace data
} // namespace thrill

/******************************************************************************/
