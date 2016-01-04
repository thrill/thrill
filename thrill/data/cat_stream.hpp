/*******************************************************************************
 * thrill/data/cat_stream.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_CAT_STREAM_HEADER
#define THRILL_DATA_CAT_STREAM_HEADER

#include <thrill/data/block_queue.hpp>
#include <thrill/data/cat_block_source.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>
#include <thrill/data/stream.hpp>
#include <thrill/data/stream_sink.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * A Stream is a virtual set of connections to all other worker instances,
 * hence a "Stream" bundles them to a logical communication context. We call an
 * individual connection from a worker to another worker a "Host".
 *
 * To use a Stream, one can get a vector of BlockWriter via OpenWriters() of
 * outbound Stream. The vector is of size of workers in the system.
 * One can then write items destined to the
 * corresponding worker. The written items are buffered into a Block and only
 * sent when the Block is full. To force a send, use BlockWriter::Flush(). When
 * all items are sent, the BlockWriters **must** be closed using
 * BlockWriter::Close().
 *
 * To read the inbound Connection items, one can get a vector of BlockReader via
 * OpenReaders(), which can then be used to read items sent by individual
 * workers.
 *
 * Alternatively, one can use OpenReader() to get a BlockReader which delivers
 * all items from *all* worker in worker order (concatenating all inbound
 * Connections).
 *
 * As soon as all attached streams of the Stream have been Close() the number of
 * expected streams is reached, the stream is marked as finished and no more
 * data will arrive.
 */
class CatStream final : public Stream
{
public:
    using BlockQueueSource = ConsumeBlockQueueSource;
    using BlockQueueReader = BlockReader<BlockQueueSource>;

    using CatBlockSource = data::CatBlockSource<DynBlockSource>;
    using CatBlockReader = BlockReader<CatBlockSource>;

    using Reader = BlockQueueReader;
    using CatReader = CatBlockReader;

    //! Creates a new stream instance
    CatStream(Multiplexer& multiplexer, const StreamId& id,
              size_t my_local_worker_id)
        : Stream(multiplexer, id, my_local_worker_id) {

        sinks_.reserve(multiplexer_.num_workers());
        queues_.reserve(multiplexer_.num_workers());

        // construct StreamSink array
        for (size_t host = 0; host < multiplexer_.num_hosts(); ++host) {
            for (size_t worker = 0; worker < multiplexer_.num_workers_per_host_; worker++) {
                if (host == multiplexer_.my_host_rank()) {
                    sinks_.emplace_back(multiplexer_.block_pool_, worker);
                }
                else {
                    sinks_.emplace_back(
                        multiplexer_.block_pool_,
                        &multiplexer_.dispatcher_,
                        &multiplexer_.group_.connection(host),
                        MagicByte::CatStreamBlock,
                        id,
                        multiplexer_.my_host_rank(), my_local_worker_id,
                        host, worker,
                        &outgoing_bytes_, &outgoing_blocks_, &tx_timespan_);
                }
                // construct inbound queues
                queues_.emplace_back(multiplexer_.block_pool_, worker);
            }
        }
    }

    //! non-copyable: delete copy-constructor
    CatStream(const CatStream&) = delete;
    //! non-copyable: delete assignment operator
    CatStream& operator = (const CatStream&) = delete;
    //! move-constructor: default
    CatStream(CatStream&&) = default;

    ~CatStream() final {
        Close();
    }

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<Writer>
    OpenWriters(size_t block_size = default_block_size) final {
        tx_timespan_.StartEventually();

        std::vector<Writer> result;

        for (size_t host = 0; host < multiplexer_.num_hosts(); ++host) {
            for (size_t local_worker_id = 0; local_worker_id < multiplexer_.num_workers_per_host_; ++local_worker_id) {
                if (host == multiplexer_.my_host_rank()) {
                    auto target_queue_ptr = multiplexer_.CatLoopback(id_, my_local_worker_id_, local_worker_id);
                    result.emplace_back(target_queue_ptr, block_size);
                }
                else {
                    size_t worker_id = host * multiplexer_.num_workers_per_host_ + local_worker_id;
                    result.emplace_back(&sinks_[worker_id], block_size);
                }
            }
        }

        assert(result.size() == multiplexer_.num_workers());
        return result;
    }

    //! Creates a BlockReader for each worker. The BlockReaders are attached to
    //! the BlockQueues in the Stream and wait for further Blocks to arrive or
    //! the Stream's remote close.
    std::vector<BlockQueueReader> OpenReaders() {
        rx_timespan_.StartEventually();

        std::vector<BlockQueueReader> result;

        for (size_t host = 0; host < multiplexer_.num_hosts(); ++host) {
            for (size_t local_worker_id = 0; local_worker_id < multiplexer_.num_workers_per_host_; ++local_worker_id) {
                size_t worker_id = host * multiplexer_.num_workers_per_host_ + local_worker_id;
                result.emplace_back(BlockQueueSource(queues_[worker_id]));
            }
        }

        assert(result.size() == multiplexer_.num_workers());
        return result;
    }

    //! Gets a CatBlockSource which includes all incoming queues of this stream.
    inline CatBlockSource GetCatBlockSource(bool consume) {
        rx_timespan_.StartEventually();

        // construct vector of BlockSources to read from queues_.
        std::vector<DynBlockSource> result;
        for (size_t worker = 0; worker < multiplexer_.num_workers(); ++worker) {
            result.emplace_back(queues_[worker].GetBlockSource(consume));
        }

        // move BlockQueueSources into concatenation BlockSource, and to Reader.
        return CatBlockSource(std::move(result));
    }

    //! Creates a BlockReader which concatenates items from all workers in
    //! worker rank order. The BlockReader is attached to one \ref
    //! CatBlockSource which includes all incoming queues of this stream.
    CatBlockReader OpenCatReader(bool consume) {
        return CatBlockReader(GetCatBlockSource(consume));
    }

    //! Open a CatReader (function name matches a method in MixStream).
    CatReader OpenAnyReader(bool consume) {
        return OpenCatReader(consume);
    }

    //! shuts the stream down.
    void Close() final {
        if (is_closed_) return;
        is_closed_ = true;

        sLOG << "CatStream" << id() << "close"
             << "host" << multiplexer_.my_host_rank()
             << "local_worker" << my_local_worker_id_;

        // close all sinks, this should emit sentinel to all other worker.
        for (size_t i = 0; i < sinks_.size(); ++i) {
            if (sinks_[i].closed()) continue;
            sLOG << "CatStream" << id() << "close"
                 << "unopened sink" << i;
            sinks_[i].Close();
        }

        // close loop-back queue from this worker to itself
        auto my_global_worker_id = multiplexer_.my_host_rank() * multiplexer_.num_workers_per_host() + my_local_worker_id_;
        if (!queues_[my_global_worker_id].write_closed())
            queues_[my_global_worker_id].Close();

        // wait for close packets to arrive (this is a busy waiting loop, try to
        // do it better -tb)
        for (size_t i = 0; i < queues_.size(); ++i) {
            while (!queues_[i].write_closed()) {
                sLOG << "CatStream" << id()
                     << "host" << multiplexer_.my_host_rank()
                     << "local_worker" << my_local_worker_id_
                     << "wait for close from worker" << i;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        tx_lifetime_.StopEventually();
        tx_timespan_.StopEventually();
        CallClosedCallbacksEventually();
    }

    //! Indicates if the stream is closed - meaning all remaining streams have
    //! been closed. This does *not* include the loopback stream
    bool closed() const final {
        bool closed = true;
        for (auto& q : queues_) {
            closed = closed && q.write_closed();
        }
        return closed;
    }

private:
    static const bool debug = false;

    bool is_closed_ = false;

    //! StreamSink objects are receivers of Blocks outbound for other worker.
    std::vector<StreamSink> sinks_;

    //! BlockQueues to store incoming Blocks with no attached destination.
    std::vector<BlockQueue> queues_;

    //! for calling methods to deliver blocks
    friend class Multiplexer;

    //! called from Multiplexer when there is a new Block on a
    //! Stream.
    void OnStreamBlock(size_t from, PinnedBlock&& b) {
        assert(from < queues_.size());
        rx_timespan_.StartEventually();
        incoming_bytes_ += b.size();
        incoming_blocks_++;

        sLOG << "OnCatStreamBlock" << b;

        if (debug) {
            sLOG << "stream" << id_ << "receive from" << from << ":"
                 << common::Hexdump(b.ToString());
        }

        queues_[from].AppendBlock(std::move(b));
    }

    //! called from Multiplexer when a CatStream closed notification was
    //! received.
    void OnCloseStream(size_t from) {
        assert(from < queues_.size());
        queues_[from].Close();

        sLOG << "OnCatCloseStream from=" << from;

        if (expected_closing_blocks_ == ++received_closing_blocks_) {
            rx_lifetime_.StopEventually();
            rx_timespan_.StopEventually();
            CallClosedCallbacksEventually();
        }
    }

    //! Returns the loopback queue for the worker of this stream.
    BlockQueue * loopback_queue(size_t from_worker_id) {
        assert(from_worker_id < multiplexer_.num_workers_per_host_);
        size_t global_worker_rank = multiplexer_.num_workers_per_host_ * multiplexer_.my_host_rank() + from_worker_id;
        sLOG << "expose loopback queue for" << from_worker_id << "->" << my_local_worker_id_;
        return &(queues_[global_worker_rank]);
    }
};

using CatStreamPtr = std::shared_ptr<CatStream>;

using CatStreamSet = StreamSet<CatStream>;
using CatStreamSetPtr = std::shared_ptr<CatStreamSet>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_CAT_STREAM_HEADER

/******************************************************************************/
