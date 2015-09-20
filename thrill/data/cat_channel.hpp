/*******************************************************************************
 * thrill/data/cat_channel.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_CAT_CHANNEL_HEADER
#define THRILL_DATA_CAT_CHANNEL_HEADER

#include <thrill/data/block_queue.hpp>
#include <thrill/data/cat_block_source.hpp>
#include <thrill/data/channel.hpp>
#include <thrill/data/channel_sink.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * A Channel is a virtual set of connections to all other worker instances,
 * hence a "Channel" bundles them to a logical communication context. We call an
 * individual connection from a worker to another worker a "Host".
 *
 * To use a Channel, one can get a vector of BlockWriter via OpenWriters() of
 * outbound Channel. The vector is of size of workers in the system.
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
 * As soon as all attached streams of the Channel have been Close() the number of
 * expected streams is reached, the channel is marked as finished and no more
 * data will arrive.
 */
class CatChannel final : public Channel
{
public:
    using BlockQueueSource = ConsumeBlockQueueSource;
    using BlockQueueReader = BlockReader<BlockQueueSource>;

    using CatBlockSource = data::CatBlockSource<DynBlockSource>;
    using CatBlockReader = BlockReader<CatBlockSource>;

    using Reader = BlockQueueReader;
    using CatReader = CatBlockReader;

    //! Creates a new channel instance
    CatChannel(Multiplexer& multiplexer, const ChannelId& id,
               size_t my_local_worker_id)
        : Channel(multiplexer, id, my_local_worker_id) {

        sinks_.reserve(multiplexer_.num_workers());
        queues_.reserve(multiplexer_.num_workers());

        // construct ChannelSink array
        for (size_t host = 0; host < multiplexer_.num_hosts(); ++host) {
            for (size_t worker = 0; worker < multiplexer_.num_workers_per_host_; worker++) {
                if (host == multiplexer_.my_host_rank()) {
                    sinks_.emplace_back(multiplexer_.block_pool_);
                }
                else {
                    sinks_.emplace_back(
                        multiplexer_.block_pool_,
                        &multiplexer_.dispatcher_,
                        &multiplexer_.group_.connection(host),
                        MagicByte::CAT_CHANNEL_BLOCK,
                        id,
                        multiplexer_.my_host_rank(), my_local_worker_id, worker,
                        &outgoing_bytes_, &outgoing_blocks_, &tx_timespan_);
                }
                // construct inbound queues
                queues_.emplace_back(multiplexer_.block_pool_);
            }
        }
    }

    //! non-copyable: delete copy-constructor
    CatChannel(const CatChannel&) = delete;
    //! non-copyable: delete assignment operator
    CatChannel& operator = (const CatChannel&) = delete;
    //! move-constructor: default
    CatChannel(CatChannel&&) = default;

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
    //! the BlockQueues in the Channel and wait for further Blocks to arrive or
    //! the Channel's remote close.
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

    //! Creates a BlockReader which concatenates items from all workers in
    //! worker rank order. The BlockReader is attached to one \ref
    //! CatBlockSource which includes all incoming queues of this channel.
    CatBlockReader OpenCatReader(bool consume) {
        rx_timespan_.StartEventually();

        // construct vector of BlockSources to read from queues_.
        std::vector<DynBlockSource> result;
        for (size_t worker = 0; worker < multiplexer_.num_workers(); ++worker) {
            result.emplace_back(queues_[worker].GetBlockSource(consume));
        }
        // move BlockQueueSources into concatenation BlockSource, and to Reader.
        return CatBlockReader(CatBlockSource(std::move(result)));
    }

    //! shuts the channel down.
    void Close() final {
        // close all sinks, this should emit sentinel to all other worker.
        for (size_t i = 0; i != sinks_.size(); ++i) {
            if (sinks_[i].closed()) continue;
            sinks_[i].Close();
        }

        // close loop-back queue from this worker to itself
        auto my_global_worker_id = multiplexer_.my_host_rank() * multiplexer_.num_workers_per_host() + my_local_worker_id_;
        if (!queues_[my_global_worker_id].write_closed())
            queues_[my_global_worker_id].Close();

        // wait for close packets to arrive (this is a busy waiting loop, try to
        // do it better -tb)
        for (size_t i = 0; i != queues_.size(); ++i) {
            while (!queues_[i].write_closed()) {
                LOG << "wait for close from worker" << i;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
        tx_lifetime_.StopEventually();
        tx_timespan_.StopEventually();
        CallClosedCallbacksEventually();
    }

    //! Indicates if the channel is closed - meaning all remaining streams have
    //! been closed. This does *not* include the loopback stream
    bool closed() const final {
        bool closed = true;
        for (auto& q : queues_) {
            closed = closed && q.write_closed();
        }
        return closed;
    }

protected:
    static const bool debug = false;

    //! ChannelSink objects are receivers of Blocks outbound for other worker.
    std::vector<ChannelSink> sinks_;

    //! BlockQueues to store incoming Blocks with no attached destination.
    std::vector<BlockQueue> queues_;

    //! for calling methods to deliver blocks
    friend class Multiplexer;

    //! called from Multiplexer when there is a new Block on a
    //! Channel.
    void OnChannelBlock(size_t from, Block&& b) {
        assert(from < queues_.size());
        rx_timespan_.StartEventually();
        incoming_bytes_ += b.size();
        incoming_blocks_++;

        sLOG << "OnCatChannelBlock" << b;

        if (debug) {
            sLOG << "channel" << id_ << "receive from" << from << ":"
                 << common::hexdump(b.ToString());
        }

        queues_[from].AppendBlock(b);
    }

    //! called from Multiplexer when a CatChannel closed notification was
    //! received.
    void OnCloseChannel(size_t from) {
        assert(from < queues_.size());
        queues_[from].Close();

        if (expected_closing_blocks_ == ++received_closing_blocks_) {
            rx_lifetime_.StopEventually();
            rx_timespan_.StopEventually();
            CallClosedCallbacksEventually();
        }
    }

    //! Returns the loopback queue for the worker of this channel.
    BlockQueue * loopback_queue(size_t from_worker_id) {
        assert(from_worker_id < multiplexer_.num_workers_per_host_);
        size_t global_worker_rank = multiplexer_.num_workers_per_host_ * multiplexer_.my_host_rank() + from_worker_id;
        sLOG << "expose loopback queue for" << from_worker_id << "->" << my_local_worker_id_;
        return &(queues_[global_worker_rank]);
    }
};

using CatChannelPtr = std::shared_ptr<CatChannel>;

using CatChannelSet = ChannelSet<CatChannel>;
using CatChannelSetPtr = std::shared_ptr<CatChannelSet>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_CAT_CHANNEL_HEADER

/******************************************************************************/
