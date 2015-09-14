/*******************************************************************************
 * thrill/data/mixed_channel.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_MIXED_CHANNEL_HEADER
#define THRILL_DATA_MIXED_CHANNEL_HEADER

#include <thrill/data/channel_base.hpp>
#include <thrill/data/channel_sink.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/mixed_block_queue.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

using ChannelId = size_t;

/*!
 * A Channel is a virtual set of connections to all other worker instances,
 * hence a "Channel" bundles them to a logical communication context. We call an
 * individual connection from a worker to another worker a "Host".
 *
 * To use a Channel, one can get a vector of BlockWriter via OpenWriters() of
 * outbound Channel. The vector is of size of workers in the system.  One can
 * then write items destined to the corresponding worker. The written items are
 * buffered into a Block and only sent when the Block is full. To force a send,
 * use BlockWriter::Flush(). When all items are sent, the BlockWriters **must**
 * be closed using BlockWriter::Close().
 *
 * The MixedChannel allows reading of items from all workers in an unordered
 * sequence, without waiting for any of the workers to complete sending items.
 */
class MixedChannel : public ChannelBase
{
public:
    using MixedReader = MixedBlockQueueReader;

    //! Creates a new channel instance
    MixedChannel(Multiplexer& multiplexer, const ChannelId& id,
                 size_t my_local_worker_id)
        : ChannelBase(multiplexer, id, my_local_worker_id),
          queue_(multiplexer_.block_pool_, multiplexer_.num_workers()) {

        sinks_.reserve(multiplexer_.num_workers());
        loopback_.reserve(multiplexer_.num_workers());

        // construct ChannelSink array
        for (size_t host = 0; host < multiplexer_.num_hosts(); ++host) {
            for (size_t worker = 0; worker < multiplexer_.num_workers_per_host_; worker++) {
                if (host == multiplexer_.my_host_rank()) {
                    // dummy entries
                    sinks_.emplace_back(multiplexer_.block_pool_);
                }
                else {
                    // ChannelSink which transmits MIXED_CHANNEL_BLOCKs
                    sinks_.emplace_back(
                        multiplexer_.block_pool_,
                        &multiplexer_.dispatcher_,
                        &multiplexer_.group_.connection(host),
                        MagicByte::MIXED_CHANNEL_BLOCK,
                        id,
                        multiplexer_.my_host_rank(), my_local_worker_id, worker,
                        &outgoing_bytes_, &outgoing_blocks_, &tx_timespan_);
                }
            }
        }

        // construct MixedBlockQueueSink for loopback writers
        for (size_t worker = 0; worker < multiplexer_.num_workers_per_host_; worker++) {
            loopback_.emplace_back(
                queue_,
                multiplexer_.my_host_rank() * multiplexer_.num_workers_per_host() + worker);
        }
    }

    //! non-copyable: delete copy-constructor
    MixedChannel(const MixedChannel&) = delete;
    //! non-copyable: delete assignment operator
    MixedChannel& operator = (const MixedChannel&) = delete;
    //! move-constructor: default
    MixedChannel(MixedChannel&&) = default;

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<Writer> OpenWriters(size_t block_size = default_block_size) {
        tx_timespan_.StartEventually();

        std::vector<Writer> result;

        for (size_t host = 0; host < multiplexer_.num_hosts(); ++host) {
            for (size_t local_worker_id = 0; local_worker_id < multiplexer_.num_workers_per_host_; ++local_worker_id) {
                if (host == multiplexer_.my_host_rank()) {
                    auto target_queue_ptr =
                        multiplexer_.MixedLoopback(id_, my_local_worker_id_, local_worker_id);
                    result.emplace_back(target_queue_ptr, block_size);
                }
                else {
                    size_t worker_id =
                        host * multiplexer_.num_workers_per_host_ + local_worker_id;
                    result.emplace_back(&sinks_[worker_id], block_size);
                }
            }
        }

        assert(result.size() == multiplexer_.num_workers());
        return result;
    }

    //! Creates a BlockReader which mixes items from all workers.
    MixedReader OpenMixedReader(bool consume) {
        rx_timespan_.StartEventually();
        return MixedReader(queue_, consume);
    }

    /*!
     * Scatters a File to many worker
     *
     * elements from 0..offset[0] are sent to the first worker,
     * elements from (offset[0] + 1)..offset[1] are sent to the second worker.
     * elements from (offset[my_rank - 1] + 1)..(offset[my_rank]) are copied
     * The offset values range from 0..Manager::GetNumElements().
     * The number of given offsets must be equal to the net::Group::num_workers() * workers_per_host_.
     *
     * /param source File containing the data to be scattered.
     *
     * /param offsets - as described above. offsets.size must be equal to group.size
     */
    template <typename ItemType>
    void Scatter(const File& source, const std::vector<size_t>& offsets) {
        tx_timespan_.StartEventually();

        // current item offset in Reader
        size_t current = 0;
        File::KeepReader reader = source.GetKeepReader();

        std::vector<Writer> writers = OpenWriters();

        for (size_t worker = 0; worker < multiplexer_.num_workers(); ++worker) {
            // write [current,limit) to this worker
            size_t limit = offsets[worker];
            assert(current <= limit);
#if 0
            for ( ; current < limit; ++current) {
                assert(reader.HasNext());
                // move over one item (with deserialization and serialization)
                writers[worker](reader.template Next<ItemType>());
            }
#else
            if (current != limit) {
                writers[worker].AppendBlocks(
                    reader.template GetItemBatch<ItemType>(limit - current));
                current = limit;
            }
#endif
            writers[worker].Close();
        }

        tx_timespan_.Stop();
    }

    //! shuts the channel down.
    void Close() {
        // close all sinks, this should emit sentinel to all other worker.
        for (size_t i = 0; i != sinks_.size(); ++i) {
            if (sinks_[i].closed()) continue;
            sinks_[i].Close();
        }

        // close loop-back queue from this worker to all others on this host.
        for (size_t local_worker_id = 0;
             local_worker_id < multiplexer_.num_workers_per_host(); ++local_worker_id)
        {
            auto queue_ptr = multiplexer_.MixedLoopback(
                id_, my_local_worker_id_, local_worker_id);

            if (!queue_ptr->write_closed())
                queue_ptr->Close();
        }

        // wait for close packets to arrive (this is a busy waiting loop, try to
        // do it better -tb)
        while (!queue_.write_closed()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        tx_lifetime_.StopEventually();
        tx_timespan_.StopEventually();
        CallClosedCallbacksEventually();
    }

    //! Indicates if the channel is closed - meaning all remaining streams have
    //! been closed.
    bool closed() const final {
        bool closed = true;
        closed = closed && queue_.write_closed();
        return closed;
    }

protected:
    static const bool debug = false;

    //! ChannelSink objects are receivers of Blocks outbound for other worker.
    std::vector<ChannelSink> sinks_;

    //! BlockQueue to store incoming Blocks with source.
    MixedBlockQueue queue_;

    //! vector of MixedBlockQueueSink which serve as loopback BlockSinks into
    //! the MixedBlockQueue
    std::vector<MixedBlockQueueSink> loopback_;

    //! for calling methods to deliver blocks
    friend class Multiplexer;

    //! called from Multiplexer when there is a new Block for this Channel.
    void OnChannelBlock(size_t from, Block&& b) {
        assert(from < multiplexer_.num_workers());
        rx_timespan_.StartEventually();
        incoming_bytes_ += b.size();
        incoming_blocks_++;

        sLOG << "OnMixedChannelBlock" << b;

        if (debug) {
            sLOG << "channel" << id_ << "receive from" << from << ":"
                 << common::hexdump(b.ToString());
        }

        queue_.AppendBlock(from, b);
    }

    //! called from Multiplexer when a MixedChannel closed notification was
    //! received.
    void OnCloseChannel(size_t from) {
        assert(from < multiplexer_.num_workers());
        queue_.Close(from);

        if (expected_closing_blocks_ == ++received_closing_blocks_) {
            rx_lifetime_.StopEventually();
            rx_timespan_.StopEventually();
            CallClosedCallbacksEventually();
        }
    }

    //! Returns the loopback queue for the worker of this channel.
    MixedBlockQueueSink * loopback_queue(size_t from_worker_id) {
        assert(from_worker_id < multiplexer_.num_workers_per_host_);
        assert(from_worker_id < loopback_.size());
        sLOG0 << "expose loopback queue for" << from_worker_id;
        return &(loopback_[from_worker_id]);
    }
};

using MixedChannelPtr = std::shared_ptr<MixedChannel>;

using MixedChannelSet = ChannelSet<MixedChannel>;
using MixedChannelSetPtr = std::shared_ptr<MixedChannelSet>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MIXED_CHANNEL_HEADER

/******************************************************************************/
