/*******************************************************************************
 * c7a/data/channel.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_HEADER
#define C7A_DATA_CHANNEL_HEADER

#include <c7a/common/stats_counter.hpp>
#include <c7a/common/stats_timer.hpp>
#include <c7a/data/block_queue.hpp>
#include <c7a/data/channel_sink.hpp>
#include <c7a/data/concat_block_source.hpp>
#include <c7a/data/dyn_block_reader.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/multiplexer_header.hpp>
#include <c7a/net/connection.hpp>
#include <c7a/net/group.hpp>

#include <sstream>
#include <string>
#include <vector>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

using ChannelId = size_t;

/*!
 * A Channel is a virtual set of connections to all other worker instances,
 * hence a "Channel" bundles them to a logical communication context. We call an
 * individual connection from a worker to another worker a "Connection".
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
class Channel
{
public:
    using BlockQueueReader = BlockReader<BlockQueueSource>;
    using ConcatBlockSource = data::ConcatBlockSource<BlockQueueSource>;
    using ConcatBlockReader = BlockReader<ConcatBlockSource>;

    using CachingConcatBlockSource = data::ConcatBlockSource<CachingBlockQueueSource>;
    using CachingConcatBlockReader = BlockReader<CachingConcatBlockSource>;

    using Reader = BlockQueueReader;
    using ConcatReader = ConcatBlockReader;
    using CachingConcatReader = CachingConcatBlockReader;

    using StatsCounter = common::StatsCounter<size_t, common::g_enable_stats>;
    using StatsTimer = common::StatsTimer<common::g_enable_stats>;

    using ClosedCallback = std::function<void()>;

    //! Creates a new channel instance
    Channel(const ChannelId& id, net::Group& group,
            net::DispatcherThread& dispatcher, size_t my_local_worker_id, size_t workers_per_connection)
        :  tx_lifetime_(true), rx_lifetime_(true),
          tx_timespan_(), rx_timespan_(),
          id_(id),
          queues_(group.num_connections() * workers_per_connection),
          cache_files_(group.num_connections() * workers_per_connection),
          group_(group),
          dispatcher_(dispatcher),
          my_local_worker_id_(my_local_worker_id),
          workers_per_connection_(workers_per_connection),
          expected_closing_blocks_((group.num_connections() - 1) * workers_per_connection),
          received_closing_blocks_(0) {
        // construct ChannelSink array
        for (size_t host = 0; host < group_.num_connections(); ++host) {
            for (size_t worker = 0; worker < workers_per_connection_; worker++) {
                if (host == group_.my_connection_id()) {
                    sinks_.emplace_back();
                }
                else {
                    sinks_.emplace_back(
                        &dispatcher, &group_.connection(host),
                        id,
                        group_.my_connection_id(),
                        my_local_worker_id_,
                        worker, &outgoing_bytes_, &outgoing_blocks_, &tx_timespan_);
                }
            }
        }
    }

    //! non-copyable: delete copy-constructor
    Channel(const Channel&) = delete;
    //! non-copyable: delete assignment operator
    Channel& operator = (const Channel&) = delete;

    //! move-constructor
    Channel(Channel&&) = default;

    const ChannelId & id() const {
        return id_;
    }

    //! Creates BlockWriters for each woker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<BlockWriter> OpenWriters(size_t block_size = default_block_size) {
        tx_timespan_.StartEventually();

        std::vector<BlockWriter> result;

        for (size_t host_id = 0; host_id < group_.num_connections(); ++host_id) {
            for (size_t local_worker_id = 0; local_worker_id < workers_per_connection_; ++local_worker_id) {
                size_t worker_id = host_id * workers_per_connection_ + local_worker_id;
                if (host_id == group_.my_connection_id()) {
                    result.emplace_back(&queues_[worker_id], block_size);
                }
                else {
                    result.emplace_back(&sinks_[worker_id], block_size);
                }
            }
        }

        assert(result.size() == num_workers());
        return result;
    }

    //! Creates a BlockReader for each worker. The BlockReaders are attached to
    //! the BlockQueues in the Channel and wait for further Blocks to arrive or
    //! the Channel's remote close.
    std::vector<BlockQueueReader> OpenReaders() {
        rx_timespan_.StartEventually();

        std::vector<BlockQueueReader> result;

        for (size_t local_worker_id = 0; local_worker_id < num_workers(); ++local_worker_id) {
            result.emplace_back(BlockQueueSource(queues_[local_worker_id]));
        }

        assert(result.size() == num_workers());
        return result;
    }

    //! Creates a BlockReader for all workers. The BlockReader is attached to
    //! one \ref ConcatBlockSource which includes all incoming queues of
    //! this channel.
    ConcatBlockReader OpenReader() {
        rx_timespan_.StartEventually();

        // construct vector of BlockQueueSources to read from queues_.
        std::vector<BlockQueueSource> result;

        for (size_t local_worker_id = 0; local_worker_id < num_workers(); ++local_worker_id) {
            result.emplace_back(queues_[local_worker_id]);
        }
        // move BlockQueueSources into concatenation BlockSource, and to Reader.
        return ConcatBlockReader(ConcatBlockSource(result));
    }

    //! Creates a BlockReader for all workers. The BlockReader is attached to
    //! one \ref ConcatBlockSource which includes all incoming queues of this
    //! channel. The received Blocks are also cached in the Channel, hence this
    //! function can be called multiple times to read the items again.
    CachingConcatBlockReader OpenCachingReader() {
        rx_timespan_.StartEventually();

        // construct vector of CachingBlockQueueSources to read from queues_.
        std::vector<CachingBlockQueueSource> result;
        for (size_t local_worker_id = 0; local_worker_id < num_workers(); ++local_worker_id) {
            result.emplace_back(queues_[local_worker_id], cache_files_[local_worker_id]);
        }
        // move CachingBlockQueueSources into concatenation BlockSource, and to
        // Reader.
        return CachingConcatBlockReader(CachingConcatBlockSource(result));
    }

    /*!
     * Scatters a File to many worker
     *
     * elements from 0..offset[0] are sent to the first worker,
     * elements from (offset[0] + 1)..offset[1] are sent to the second worker.
     * elements from (offset[my_rank - 1] + 1)..(offset[my_rank]) are copied
     * The offset values range from 0..Manager::GetNumElements().
     * The number of given offsets must be equal to the net::Group::num_workers() * workers_per_connection_.
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
        typename File::Reader reader = source.GetReader();

        std::vector<BlockWriter> writers = OpenWriters();

        for (size_t local_worker_id = 0; local_worker_id < num_workers(); ++local_worker_id) {
            // write [current,limit) to this worker
            size_t limit = offsets[local_worker_id];
            assert(current <= limit);
#if 0
            for ( ; current < limit; ++current) {
                assert(reader.HasNext());
                // move over one item (with deserialization and serialization)
                writers[local_worker_id](reader.template Next<ItemType>());
            }
#else
            if (current != limit) {
                writers[local_worker_id].AppendBlocks(
                    reader.template GetItemBatch<ItemType>(limit - current));
                current = limit;
            }
#endif
            writers[local_worker_id].Close();
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

        // close self-loop queues
        for (size_t my_worker = 0; my_worker < workers_per_connection_; my_worker++) {
            size_t local_worker_id = group_.my_connection_id() + my_worker;
            if (!queues_[local_worker_id].write_closed())
                queues_[local_worker_id].Close();
        }

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

    void CallClosedCallbacksEventually() {
        if (closed()) {
            for (const auto& cb : closed_callbacks_)
                cb();
            closed_callbacks_.clear();
        }
    }

    //! Indicates if the channel is closed - meaning all remite streams have
    //! been closed. This does *not* include the loopback stream
    bool closed() const {
        bool closed = true;
        for (auto& q : queues_) {
            closed = closed && q.write_closed();
        }
        return closed;
    }

    //! Adds a Callback that is called when the channel is closed (r+w)
    void OnClose(ClosedCallback cb) {
        closed_callbacks_.push_back(cb);
    }

    ///////// expse these members - getters would be too java-ish /////////////
    //! StatsCounter for incoming data transfer
    //! Do not include loopback data transfer
    StatsCounter incoming_bytes_, incoming_blocks_;

    //! StatsCounters for outgoing data transfer - shared by all sinks
    //! Do not include loopback data transfer
    StatsCounter outgoing_bytes_, outgoing_blocks_;

    //! Timers from creation of channel until rx / tx direction is closed.
    StatsTimer tx_lifetime_, rx_lifetime_;

    //! Timers from first rx / tx package until rx / tx direction is closed.
    StatsTimer tx_timespan_, rx_timespan_;
    ///////////////////////////////////////////////////////////////////////////

protected:
    static const bool debug = false;

    ChannelId id_;

    //! ChannelSink objects are receivers of Blocks outbound for other worker.
    std::vector<ChannelSink> sinks_;

    //! BlockQueues to store incoming Blocks with no attached destination.
    std::vector<BlockQueue> queues_;

    //! Vector of Files to cache inbound Blocks needed for OpenCachingReader().
    std::vector<File> cache_files_;

    net::Group& group_;
    net::DispatcherThread& dispatcher_;

    size_t my_local_worker_id_;

    size_t workers_per_connection_;

    //! number of expected / received stream closing operations. Required to know when to
    //! stop rx_lifetime
    size_t expected_closing_blocks_, received_closing_blocks_;

    //! Callbacks that are called once when the channel is closed (r+w)
    std::vector<ClosedCallback> closed_callbacks_;

    friend class Multiplexer;

    size_t num_workers() const {
        return group_.num_connections() * workers_per_connection_;
    }

    //! called from Multiplexer when there is a new Block on a
    //! Channel.
    //! \param from the worker rank (host rank * num_workers/host + worker id)
    void OnChannelBlock(size_t from, Block&& b) {
        assert(from < queues_.size());
        rx_timespan_.StartEventually();
        incoming_bytes_ += b.size();
        incoming_blocks_++;

        sLOG << "OnChannelBlock" << b;

        if (debug) {
            sLOG << "channel" << id_ << "receive from" << from << ":"
                 << common::hexdump(b.ToString());
        }

        queues_[from].AppendBlock(b);
    }

    //! called from Multiplexer when a Channel closed notification was
    //! received.
    //! \param from the worker rank (host rank * num_workers/host + worker id)
    void OnCloseChannel(size_t from) {
        assert(from < queues_.size());
        assert(!queues_[from].write_closed());
        queues_[from].Close();
        if (expected_closing_blocks_ == ++received_closing_blocks_) {
            rx_lifetime_.StopEventually();
            rx_timespan_.StopEventually();
            CallClosedCallbacksEventually();
        }
    }
};

using ChannelPtr = std::shared_ptr<Channel>;

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHANNEL_HEADER

/******************************************************************************/
