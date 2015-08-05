/*******************************************************************************
 * c7a/data/channel.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_CHANNEL_HEADER
#define C7A_DATA_CHANNEL_HEADER

#include <c7a/data/block_queue.hpp>
#include <c7a/data/channel_sink.hpp>
#include <c7a/data/concat_block_source.hpp>
#include <c7a/data/file.hpp>
#include <c7a/data/stream_block_header.hpp>
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
 * A Channel is a virtual set of connections to all other nodes instances,
 * hence a "Channel" bundles them to a logical communication context. We call an
 * individual connection from a node to another node a "Stream", though no
 * such class exists.
 *
 * To use a Channel, one can get a vector of BlockWriter via OpenWriters() of
 * outbound Stream. The vector is of size nodes, including virtual
 * connections to the local node(s). One can then write items destined to the
 * corresponding node. The written items are buffered into a Block and only
 * sent when the Block is full. To force a send, use BlockWriter::Flush(). When
 * all items are sent, the BlockWriters **must** be closed using
 * BlockWriter::Close().
 *
 * To read the inbound Stream items, one can get a vector of BlockReader via
 * OpenReaders(), which can then be used to read items sent by individual
 * nodes.
 *
 * Alternatively, one can use OpenReader() to get a BlockReader which delivers
 * all items from *all* nodes in node order (concatenating all inbound
 * Streams).
 *
 * As soon as all attached streams of the Channel have been Close()the number of
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

    //! Creates a new channel instance
    Channel(const ChannelId& id, net::Group& group,
            net::DispatcherThread& dispatcher, size_t my_worker_id, size_t workers_per_connection)
        : id_(id),
          queues_(group.num_connections()),
          cache_files_(group.num_connections()),
          group_(group),
          dispatcher_(dispatcher),
          my_worker_id_(my_worker_id),
          workers_per_connection_(workers_per_connection) {
        // construct ChannelSink array
        for (size_t node = 0; node < group_.num_connections(); ++node) {
            for (size_t worker = 0; worker < workers_per_connection_; worker++) {
                if (node == group_.my_connection_id()) {
                    sinks_.emplace_back();
                }
                else {
                    size_t partner_worker_id = node * workers_per_connection_ + worker;
                    sinks_.emplace_back(
                        &dispatcher, &group_.connection(node),
                        id,
                        group_.my_connection_id(),
                        my_worker_id_,
                        partner_worker_id);
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

    //! Creates BlockWriters for each node. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<BlockWriter> OpenWriters(size_t block_size = default_block_size) {
        std::vector<BlockWriter> result;

        for (size_t node_id = 0; node_id < group_.num_connections(); ++node_id) {
            for (size_t worker_id = 0; worker_id < workers_per_connection_; ++worker_id) {
                size_t queue_id = node_id * workers_per_connection_ + worker_id;
                if (node_id == group_.my_connection_id()) {
                    result.emplace_back(&queues_[queue_id], block_size);
                }
                else {
                    result.emplace_back(&sinks_[queue_id], block_size);
                }
            }
        }

        assert(result.size() == group_.num_connections() * workers_per_connection_);
        return result;
    }

    //! Creates a BlockReader for each node. The BlockReaders are attached to
    //! the BlockQueues in the Channel and wait for further Blocks to arrive or
    //! the Channel's remote close.
    std::vector<BlockQueueReader> OpenReaders() {
        std::vector<BlockQueueReader> result;

        for (size_t worker_id = 0; worker_id < workers_per_connection_; ++worker_id) {
            result.emplace_back(BlockQueueSource(queues_[worker_id]));
        }

        assert(result.size() == group_.num_connections() * workers_per_connection_);
        return result;
    }

    //! Creates a BlockReader for all nodes. The BlockReader is attached to
    //! one \ref ConcatBlockSource which includes all incoming queues of
    //! this channel.
    ConcatBlockReader OpenReader() {
        // construct vector of BlockQueueSources to read from queues_.
        std::vector<BlockQueueSource> result;
        for (size_t worker_id = 0; worker_id < num_workers(); ++worker_id) {
            result.emplace_back(queues_[worker_id]);
        }
        // move BlockQueueSources into concatenation BlockSource, and to Reader.
        return ConcatBlockReader(ConcatBlockSource(result));
    }

    //! Creates a BlockReader for all nodes. The BlockReader is attached to
    //! one \ref ConcatBlockSource which includes all incoming queues of this
    //! channel. The received Blocks are also cached in the Channel, hence this
    //! function can be called multiple times to read the items again.
    CachingConcatBlockReader OpenCachingReader() {
        // construct vector of CachingBlockQueueSources to read from queues_.
        std::vector<CachingBlockQueueSource> result;
        for (size_t worker_id = 0; worker_id < num_workers(); ++worker_id) {
            result.emplace_back(queues_[worker_id], cache_files_[worker_id]);
        }
        // move CachingBlockQueueSources into concatenation BlockSource, and to
        // Reader.
        return CachingConcatBlockReader(CachingConcatBlockSource(result));
    }

    /*!
     * Scatters a File to many nodes
     *
     * elements from 0..offset[0] are sent to the first node,
     * elements from (offset[0] + 1)..offset[1] are sent to the second node.
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
        // current item offset in Reader
        size_t current = 0;
        typename File::Reader reader = source.GetReader();

        std::vector<BlockWriter> writers = OpenWriters();

        for (size_t worker_id = 0; worker_id < num_workers(); ++worker_id) {
            // write [current,limit) to this node
            size_t limit = offsets[worker_id];
            assert(current <= limit);
#if 0
            for ( ; current < limit; ++current) {
                assert(reader.HasNext());
                // move over one item (with deserialization and serialization)
                writers[worker_id](reader.template Next<ItemType>());
            }
#else
            if (current != limit) {
                writers[worker_id].AppendBlocks(
                    reader.template GetItemBatch<ItemType>(limit - current));
                current = limit;
            }
#endif
            writers[worker_id].Close();
        }
    }

    //! shuts the channel down.
    void Close() {
        // close all sinks, this should emit sentinel to all other nodes.
        for (size_t i = 0; i != sinks_.size(); ++i) {
            if (sinks_[i].closed()) continue;
            sinks_[i].Close();
        }

        // close self-loop queues
        for (size_t my_worker = 0; my_worker < workers_per_connection_; my_worker++) {
            size_t worker_id = group_.my_connection_id() + my_worker;
            if (!queues_[worker_id].write_closed())
                queues_[worker_id].Close();
        }

        // wait for close packets to arrive (this is a busy waiting loop, try to
        // do it better -tb)
        for (size_t i = 0; i != queues_.size(); ++i) {
            while (!queues_[i].write_closed()) {
                LOG << "wait for close from node" << i;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
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

protected:
    static const bool debug = false;

    ChannelId id_;

    //! ChannelSink objects are receivers of Blocks outbound for other nodes.
    std::vector<ChannelSink> sinks_;

    //! BlockQueues to store incoming Blocks with no attached destination.
    std::vector<BlockQueue> queues_;

    //! Vector of Files to cache inbound Blocks needed for OpenCachingReader().
    std::vector<File> cache_files_;

    net::Group& group_;
    net::DispatcherThread& dispatcher_;

    size_t my_worker_id_;

    size_t workers_per_connection_;

    friend class ChannelMultiplexer;

    size_t num_workers() const {
        return group_.num_connections() * workers_per_connection_;
    }

    //! called from ChannelMultiplexer when there is a new Block on a
    //! Stream.
    //! \param from the worker rank (node rank * #workers/node + worker id)
    void OnStreamBlock(size_t from, VirtualBlock&& vb) {
        assert(from < queues_.size());

        sLOG << "OnStreamBlock" << vb;

        if (debug) {
            sLOG << "channel" << id_ << "receive from" << from << ":"
                 << common::hexdump(vb.ToString());
        }

        queues_[from].AppendBlock(vb);
    }

    //! called from ChannelMultiplexer when a Stream closed notification was
    //! received.
    //! \param from the worker rank (node rank * #workers/node + worker id)
    void OnCloseStream(size_t from) {
        assert(from < queues_.size());
        assert(!queues_[from].write_closed());
        queues_[from].Close();
    }
};

using ChannelPtr = std::shared_ptr<Channel>;

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_CHANNEL_HEADER

/******************************************************************************/
