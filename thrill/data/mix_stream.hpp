/*******************************************************************************
 * thrill/data/mix_stream.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_MIX_STREAM_HEADER
#define THRILL_DATA_MIX_STREAM_HEADER

#include <thrill/data/mix_block_queue.hpp>
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
 * outbound Stream. The vector is of size of workers in the system.  One can
 * then write items destined to the corresponding worker. The written items are
 * buffered into a Block and only sent when the Block is full. To force a send,
 * use BlockWriter::Flush(). When all items are sent, the BlockWriters **must**
 * be closed using BlockWriter::Close().
 *
 * The MixStream allows reading of items from all workers in an unordered
 * sequence, without waiting for any of the workers to complete sending items.
 */
class MixStream final : public Stream
{
public:
    using MixReader = MixBlockQueueReader;

    //! Creates a new stream instance
    MixStream(Multiplexer& multiplexer, const StreamId& id,
              size_t my_local_worker_id)
        : Stream(multiplexer, id, my_local_worker_id),
          queue_(multiplexer_.block_pool_, multiplexer_.num_workers()) {

        sinks_.reserve(multiplexer_.num_workers());
        loopback_.reserve(multiplexer_.num_workers());

        // construct StreamSink array
        for (size_t host = 0; host < multiplexer_.num_hosts(); ++host) {
            for (size_t worker = 0; worker < multiplexer_.num_workers_per_host_; worker++) {
                if (host == multiplexer_.my_host_rank()) {
                    // dummy entries
                    sinks_.emplace_back(multiplexer_.block_pool_);
                }
                else {
                    // StreamSink which transmits MIX_STREAM_BLOCKs
                    sinks_.emplace_back(
                        multiplexer_.block_pool_,
                        &multiplexer_.dispatcher_,
                        &multiplexer_.group_.connection(host),
                        MagicByte::MIX_STREAM_BLOCK,
                        id,
                        multiplexer_.my_host_rank(), my_local_worker_id, worker,
                        &outgoing_bytes_, &outgoing_blocks_, &tx_timespan_);
                }
            }
        }

        // construct MixBlockQueueSink for loopback writers
        for (size_t worker = 0; worker < multiplexer_.num_workers_per_host_; worker++) {
            loopback_.emplace_back(
                queue_,
                multiplexer_.my_host_rank() * multiplexer_.num_workers_per_host() + worker);
        }
    }

    //! non-copyable: delete copy-constructor
    MixStream(const MixStream&) = delete;
    //! non-copyable: delete assignment operator
    MixStream& operator = (const MixStream&) = delete;
    //! move-constructor: default
    MixStream(MixStream&&) = default;

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    std::vector<Writer>
    OpenWriters(size_t block_size = default_block_size) final {
        tx_timespan_.StartEventually();

        std::vector<Writer> result;

        for (size_t host = 0; host < multiplexer_.num_hosts(); ++host) {
            for (size_t local_worker_id = 0; local_worker_id < multiplexer_.num_workers_per_host_; ++local_worker_id) {
                if (host == multiplexer_.my_host_rank()) {
                    auto target_queue_ptr =
                        multiplexer_.MixLoopback(id_, my_local_worker_id_, local_worker_id);
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
    MixReader OpenMixReader(bool consume) {
        rx_timespan_.StartEventually();
        return MixReader(queue_, consume);
    }

    //! shuts the stream down.
    void Close() final {
        // close all sinks, this should emit sentinel to all other worker.
        for (size_t i = 0; i != sinks_.size(); ++i) {
            if (sinks_[i].closed()) continue;
            sinks_[i].Close();
        }

        // close loop-back queue from this worker to all others on this host.
        for (size_t local_worker_id = 0;
             local_worker_id < multiplexer_.num_workers_per_host(); ++local_worker_id)
        {
            auto queue_ptr = multiplexer_.MixLoopback(
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

    //! Indicates if the stream is closed - meaning all remaining streams have
    //! been closed.
    bool closed() const final {
        bool closed = true;
        closed = closed && queue_.write_closed();
        return closed;
    }

private:
    static const bool debug = false;

    //! StreamSink objects are receivers of Blocks outbound for other worker.
    std::vector<StreamSink> sinks_;

    //! BlockQueue to store incoming Blocks with source.
    MixBlockQueue queue_;

    //! vector of MixBlockQueueSink which serve as loopback BlockSinks into
    //! the MixBlockQueue
    std::vector<MixBlockQueueSink> loopback_;

    //! for calling methods to deliver blocks
    friend class Multiplexer;

    //! called from Multiplexer when there is a new Block for this Stream.
    void OnStreamBlock(size_t from, Block&& b) {
        assert(from < multiplexer_.num_workers());
        rx_timespan_.StartEventually();
        incoming_bytes_ += b.size();
        incoming_blocks_++;

        sLOG << "OnMixStreamBlock" << b;

        if (debug) {
            sLOG << "stream" << id_ << "receive from" << from << ":"
                 << common::Hexdump(b.ToString());
        }

        queue_.AppendBlock(from, b);
    }

    //! called from Multiplexer when a MixStream closed notification was
    //! received.
    void OnCloseStream(size_t from) {
        assert(from < multiplexer_.num_workers());
        queue_.Close(from);

        if (expected_closing_blocks_ == ++received_closing_blocks_) {
            rx_lifetime_.StopEventually();
            rx_timespan_.StopEventually();
            CallClosedCallbacksEventually();
        }
    }

    //! Returns the loopback queue for the worker of this stream.
    MixBlockQueueSink * loopback_queue(size_t from_worker_id) {
        assert(from_worker_id < multiplexer_.num_workers_per_host_);
        assert(from_worker_id < loopback_.size());
        sLOG0 << "expose loopback queue for" << from_worker_id;
        return &(loopback_[from_worker_id]);
    }
};

using MixStreamPtr = std::shared_ptr<MixStream>;

using MixStreamSet = StreamSet<MixStream>;
using MixStreamSetPtr = std::shared_ptr<MixStreamSet>;

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MIX_STREAM_HEADER

/******************************************************************************/
