/*******************************************************************************
 * thrill/data/stream.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_STREAM_HEADER
#define THRILL_DATA_STREAM_HEADER

#include <thrill/common/semaphore.hpp>
#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/multiplexer.hpp>

#include <mutex>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

using StreamId = size_t;

enum class MagicByte : uint8_t {
    Invalid, CatStreamBlock, MixStreamBlock, PartitionBlock
};

/*!
 * Base class for common structures for ConcatStream and MixedStream. This is
 * also a virtual base class use by Multiplexer to pass blocks to streams!
 * Instead, it contains common items like stats.
 */
class StreamData : public tlx::ReferenceCounter
{
public:
    using Writer = DynBlockWriter;

    StreamData(Multiplexer& multiplexer, const StreamId& id,
               size_t local_worker_id, size_t dia_id);

    virtual ~StreamData();

    //! Return stream id
    const StreamId& id() const { return id_; }

    //! Returns my_host_rank
    size_t my_host_rank() const { return multiplexer_.my_host_rank(); }
    //! Number of hosts in system
    size_t num_hosts() const { return multiplexer_.num_hosts(); }
    //! Number of workers in system
    size_t num_workers() const { return multiplexer_.num_workers(); }

    //! Returns workers_per_host
    size_t workers_per_host() const { return multiplexer_.workers_per_host(); }
    //! Returns my_worker_rank_
    size_t my_worker_rank() const {
        return my_host_rank() * workers_per_host() + local_worker_id_;
    }

    void OnAllClosed(const char* stream_type);

    //! shuts the stream down.
    virtual void Close() = 0;

    virtual bool closed() const = 0;

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    virtual std::vector<Writer> GetWriters() = 0;

    ///////// expose these members - getters would be too java-ish /////////////

    //! StatsCounter for incoming data transfer.  Does not include loopback data
    //! transfer
    size_t rx_net_items_ = 0, rx_net_bytes_ = 0, rx_net_blocks_ = 0;

    //! StatsCounters for outgoing data transfer - shared by all sinks.  Does
    //! not include loopback data transfer
    std::atomic<size_t>
    tx_net_items_ { 0 }, tx_net_bytes_ { 0 }, tx_net_blocks_ { 0 };

    //! StatsCounter for incoming data transfer.  Exclusively contains only
    //! loopback (internal) data transfer
    std::atomic<size_t>
    rx_int_items_ { 0 }, rx_int_bytes_ { 0 }, rx_int_blocks_ { 0 };

    //! StatsCounters for outgoing data transfer - shared by all sinks.
    //! Exclusively contains only loopback (internal) data transfer
    std::atomic<size_t>
    tx_int_items_ { 0 }, tx_int_bytes_ { 0 }, tx_int_blocks_ { 0 };

    //! Timers from creation of stream until rx / tx direction is closed.
    common::StatsTimerStart tx_lifetime_, rx_lifetime_;

    //! Timers from first rx / tx package until rx / tx direction is closed.
    common::StatsTimerStopped tx_timespan_, rx_timespan_;

    ///////////////////////////////////////////////////////////////////////////

protected:
    //! our own stream id.
    StreamId id_;

    size_t local_worker_id_;

    //! Associated DIANode id.
    size_t dia_id_;

    //! reference to multiplexer
    Multiplexer& multiplexer_;

    //! number of remaining expected stream closing operations. Required to know
    //! when to stop rx_lifetime
    size_t remaining_closing_blocks_;

    //! number of received stream closing Blocks.
    common::Semaphore sem_closing_blocks_;

    //! friends for access to multiplexer_
    friend class StreamSink;
};

using StreamDataPtr = tlx::CountingPtr<StreamData>;

/*!
 * Base class for StreamSet.
 */
class StreamSetBase : public tlx::ReferenceCounter
{
public:
    virtual ~StreamSetBase() { }

    //! Close all streams in the set.
    virtual void Close() = 0;
};

/*!
 * Simple structure that holds a all stream instances for the workers on the
 * local host for a given stream id.
 */
template <typename StreamData>
class StreamSet : public StreamSetBase
{
public:
    using StreamDataPtr = tlx::CountingPtr<StreamData>;

    //! Creates a StreamSet with the given number of streams (num workers per
    //! host).
    StreamSet(Multiplexer& multiplexer, StreamId id,
              size_t workers_per_host, size_t dia_id) {
        for (size_t i = 0; i < workers_per_host; ++i) {
            streams_.emplace_back(
                tlx::make_counting<StreamData>(multiplexer, id, i, dia_id));
        }
        remaining_ = workers_per_host;
    }

    //! Returns the stream that will be consumed by the worker with the given
    //! local id
    StreamDataPtr Peer(size_t local_worker_id) {
        assert(local_worker_id < streams_.size());
        return streams_[local_worker_id];
    }

    //! Release local_worker_id, returns true when all individual streams are
    //! done.
    bool Release(size_t local_worker_id) {
        assert(local_worker_id < streams_.size());
        if (streams_[local_worker_id]) {
            assert(remaining_ > 0);
            streams_[local_worker_id].reset();
            --remaining_;
        }
        return (remaining_ == 0);
    }

    void Close() final {
        for (StreamDataPtr& c : streams_)
            c->Close();
    }

private:
    //! 'owns' all streams belonging to one stream id for all local workers.
    std::vector<StreamDataPtr> streams_;
    //! countdown to destruction
    size_t remaining_;
};

/******************************************************************************/
//! Stream - base class for CatStream and MixStream

class Stream : public tlx::ReferenceCounter
{
public:
    using Writer = DynBlockWriter;

    virtual ~Stream();

    //! Return stream id
    virtual const StreamId& id() const = 0;

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    virtual std::vector<Writer> GetWriters() = 0;

    /*!
     * Scatters a File to many worker: elements from [offset[0],offset[1]) are
     * sent to the first worker, elements from [offset[1], offset[2]) are sent
     * to the second worker, ..., elements from [offset[my_rank -
     * 1],offset[my_rank]) are copied locally, ..., elements from
     * [offset[num_workers - 1], offset[num_workers]) are sent to the last
     * worker.
     *
     * The number of given offsets must be equal to the
     * net::Group::num_hosts() * workers_per_host_ + 1.
     *
     * /param source File containing the data to be scattered.
     *
     * /param offsets - as described above. offsets.size must be equal to
     * num_workers + 1
     */
    template <typename ItemType>
    void Scatter(File& source, const std::vector<size_t>& offsets,
                 bool consume = false) {
        // tx_timespan_.StartEventually();

        File::Reader reader = source.GetReader(consume);
        size_t current = 0;

        {
            // discard first items in Reader
            size_t limit = offsets[0];
#if 0
            for ( ; current < limit; ++current) {
                assert(reader.HasNext());
                // discard one item (with deserialization)
                reader.template Next<ItemType>();
            }
#else
            if (current != limit) {
                reader.template GetItemBatch<ItemType>(limit - current);
                current = limit;
            }
#endif
        }

        std::vector<Writer> writers = GetWriters();

        size_t num_workers = writers.size();
        assert(offsets.size() == num_workers + 1);

        for (size_t worker = 0; worker < num_workers; ++worker) {
            // write [current,limit) to this worker
            size_t limit = offsets[worker + 1];
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

        // tx_timespan_.Stop();
    }
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_STREAM_HEADER

/******************************************************************************/
