/*******************************************************************************
 * thrill/data/stream_data.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_STREAM_DATA_HEADER
#define THRILL_DATA_STREAM_DATA_HEADER

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

class StreamSink;

/*!
 * Base class for common structures for ConcatStream and MixedStream. This is
 * also a virtual base class use by Multiplexer to pass blocks to streams!
 * Instead, it contains common items like stats.
 */
class StreamData : public tlx::ReferenceCounter
{
public:
    using Writer = BlockWriter<StreamSink>;

    /*!
     * An extra class derived from std::vector<> for delivery of the
     * BlockWriters of a Stream. The purpose is to enforce a custom way to close
     * stream writers cyclically such that PE k first sends it's Close-packet to
     * k+1, k+2, etc.
     */
    class Writers : public std::vector<BlockWriter<StreamSink> >
    {
    public:
        Writers(size_t my_worker_rank = 0);

        //! copyable: default copy-constructor
        Writers(const Writers&) = default;
        //! copyable: default assignment operator
        Writers& operator = (const Writers&) = default;
        //! move-constructor: default
        Writers(Writers&&) = default;
        //! move-assignment operator: default
        Writers& operator = (Writers&&) = default;

        //! custom destructor to close writers is a cyclic fashion
        void Close();

        //! custom destructor to close writers is a cyclic fashion
        ~Writers();

    private:
        //! rank of this worker
        size_t my_worker_rank_;
    };

    StreamData(Multiplexer& multiplexer, size_t send_size_limit,
               const StreamId& id, size_t local_worker_id, size_t dia_id);

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
    virtual Writers GetWriters() = 0;

    ///////// expose these members - getters would be too java-ish /////////////

    //! StatsCounter for incoming data transfer.  Does not include loopback data
    //! transfer
    std::atomic<size_t>
    rx_net_items_ { 0 }, rx_net_bytes_ { 0 }, rx_net_blocks_ { 0 };

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

    //! semaphore to stall the amount of PinnedBlocks (measured in bytes) passed
    //! to the network layer for transmission.
    common::Semaphore sem_queue_;

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
    StreamSet(Multiplexer& multiplexer, size_t send_size_limit,
              StreamId id, size_t workers_per_host, size_t dia_id) {
        for (size_t i = 0; i < workers_per_host; ++i) {
            streams_.emplace_back(
                tlx::make_counting<StreamData>(
                    multiplexer, send_size_limit, id, i, dia_id));
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

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_STREAM_DATA_HEADER

/******************************************************************************/
