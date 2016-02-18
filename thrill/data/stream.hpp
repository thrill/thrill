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

#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>

#include <mutex>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

using StreamId = size_t;

/*!
 * Base class for common structures for ConcatStream and MixedStream. This is
 * also a virtual base class use by Multiplexer to pass blocks to streams!
 * Instead, it contains common items like stats.
 */
class Stream
{
public:
    using StatsTimer = common::StatsTimer<common::g_enable_stats>;

    using ClosedCallback = std::function<void()>;

    using Writer = DynBlockWriter;

    Stream(Multiplexer& multiplexer, const StreamId& id,
           size_t my_local_worker_id)
        : tx_lifetime_(true), rx_lifetime_(true),
          tx_timespan_(), rx_timespan_(),
          id_(id),
          my_local_worker_id_(my_local_worker_id),
          multiplexer_(multiplexer),
          expected_closing_blocks_(
              (multiplexer_.num_hosts() - 1) * multiplexer_.num_workers_per_host()),
          received_closing_blocks_(0) { }

    virtual ~Stream() { }

    const StreamId & id() const {
        return id_;
    }

    void CallClosedCallbacksEventually() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (closed()) {
            for (const auto& cb : closed_callbacks_)
                cb();
            closed_callbacks_.clear();
        }
    }

    //! shuts the stream down.
    virtual void Close() = 0;

    //! Adds a Callback that is called when the stream is closed (r+w)
    void OnClose(const ClosedCallback& cb) {
        closed_callbacks_.push_back(cb);
    }

    virtual bool closed() const = 0;

    //! Creates BlockWriters for each worker. BlockWriter can only be opened
    //! once, otherwise the block sequence is incorrectly interleaved!
    virtual std::vector<Writer>
    OpenWriters(size_t block_size = default_block_size) = 0;

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

    ///////// expose these members - getters would be too java-ish /////////////

    //! StatsCounter for incoming data transfer
    //! Do not include loopback data transfer
    size_t incoming_bytes_, incoming_blocks_;

    //! StatsCounters for outgoing data transfer - shared by all sinks
    //! Do not include loopback data transfer
    size_t outgoing_bytes_, outgoing_blocks_;

    //! Timers from creation of stream until rx / tx direction is closed.
    StatsTimer tx_lifetime_, rx_lifetime_;

    //! Timers from first rx / tx package until rx / tx direction is closed.
    StatsTimer tx_timespan_, rx_timespan_;

    ///////////////////////////////////////////////////////////////////////////

protected:
    //! our own stream id.
    StreamId id_;

    size_t my_local_worker_id_;

    //! reference to multiplexer
    Multiplexer& multiplexer_;

    //! number of expected / received stream closing operations. Required to
    //! know when to stop rx_lifetime
    size_t expected_closing_blocks_, received_closing_blocks_;

    //! Callbacks that are called once when the stream is closed (r+w)
    std::vector<ClosedCallback> closed_callbacks_;

    //! protects against race conditions in closed_callbacks_ loop
    std::mutex mutex_;

    //! friends for access to multiplexer_
    friend class StreamSink;
};

using StreamPtr = std::shared_ptr<Stream>;

/*!
 * Base class for StreamSet.
 */
class StreamSetBase
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
template <typename Stream>
class StreamSet : public StreamSetBase
{
public:
    using StreamPtr = std::shared_ptr<Stream>;

    //! Creates a StreamSet with the given number of streams (num workers per
    //! host).
    StreamSet(data::Multiplexer& multiplexer, StreamId id,
              size_t num_workers_per_host) {
        for (size_t i = 0; i < num_workers_per_host; i++)
            streams_.push_back(std::make_shared<Stream>(multiplexer, id, i));
    }

    //! Returns the stream that will be consumed by the worker with the given
    //! local id
    StreamPtr peer(size_t local_worker_id) {
        assert(local_worker_id < streams_.size());
        return streams_[local_worker_id];
    }

    void Close() final {
        for (StreamPtr& c : streams_)
            c->Close();
    }

private:
    //! 'owns' all streams belonging to one stream id for all local workers.
    std::vector<StreamPtr> streams_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_STREAM_HEADER

/******************************************************************************/
