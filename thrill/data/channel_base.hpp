/*******************************************************************************
 * thrill/data/channel_base.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_CHANNEL_BASE_HEADER
#define THRILL_DATA_CHANNEL_BASE_HEADER

#include <thrill/common/stats_counter.hpp>
#include <thrill/common/stats_timer.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>

#include <mutex>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

using ChannelId = size_t;

/*!
 * Base class for common structures for ConcatChannel and MixedChannel. This is
 * also a virtual base class use by Multiplexer to pass blocks to channels!
 * Instead, it contains common items like stats.
 */
class ChannelBase
{
public:
    using StatsCounter = common::StatsCounter<size_t, common::g_enable_stats>;
    using StatsTimer = common::StatsTimer<common::g_enable_stats>;

    using ClosedCallback = std::function<void()>;

    using Writer = DynBlockWriter;

    ChannelBase(Multiplexer& multiplexer, const ChannelId& id,
                size_t my_local_worker_id)
        : tx_lifetime_(true), rx_lifetime_(true),
          tx_timespan_(), rx_timespan_(),
          id_(id),
          my_local_worker_id_(my_local_worker_id),
          multiplexer_(multiplexer),
          expected_closing_blocks_(
              (multiplexer_.num_hosts() - 1) * multiplexer_.num_workers_per_host()),
          received_closing_blocks_(0) { }

    virtual ~ChannelBase() { }

    const ChannelId & id() const {
        return id_;
    }

    void CallClosedCallbacksEventually() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (closed()) {
            for (const auto& cb : closed_callbacks_)
                cb();
            closed_callbacks_.clear();
        }
    }

    //! Adds a Callback that is called when the channel is closed (r+w)
    void OnClose(const ClosedCallback& cb) {
        closed_callbacks_.push_back(cb);
    }

    virtual bool closed() const = 0;

    ///////// expose these members - getters would be too java-ish /////////////

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
    //! our own channel id.
    ChannelId id_;

    size_t my_local_worker_id_;

    //! reference to multiplexer
    Multiplexer& multiplexer_;

    //! number of expected / received stream closing operations. Required to
    //! know when to stop rx_lifetime
    size_t expected_closing_blocks_, received_closing_blocks_;

    //! Callbacks that are called once when the channel is closed (r+w)
    std::vector<ClosedCallback> closed_callbacks_;

    // protects against race conditions in closed_callbacks_ loop
    std::mutex mutex_;
};

using ChannelBasePtr = std::shared_ptr<ChannelBase>;

/*!
 * Base class for ChannelSet.
 */
class ChannelSetBase
{
public:
    virtual ~ChannelSetBase() { }

    //! Close all channels in the set.
    virtual void Close() = 0;
};

/*!
 * Simple structure that holds a all channel instances for the workers on the
 * local host for a given channel id.
 */
template <typename Channel>
class ChannelSet : public ChannelSetBase
{
public:
    using ChannelPtr = std::shared_ptr<Channel>;

    //! Creates a ChannelSet with the given number of channels (num workers per
    //! host).
    ChannelSet(data::Multiplexer& multiplexer, ChannelId id,
               size_t num_workers_per_host) {
        for (size_t i = 0; i < num_workers_per_host; i++)
            channels_.push_back(std::make_shared<Channel>(multiplexer, id, i));
    }

    //! Returns the channel that will be consumed by the worker with the given
    //! local id
    ChannelPtr peer(size_t local_worker_id) {
        assert(local_worker_id < channels_.size());
        return channels_[local_worker_id];
    }

    void Close() final {
        for (auto& c : channels_)
            c->Close();
    }

private:
    //! 'owns' all channels belonging to one channel id for all local workers.
    std::vector<ChannelPtr> channels_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_CHANNEL_BASE_HEADER

/******************************************************************************/
