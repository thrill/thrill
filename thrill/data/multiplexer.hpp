/*******************************************************************************
 * thrill/data/multiplexer.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_MULTIPLEXER_HEADER
#define THRILL_DATA_MULTIPLEXER_HEADER

#include <thrill/data/repository.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/group.hpp>

#include <algorithm>
#include <atomic>
#include <memory>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

class ChannelSetBase;

template <typename Channel>
class ChannelSet;

class CatChannel;
using CatChannelPtr = std::shared_ptr<CatChannel>;
using CatChannelSet = ChannelSet<CatChannel>;
using CatChannelSetPtr = std::shared_ptr<CatChannelSet>;

class MixChannel;
using MixChannelPtr = std::shared_ptr<MixChannel>;
using MixChannelSet = ChannelSet<MixChannel>;
using MixChannelSetPtr = std::shared_ptr<MixChannelSet>;

class BlockQueue;
class MixBlockQueueSink;

struct ChannelBlockHeader;

/*!
 * Multiplexes virtual Connections on Dispatcher.
 *
 * A worker as a TCP conneciton to each other worker to exchange large amounts
 * of data. Since multiple exchanges can occur at the same time on this single
 * connection we use multiplexing. The slices are called Blocks and are
 * indicated by a \ref ChannelBlockHeader. Multiple Blocks form a Channel on a
 * single TCP connection. The multiplexer multiplexes all streams on all
 * sockets.
 *
 * All sockets are polled for headers. As soon as the a header arrives it is
 * either attached to an existing channel or a new channel instance is
 * created.
 */
class Multiplexer
{
public:
    explicit Multiplexer(mem::Manager& mem_manager,
                         data::BlockPool& block_pool,
                         size_t num_workers_per_host, net::Group& group)
        : mem_manager_(mem_manager),
          block_pool_(block_pool),
          dispatcher_(mem_manager, group, "multiplexer"),
          group_(group),
          num_workers_per_host_(num_workers_per_host),
          channel_sets_(num_workers_per_host) {
        for (size_t id = 0; id < group_.num_hosts(); id++) {
            if (id == group_.my_host_rank()) continue;
            AsyncReadBlockHeader(group_.connection(id));
        }
    }

    //! non-copyable: delete copy-constructor
    Multiplexer(const Multiplexer&) = delete;
    //! non-copyable: delete assignment operator
    Multiplexer& operator = (const Multiplexer&) = delete;

    //! Closes all client connections
    ~Multiplexer();

    //! total number of hosts.
    size_t num_hosts() const {
        return group_.num_hosts();
    }

    //! my rank among the hosts.
    size_t my_host_rank() const {
        return group_.my_host_rank();
    }

    //! total number of workers.
    size_t num_workers() const {
        return num_hosts() * num_workers_per_host_;
    }

    //! number of workers per host
    size_t num_workers_per_host() const {
        return num_workers_per_host_;
    }

    //! Get the used BlockPool
    BlockPool & block_pool() { return block_pool_; }

    //! \name CatChannel
    //! \{

    //! Allocate the next channel
    size_t AllocateCatChannelId(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return channel_sets_.AllocateId(local_worker_id);
    }

    //! Get channel with given id, if it does not exist, create it.
    CatChannelPtr GetOrCreateCatChannel(size_t id, size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return _GetOrCreateCatChannel(id, local_worker_id);
    }

    //! Request next channel.
    CatChannelPtr GetNewCatChannel(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return _GetOrCreateCatChannel(
            channel_sets_.AllocateId(local_worker_id), local_worker_id);
    }

    //! \}

    //! \name MixChannel
    //! \{

    //! Allocate the next channel
    size_t AllocateMixChannelId(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return channel_sets_.AllocateId(local_worker_id);
    }

    //! Get channel with given id, if it does not exist, create it.
    MixChannelPtr GetOrCreateMixChannel(size_t id, size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return _GetOrCreateMixChannel(id, local_worker_id);
    }

    //! Request next channel.
    MixChannelPtr GetNewMixChannel(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return _GetOrCreateMixChannel(
            channel_sets_.AllocateId(local_worker_id), local_worker_id);
    }

    //! \}

protected:
    static const bool debug = false;

    //! reference to host-global memory manager
    mem::Manager& mem_manager_;

    //! reference to host-global BlockPool.
    data::BlockPool& block_pool_;

    //! dispatcher used for all communication by data::Multiplexer, the thread
    //! never leaves the data components!
    net::DispatcherThread dispatcher_;

    // Holds NetConnections for outgoing Channels
    net::Group& group_;

    //! Number of workers per host
    size_t num_workers_per_host_;

    //! protects critical sections
    std::mutex mutex_;

    //! friends for access to network components
    friend class CatChannel;
    friend class MixChannel;

    //! Pointer to queue that is used for communication between two workers on
    //! the same host.
    BlockQueue * CatLoopback(
        size_t channel_id, size_t from_worker_id, size_t to_worker_id);

    MixBlockQueueSink * MixLoopback(
        size_t channel_id, size_t from_worker_id, size_t to_worker_id);

    /**************************************************************************/

    //! Channels have an ID in block headers. (worker id, channel id)
    Repository<ChannelSetBase> channel_sets_;

    CatChannelPtr _GetOrCreateCatChannel(size_t id, size_t local_worker_id);
    MixChannelPtr _GetOrCreateMixChannel(size_t id, size_t local_worker_id);

    /**************************************************************************/

    using Connection = net::Connection;

    //! expects the next BlockHeader from a socket and passes to OnBlockHeader
    void AsyncReadBlockHeader(Connection& s);

    //! parses BlockHeader and decides whether to receive Block or close Channel
    void OnBlockHeader(Connection& s, net::Buffer&& buffer);

    //! Receives and dispatches a Block to a CatChannel
    void OnCatChannelBlock(
        Connection& s, const ChannelBlockHeader& header,
        const CatChannelPtr& channel, const ByteBlockPtr& bytes);

    //! Receives and dispatches a Block to a MixChannel
    void OnMixChannelBlock(
        Connection& s, const ChannelBlockHeader& header,
        const MixChannelPtr& channel, const ByteBlockPtr& bytes);
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MULTIPLEXER_HEADER

/******************************************************************************/
