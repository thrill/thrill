/*******************************************************************************
 * thrill/data/multiplexer.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
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

class Channel;
using ChannelPtr = std::shared_ptr<Channel>;

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
    explicit Multiplexer(data::BlockPool& block_pool,
                         size_t num_workers_per_host, net::Group& group)
        : block_pool_(block_pool),
          dispatcher_("multiplexer"),
          group_(group),
          num_workers_per_host_(num_workers_per_host),
          channels_(num_workers_per_host) {
        for (size_t id = 0; id < group_.num_hosts(); id++) {
            if (id == group_.my_host_rank()) continue;
            AsyncReadChannelBlockHeader(group_.connection(id));
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

    //! Get the used BlockPool
    BlockPool & block_pool() { return block_pool_; }

    //! Allocate the next channel
    size_t AllocateChannelId(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return channels_.AllocateId(local_worker_id);
    }

    //! Get channel with given id, if it does not exist, create it.
    ChannelPtr GetOrCreateChannel(size_t id, size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return _GetOrCreateChannel(id, local_worker_id);
    }

    //! Request next channel.
    ChannelPtr GetNewChannel(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return _GetOrCreateChannel(
            channels_.AllocateId(local_worker_id), local_worker_id);
    }

private:
    static const bool debug = false;

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
    friend class Channel;

    /**************************************************************************/

    //! Channels have an ID in block headers. (worker id, channel id)
    Repository<Channel> channels_;

    ChannelPtr _GetOrCreateChannel(size_t id, size_t local_worker_id);

    /**************************************************************************/

    using Connection = net::Connection;

    //! expects the next ChannelBlockHeader from a socket and passes to
    //! OnChannelBlockHeader
    void AsyncReadChannelBlockHeader(Connection& s);

    void OnChannelBlockHeader(Connection& s, net::Buffer&& buffer);

    void OnChannelBlock(
        Connection& s, const ChannelBlockHeader& header, const ChannelPtr& channel,
        const ByteBlockPtr& bytes);
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MULTIPLEXER_HEADER

/******************************************************************************/
