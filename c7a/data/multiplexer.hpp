/*******************************************************************************
 * c7a/data/multiplexer.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_MULTIPLEXER_HEADER
#define C7A_DATA_MULTIPLEXER_HEADER

#include <c7a/common/atomic_movable.hpp>
#include <c7a/data/repository.hpp>
#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/net/group.hpp>

#include <algorithm>
#include <memory>

namespace c7a {
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
    explicit Multiplexer(size_t num_workers_per_host)
        : dispatcher_("multiplexer"),
          num_workers_per_host_(num_workers_per_host),
          channels_(num_workers_per_host) { }

    //! non-copyable: delete copy-constructor
    Multiplexer(const Multiplexer&) = delete;
    //! non-copyable: delete assignment operator
    Multiplexer& operator = (const Multiplexer&) = delete;
    //! default move constructor
    Multiplexer(Multiplexer&&) = default;

    //! Closes all client connections
    ~Multiplexer();

    void Connect(net::Group* group) {
        group_ = group;
        for (size_t id = 0; id < group_->num_hosts(); id++) {
            if (id == group_->my_host_rank()) continue;
            AsyncReadChannelBlockHeader(group_->connection(id));
        }
    }

    //! total number of hosts.
    size_t num_hosts() const {
        return group_->num_hosts();
    }

    //! my rank among the hosts.
    size_t my_host_rank() const {
        return group_->my_host_rank();
    }

    //! total number of workers.
    size_t num_workers() const {
        return num_hosts() * num_workers_per_host_;
    }

    //! Allocate the next channel
    size_t AllocateChannelId(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return channels_.AllocateId(local_worker_id);
    }

    //! Get channel with given id, if it does not exist, create it.
    ChannelPtr GetOrCreateChannel(size_t id, size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::move(_GetOrCreateChannel(id, local_worker_id));
    }

    //! Request next channel.
    ChannelPtr GetNewChannel(size_t local_worker_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        return std::move(
            _GetOrCreateChannel(
                channels_.AllocateId(local_worker_id), local_worker_id));
    }

private:
    static const bool debug = false;

    //! dispatcher used for all communication by data::Multiplexer, the thread
    //! never leaves the data components!
    net::DispatcherThread dispatcher_;

    // Holds NetConnections for outgoing Channels
    net::Group* group_ = nullptr;

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
} // namespace c7a

#endif // !C7A_DATA_MULTIPLEXER_HEADER

/******************************************************************************/
