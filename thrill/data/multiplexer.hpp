/*******************************************************************************
 * thrill/data/multiplexer.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_MULTIPLEXER_HEADER
#define THRILL_DATA_MULTIPLEXER_HEADER

#include <thrill/common/json_logger.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/group.hpp>

#include <algorithm>
#include <atomic>
#include <memory>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

class StreamSetBase;

template <typename Stream>
class StreamSet;

class CatStream;
using CatStreamPtr = std::shared_ptr<CatStream>;
using CatStreamSet = StreamSet<CatStream>;
using CatStreamSetPtr = std::shared_ptr<CatStreamSet>;

class MixStream;
using MixStreamPtr = std::shared_ptr<MixStream>;
using MixStreamSet = StreamSet<MixStream>;
using MixStreamSetPtr = std::shared_ptr<MixStreamSet>;

class BlockQueue;
class MixBlockQueueSink;

struct StreamBlockHeader;

/*!
 * Multiplexes virtual Connections on Dispatcher.
 *
 * A worker as a TCP conneciton to each other worker to exchange large amounts
 * of data. Since multiple exchanges can occur at the same time on this single
 * connection we use multiplexing. The slices are called Blocks and are
 * indicated by a \ref StreamBlockHeader. Multiple Blocks form a Stream on a
 * single TCP connection. The multiplexer multiplexes all streams on all
 * sockets.
 *
 * All sockets are polled for headers. As soon as the a header arrives it is
 * either attached to an existing stream or a new stream instance is
 * created.
 */
class Multiplexer
{
    static constexpr bool debug = false;

public:
    Multiplexer(mem::Manager& mem_manager,
                data::BlockPool& block_pool,
                size_t workers_per_host, net::Group& group);

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
        return num_hosts() * workers_per_host_;
    }

    //! number of workers per host
    size_t workers_per_host() const {
        return workers_per_host_;
    }

    //! Get the used BlockPool
    BlockPool& block_pool() { return block_pool_; }

    //! Get the JsonLogger from the BlockPool
    common::JsonLogger& logger();

    //! \name CatStream
    //! \{

    //! Allocate the next stream
    size_t AllocateCatStreamId(size_t local_worker_id);

    //! Get stream with given id, if it does not exist, create it.
    CatStreamPtr GetOrCreateCatStream(
        size_t id, size_t local_worker_id, size_t dia_id);

    //! Request next stream.
    CatStreamPtr GetNewCatStream(size_t local_worker_id, size_t dia_id);

    //! \}

    //! \name MixStream
    //! \{

    //! Allocate the next stream
    size_t AllocateMixStreamId(size_t local_worker_id);

    //! Get stream with given id, if it does not exist, create it.
    MixStreamPtr GetOrCreateMixStream(
        size_t id, size_t local_worker_id, size_t dia_id);

    //! Request next stream.
    MixStreamPtr GetNewMixStream(size_t local_worker_id, size_t dia_id);

    //! \}

private:
    //! reference to host-global memory manager
    mem::Manager& mem_manager_;

    //! reference to host-global BlockPool.
    data::BlockPool& block_pool_;

    //! dispatcher used for all communication by data::Multiplexer, the thread
    //! never leaves the data components!
    net::DispatcherThread dispatcher_;

    // Holds NetConnections for outgoing Streams
    net::Group& group_;

    //! Number of workers per host
    size_t workers_per_host_;

    //! protects critical sections
    std::mutex mutex_;

    //! friends for access to network components
    friend class CatStream;
    friend class MixStream;
    friend class StreamSink;

    //! Pointer to queue that is used for communication between two workers on
    //! the same host.
    BlockQueue * CatLoopback(
        size_t stream_id, size_t from_worker_id, size_t to_worker_id);

    MixBlockQueueSink * MixLoopback(
        size_t stream_id, size_t from_worker_id, size_t to_worker_id);

    /**************************************************************************/

    //! pimpl data structure
    struct Data;

    //! pimpl data structure
    std::unique_ptr<Data> d_;

    CatStreamPtr IntGetOrCreateCatStream(
        size_t id, size_t local_worker_id, size_t dia_id);
    MixStreamPtr IntGetOrCreateMixStream(
        size_t id, size_t local_worker_id, size_t dia_id);

    /**************************************************************************/

    using Connection = net::Connection;

    //! expects the next BlockHeader from a socket and passes to OnBlockHeader
    void AsyncReadBlockHeader(Connection& s);

    //! parses BlockHeader and decides whether to receive Block or close Stream
    void OnBlockHeader(Connection& s, net::Buffer&& buffer);

    //! Receives and dispatches a Block to a CatStream
    void OnCatStreamBlock(
        Connection& s, const StreamBlockHeader& header,
        const CatStreamPtr& stream, PinnedByteBlockPtr&& bytes);

    //! Receives and dispatches a Block to a MixStream
    void OnMixStreamBlock(
        Connection& s, const StreamBlockHeader& header,
        const MixStreamPtr& stream, PinnedByteBlockPtr&& bytes);
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MULTIPLEXER_HEADER

/******************************************************************************/
