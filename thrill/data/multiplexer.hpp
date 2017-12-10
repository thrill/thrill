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

class CatStreamData;
using CatStreamDataPtr = tlx::CountingPtr<CatStreamData>;
class CatStream;
using CatStreamPtr = tlx::CountingPtr<CatStream>;

class MixStreamData;
using MixStreamDataPtr = tlx::CountingPtr<MixStreamData>;
class MixStream;
using MixStreamPtr = tlx::CountingPtr<MixStream>;

using CatStreamSet = StreamSet<CatStreamData>;
using MixStreamSet = StreamSet<MixStreamData>;

class BlockQueue;
class MixBlockQueueSink;

class StreamMultiplexerHeader;

/*!
 * Multiplexes virtual Connections on Dispatcher.
 *
 * A worker as a TCP conneciton to each other worker to exchange large amounts
 * of data. Since multiple exchanges can occur at the same time on this single
 * connection we use multiplexing. The slices are called Blocks and are
 * indicated by a \ref MultiplexerHeader. Multiple Blocks form a Stream on a
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
    Multiplexer(mem::Manager& mem_manager, BlockPool& block_pool,
                net::DispatcherThread& dispatcher, net::Group& group,
                size_t workers_per_host);

    //! non-copyable: delete copy-constructor
    Multiplexer(const Multiplexer&) = delete;
    //! non-copyable: delete assignment operator
    Multiplexer& operator = (const Multiplexer&) = delete;

    //! Closes all client connections
    ~Multiplexer();

    //! Closes all client connections
    void Close();

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

    //! \name CatStreamData
    //! \{

    //! Allocate the next stream
    size_t AllocateCatStreamId(size_t local_worker_id);

    //! Get stream with given id, if it does not exist, create it.
    CatStreamDataPtr GetOrCreateCatStreamData(
        size_t id, size_t local_worker_id, size_t dia_id);

    //! Request next stream.
    CatStreamPtr GetNewCatStream(size_t local_worker_id, size_t dia_id);

    //! \}

    //! \name MixStream
    //! \{

    //! Allocate the next stream
    size_t AllocateMixStreamId(size_t local_worker_id);

    //! Get stream with given id, if it does not exist, create it.
    MixStreamDataPtr GetOrCreateMixStreamData(
        size_t id, size_t local_worker_id, size_t dia_id);

    //! Request next stream.
    MixStreamPtr GetNewMixStream(size_t local_worker_id, size_t dia_id);

    //! \}

private:
    //! reference to host-global memory manager
    mem::Manager& mem_manager_;

    //! reference to host-global BlockPool.
    BlockPool& block_pool_;

    //! dispatcher used for all communication by data::Multiplexer, the thread
    //! never leaves the data components!
    net::DispatcherThread& dispatcher_;

    // Holds NetConnections for outgoing Streams
    net::Group& group_;

    //! Number of workers per host
    size_t workers_per_host_;

    //! protects critical sections
    std::mutex mutex_;

    //! closed
    bool closed_ = false;

    //! number of parallel recv requests
    size_t num_parallel_async_;

    //! number of active Cat/MixStreams
    std::atomic<size_t> active_streams_ { 0 };

    //! maximu number of active Cat/MixStreams
    size_t max_active_streams_ = 0;

    //! friends for access to network components
    friend class CatStreamData;
    friend class MixStreamData;
    friend class StreamSink;

    //! Pointer to queue that is used for communication between two workers on
    //! the same host.
    CatStreamDataPtr CatLoopback(size_t stream_id, size_t to_worker_id);
    MixStreamDataPtr MixLoopback(size_t stream_id, size_t to_worker_id);

    /**************************************************************************/

    //! pimpl data structure
    struct Data;

    //! pimpl data structure
    std::unique_ptr<Data> d_;

    CatStreamDataPtr IntGetOrCreateCatStreamData(
        size_t id, size_t local_worker_id, size_t dia_id);
    MixStreamDataPtr IntGetOrCreateMixStreamData(
        size_t id, size_t local_worker_id, size_t dia_id);

    //! release pointer onto a CatStreamData object
    void IntReleaseCatStream(size_t id, size_t local_worker_id);
    //! release pointer onto a MixStream object
    void IntReleaseMixStream(size_t id, size_t local_worker_id);

    /**************************************************************************/

    using Connection = net::Connection;

    //! expects the next MultiplexerHeader from a socket and passes to
    //! OnMultiplexerHeader
    void AsyncReadMultiplexerHeader(size_t peer, Connection& s);

    //! parses MultiplexerHeader and decides whether to receive Block or close
    //! Stream
    void OnMultiplexerHeader(
        size_t peer, uint32_t seq, Connection& s, net::Buffer&& buffer);

    //! Receives and dispatches a Block to a CatStreamData
    void OnCatStreamBlock(
        size_t peer, Connection& s, const StreamMultiplexerHeader& header,
        const CatStreamDataPtr& stream, PinnedByteBlockPtr&& bytes);

    //! Receives and dispatches a Block to a MixStream
    void OnMixStreamBlock(
        size_t peer, Connection& s, const StreamMultiplexerHeader& header,
        const MixStreamDataPtr& stream, PinnedByteBlockPtr&& bytes);
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MULTIPLEXER_HEADER

/******************************************************************************/
