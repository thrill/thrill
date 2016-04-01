/*******************************************************************************
 * thrill/net/dispatcher_thread.hpp
 *
 * Asynchronous callback wrapper around select(), epoll(), or other kernel-level
 * dispatchers.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_DISPATCHER_THREAD_HEADER
#define THRILL_NET_DISPATCHER_THREAD_HEADER

#include <thrill/common/concurrent_queue.hpp>
#include <thrill/common/delegate.hpp>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/block.hpp>
#include <thrill/mem/allocator.hpp>
#include <thrill/net/buffer.hpp>
#include <thrill/net/connection.hpp>

#include <string>

namespace thrill {
namespace net {

//! \addtogroup net_layer
//! \{

//! Signature of timer callbacks.
using TimerCallback = common::Delegate<bool(), mem::GPoolAllocator<char> >;

//! Signature of async connection readability/writability callbacks.
using AsyncCallback = common::Delegate<bool(), mem::GPoolAllocator<char> >;

//! Signature of async read callbacks.
using AsyncReadCallback = common::Delegate<
          void(Connection& c, Buffer&& buffer), mem::GPoolAllocator<char> >;

//! Signature of async read ByteBlock callbacks.
using AsyncReadByteBlockCallback = common::Delegate<
          void(Connection& c, data::PinnedByteBlockPtr&& block),
          mem::GPoolAllocator<char> >;

//! Signature of async write callbacks.
using AsyncWriteCallback = common::Delegate<
          void(Connection&), mem::GPoolAllocator<char> >;

/*!
 * DispatcherThread contains a net::Dispatcher object and an associated thread
 * that runs in the dispatching loop.
 */
class DispatcherThread
{
    static constexpr bool debug = false;

public:
    //! Signature of async jobs to be run by the dispatcher thread.
    using Job = common::Delegate<void(), mem::GPoolAllocator<char> >;

    DispatcherThread(
        mem::Manager& mem_manager,
        std::unique_ptr<class Dispatcher>&& dispatcher,
        const mem::by_string& thread_name);

    DispatcherThread(
        mem::Manager& mem_manager,
        class Group& group, const mem::by_string& thread_name);

    ~DispatcherThread();

    //! non-copyable: delete copy-constructor
    DispatcherThread(const DispatcherThread&) = delete;
    //! non-copyable: delete assignment operator
    DispatcherThread& operator = (const DispatcherThread&) = delete;

    //! Terminate the dispatcher thread (if now already done).
    void Terminate();

    // *** note that callbacks are passed by value, because they must be copied
    // *** into the closured by the methods. -tb

    //! \name Timeout Callbacks
    //! \{

    //! Register a relative timeout callback
    void AddTimer(std::chrono::milliseconds timeout, TimerCallback cb);

    //! \}

    //! \name Connection Callbacks
    //! \{

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Connection& c, AsyncCallback read_cb);

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Connection& c, AsyncCallback write_cb);

    //! Cancel all callbacks on a given connection.
    void Cancel(Connection& c);

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! asynchronously read n bytes and deliver them to the callback
    void AsyncRead(Connection& c, size_t n, AsyncReadCallback done_cb);

    //! asynchronously read the full ByteBlock and deliver it to the callback
    void AsyncRead(Connection& c, size_t n, data::PinnedByteBlockPtr&& block,
                   AsyncReadByteBlockCallback done_cb);

    //! asynchronously write byte and block and callback when delivered. The
    //! block is reference counted by the async writer.
    void AsyncWrite(Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = AsyncWriteCallback());

    //! asynchronously write TWO buffers and callback when delivered. The
    //! buffer2 are MOVED into the async writer. This is most useful to write a
    //! header and a payload Buffers that are hereby guaranteed to be written in
    //! order.
    void AsyncWrite(Connection& c,
                    Buffer&& buffer, const data::PinnedBlock& block,
                    AsyncWriteCallback done_cb = AsyncWriteCallback());

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(
        Connection& c, const void* buffer, size_t size,
        AsyncWriteCallback done_cb = AsyncWriteCallback());

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(
        Connection& c, const std::string& str,
        AsyncWriteCallback done_cb = AsyncWriteCallback());

    //! \}

private:
    //! Enqueue job in queue for dispatching thread to run at its discretion.
    void Enqueue(Job&& job);

    //! What happens in the dispatcher thread
    void Work();

    //! wake up select() in dispatching thread.
    void WakeUpThread();

private:
    //! common memory stats, should become a HostContext member.
    mem::Manager mem_manager_;

    //! Queue of jobs to be run by dispatching thread at its discretion.
    common::ConcurrentQueue<Job, mem::Allocator<Job> > jobqueue_ {
        mem::Allocator<Job>(mem_manager_)
    };

    //! thread of dispatcher
    std::thread thread_;

    //! enclosed dispatcher.
    std::unique_ptr<class Dispatcher> dispatcher_;

    //! termination flag
    std::atomic<bool> terminate_ { false };

    //! thread name for logging
    mem::by_string name_;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_DISPATCHER_THREAD_HEADER

/******************************************************************************/
