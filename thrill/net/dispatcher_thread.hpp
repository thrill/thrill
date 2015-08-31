/*******************************************************************************
 * thrill/net/dispatcher_thread.hpp
 *
 * Asynchronous callback wrapper around select(), epoll(), or other kernel-level
 * dispatchers.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_DISPATCHER_THREAD_HEADER
#define THRILL_NET_DISPATCHER_THREAD_HEADER

#include <thrill/common/concurrent_queue.hpp>
#include <thrill/common/delegate.hpp>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/block.hpp>
#include <thrill/mem/allocator.hpp>
#include <thrill/net/connection.hpp>

#include <string>

namespace thrill {
namespace net {

//! \addtogroup net Network Communication
//! \{

//! Signature of timer callbacks.
using TimerCallback = common::delegate<bool()>;

//! Signature of async connection readability/writability callbacks.
using AsyncCallback = common::delegate<bool()>;

//! Signature of async read callbacks.
using AsyncReadCallback = common::delegate<void(Connection& c, Buffer&& buffer)>;

//! Signature of async read ByteBlock callbacks.
using AsyncReadByteBlockCallback = common::delegate<void(Connection& c)>;

//! Signature of async write callbacks.
using AsyncWriteCallback = common::delegate<void(Connection&)>;

/**
 * DispatcherThread contains a net::Dispatcher object and an associated thread
 * that runs in the dispatching loop.
 */
class DispatcherThread
{
    static const bool debug = false;

public:
    //! \name Imported Typedefs
    //! \{

    //! Signature of async jobs to be run by the dispatcher thread.
    using Job = common::ThreadPool::Job;

    //! \}

    //! common global memory stats, should become a HostContext member.
    mem::Manager mem_manager_ { nullptr };

public:
    explicit DispatcherThread(const mem::string& thread_name);
    ~DispatcherThread();

    //! non-copyable: delete copy-constructor
    DispatcherThread(const DispatcherThread&) = delete;
    //! non-copyable: delete assignment operator
    DispatcherThread& operator = (const DispatcherThread&) = delete;

    //! Terminate the dispatcher thread (if now already done).
    void Terminate();

    //! \name Timeout Callbacks
    //! \{

    //! Register a relative timeout callback
    void AddTimer(const std::chrono::milliseconds& timeout,
                  const TimerCallback& cb);

    //! \}

    //! \name Connection Callbacks
    //! \{

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Connection& c, const AsyncCallback& read_cb);

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Connection& c, const AsyncCallback& write_cb);

    //! Cancel all callbacks on a given connection.
    void Cancel(Connection& c);

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! asynchronously read n bytes and deliver them to the callback
    void AsyncRead(Connection& c, size_t n, AsyncReadCallback done_cb);

    //! asynchronously read the full ByteBlock and deliver it to the callback
    void AsyncRead(Connection& c, const data::ByteBlockPtr& block,
                   AsyncReadByteBlockCallback done_cb);

    //! asynchronously write TWO buffers and callback when delivered. The
    //! buffer2 are MOVED into the async writer. This is most useful to write a
    //! header and a payload Buffers that are hereby guaranteed to be written in
    //! order.
    void AsyncWrite(Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = nullptr);

    //! asynchronously write byte and block and callback when delivered. The
    //! block is reference counted by the async writer.
    void AsyncWrite(Connection& c, Buffer&& buffer, const data::Block& block,
                    AsyncWriteCallback done_cb = nullptr);

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const void* buffer, size_t size,
                        AsyncWriteCallback done_cb = nullptr);

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const std::string& str,
                        AsyncWriteCallback done_cb = nullptr);

    //! \}

protected:
    //! Enqueue job in queue for dispatching thread to run at its discretion.
    void Enqueue(Job&& job);

    //! What happens in the dispatcher thread
    void Work();

    //! wake up select() in dispatching thread.
    void WakeUpThread();

private:
    //! Queue of jobs to be run by dispatching thread at its discretion.
    common::ConcurrentQueue<Job, mem::Allocator<Job> > jobqueue_ {
        mem::Allocator<Job>(mem_manager_)
    };

    //! thread of dispatcher
    std::thread thread_;

    //! enclosed dispatcher.
    mem::mm_unique_ptr<class Dispatcher> dispatcher_;

    //! termination flag
    std::atomic<bool> terminate_ { false };

    //! thread name for logging
    mem::string name_;

    //! self-pipe to wake up thread.
    int self_pipe_[2];

    //! buffer to receive one byte from self-pipe
    int self_pipe_buffer_;
};

//! \}

} // namespace net
} // namespace thrill

#endif // !THRILL_NET_DISPATCHER_THREAD_HEADER

/******************************************************************************/
