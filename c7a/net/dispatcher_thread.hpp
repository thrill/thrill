/*******************************************************************************
 * c7a/net/dispatcher_thread.hpp
 *
 * Asynchronous callback wrapper around select(), epoll(), or other kernel-level
 * dispatchers.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_DISPATCHER_THREAD_HEADER
#define C7A_NET_DISPATCHER_THREAD_HEADER

#include <c7a/net/dispatcher.hpp>

#include <thread>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

/**
 * DispatcherThread contains a net::Dispatcher object and an associated thread
 * that runs in the dispatching loop.
 */
class DispatcherThread
{
    static const bool debug = true;

public:
    //! \name Imported Typedefs
    //! \{

    //! Signature of timer callbacks.
    typedef Dispatcher::TimerCallback TimerCallback;

    //! Signature of async connection readability/writability callbacks.
    typedef Dispatcher::ConnectionCallback ConnectionCallback;

    //! Signature of async read callbacks.
    typedef Dispatcher::AsyncReadCallback AsyncReadCallback;

    //! Signature of async write callbacks.
    typedef Dispatcher::AsyncWriteCallback AsyncWriteCallback;

    //! \}

public:
    //! \name Start and Stop Threads
    //! \{

    //! Start dispatching thread
    void Start() {
        die_unless(!running_);
        LOG << "DispatcherThread::Start(): starting";
        dispatcher_.terminate_ = false;
        thread_ = std::thread(&Dispatcher::Loop, &dispatcher_);
        running_ = true;
    }

    //! Stop dispatching thread
    void Stop() {
        die_unless(running_);
        LOG << "DispatcherThread::Stop(): stopping thread";
        dispatcher_.Terminate();
        thread_.join();
        running_ = false;
    }

    //! \}

    //! \name Timeout Callbacks
    //! \{

    //! Register a relative timeout callback
    template <class Rep, class Period>
    void AddRelativeTimeout(const std::chrono::duration<Rep, Period>& timeout,
                            const TimerCallback& cb) {
        return dispatcher_.AddRelativeTimeout(timeout, cb);
    }

    //! \}

    //! \name Connection Callbacks
    //! \{

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Connection& c, const ConnectionCallback& read_cb) {
        return dispatcher_.AddRead(c, read_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Connection& c, const ConnectionCallback& write_cb) {
        return dispatcher_.AddWrite(c, write_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddReadWrite(
        Connection& c,
        const ConnectionCallback& read_cb, const ConnectionCallback& write_cb) {
        return dispatcher_.AddReadWrite(c, read_cb, write_cb);
    }

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! asynchronously read n bytes and deliver them to the callback
    void AsyncRead(Connection& c, size_t n, AsyncReadCallback done_cb) {
        return dispatcher_.AsyncRead(c, n, done_cb);
    }

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    void AsyncWrite(Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = nullptr) {
        return dispatcher_.AsyncWrite(c, std::move(buffer), done_cb);
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const void* buffer, size_t size,
                        AsyncWriteCallback done_cb = NULL) {
        return dispatcher_.AsyncWriteCopy(c, buffer, size, done_cb);
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const std::string& str,
                        AsyncWriteCallback done_cb = NULL) {
        return dispatcher_.AsyncWriteCopy(c, str, done_cb);
    }

    //! \}

private:
    //! enclosed dispatcher.
    Dispatcher dispatcher_;

    //! flag whether our thread is running
    bool running_ = false;

    //! handle to our thread
    std::thread thread_;
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_DISPATCHER_THREAD_HEADER

/******************************************************************************/
