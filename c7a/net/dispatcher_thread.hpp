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
#include <mutex>
#include <csignal>

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
    static const bool debug = false;

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

protected:
    typedef std::unique_lock<std::mutex> unique_lock;

public:
    DispatcherThread()
        : dispatcher_(&mutex_)
    { }

    //! \name Start and Stop Threads
    //! \{

    //! Start dispatching thread
    void Start() {
        die_unless(!running_);
        LOG << "DispatcherThread::Start(): starting";
        dispatcher_.terminate_ = false;
        thread_ = std::thread(&DispatcherThread::Work, this);
        running_ = true;
    }

    //! Stop dispatching thread
    void Stop() {
        die_unless(running_);
        LOG << "DispatcherThread::Stop(): stopping thread";
        dispatcher_.Terminate();
        WakeUpThread();
        thread_.join();
        running_ = false;
    }

    //! Return Dispatcher
    Dispatcher & dispatcher() { return dispatcher_; }

    //! \}

    //! \name Timeout Callbacks
    //! \{

    //! Register a relative timeout callback
    template <class Rep, class Period>
    void AddRelativeTimeout(const std::chrono::duration<Rep, Period>& timeout,
                            const TimerCallback& cb) {
        unique_lock lock(mutex_);
        dispatcher_.AddRelativeTimeout(timeout, cb);
        WakeUpThread();
    }

    //! \}

    //! \name Connection Callbacks
    //! \{

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Connection& c, const ConnectionCallback& read_cb) {
        unique_lock lock(mutex_);
        dispatcher_.AddRead(c, read_cb);
        WakeUpThread();
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Connection& c, const ConnectionCallback& write_cb) {
        unique_lock lock(mutex_);
        dispatcher_.AddWrite(c, write_cb);
        WakeUpThread();
    }

    //! Register a buffered write callback and a default exception callback.
    void AddReadWrite(
        Connection& c,
        const ConnectionCallback& read_cb, const ConnectionCallback& write_cb) {
        unique_lock lock(mutex_);
        dispatcher_.AddReadWrite(c, read_cb, write_cb);
        WakeUpThread();
    }

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! asynchronously read n bytes and deliver them to the callback
    void AsyncRead(Connection& c, size_t n, AsyncReadCallback done_cb) {
        unique_lock lock(mutex_);
        dispatcher_.AsyncRead(c, n, done_cb);
        WakeUpThread();
    }

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    void AsyncWrite(Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = nullptr) {
        unique_lock lock(mutex_);
        dispatcher_.AsyncWrite(c, std::move(buffer), done_cb);
        WakeUpThread();
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const void* buffer, size_t size,
                        AsyncWriteCallback done_cb = NULL) {
        unique_lock lock(mutex_);
        dispatcher_.AsyncWriteCopy(c, buffer, size, done_cb);
        WakeUpThread();
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const std::string& str,
                        AsyncWriteCallback done_cb = NULL) {
        unique_lock lock(mutex_);
        dispatcher_.AsyncWriteCopy(c, str, done_cb);
        WakeUpThread();
    }

    //! \}

protected:
    //! signal handler: do nothing, but receiving this interrupts the select().
    static void SignalUSR1(int) {
        return;
    }

    //! What happens in the dispatcher thread
    void Work() {
        {
            // Set USR1 signal handler
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sa.sa_handler = SignalUSR1;

            int r = sigaction(SIGUSR1, &sa, NULL);
            die_unless(r == 0);
        }

        dispatcher_.Loop();
    }

    //! wake up select() in dispatching thread.
    void WakeUpThread() {
        // there are multiple very platform-dependent ways to do this. we'll try
        // signals for now. The signal should interrupt the select(), and as we
        // do no processing in the signal, do nothing else.

        // Another way to wake up the blocking select() in the dispatcher is to
        // create a "self-pipe", and write one byte to it.

        if (running_)
            pthread_kill(thread_.native_handle(), SIGUSR1);
    }

private:
    //! lock all calls to dispatcher
    std::mutex mutex_;

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
