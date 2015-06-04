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
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_DISPATCHER_THREAD_HEADER
#define C7A_NET_DISPATCHER_THREAD_HEADER

#include <c7a/net/dispatcher.hpp>
#include <c7a/common/thread_pool.hpp>
#include <c7a/common/concurrent_queue.hpp>
#include <c7a/common/delegate.hpp>

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
    using TimerCallback = Dispatcher::TimerCallback;

    //! Signature of async connection readability/writability callbacks.
    using ConnectionCallback = Dispatcher::ConnectionCallback;

    //! Signature of async read callbacks.
    using AsyncReadCallback = Dispatcher::AsyncReadCallback;

    //! Signature of async write callbacks.
    using AsyncWriteCallback = Dispatcher::AsyncWriteCallback;

    //! Signature of async jobs to be run by the dispatcher thread.
    using Job = common::ThreadPool::Job;

    //! \}

public:
    DispatcherThread()
        : dispatcher_() {
        thread_ = std::thread(&DispatcherThread::Work, this);
    }

    ~DispatcherThread() {
        // set termination flag.
        terminate_ = true;
        // interrupt select().
        WakeUpThread();
        // wait for last round to finish.
        thread_.join();
    }

    //! non-copyable: delete copy-constructor
    DispatcherThread(const DispatcherThread&) = delete;
    //! non-copyable: delete assignment operator
    DispatcherThread& operator = (const DispatcherThread&) = delete;

    //! Return internal Dispatcher object
    //Dispatcher & dispatcher() { return dispatcher_; }

    //! \name Timeout Callbacks
    //! \{

    //! Register a relative timeout callback
    template <class Rep, class Period>
    void AddRelativeTimeout(const std::chrono::duration<Rep, Period>& timeout,
                            const TimerCallback& cb) {
        Enqueue([=]() {
                    dispatcher_.AddRelativeTimeout(timeout, cb);
                });
        WakeUpThread();
    }

    //! \}

    //! \name Connection Callbacks
    //! \{

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Connection& c, const ConnectionCallback& read_cb) {
        Enqueue([ =, &c]() {
                    dispatcher_.AddRead(c, read_cb);
                });
        WakeUpThread();
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Connection& c, const ConnectionCallback& write_cb) {
        Enqueue([ =, &c]() {
                    dispatcher_.AddWrite(c, write_cb);
                });
        WakeUpThread();
    }

    //! Register a buffered write callback and a default exception callback.
    void AddReadWrite(
        Connection& c,
        const ConnectionCallback& read_cb, const ConnectionCallback& write_cb) {
        Enqueue([ =, &c]() {
                    dispatcher_.AddReadWrite(c, read_cb, write_cb);
                });
        WakeUpThread();
    }

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! asynchronously read n bytes and deliver them to the callback
    void AsyncRead(Connection& c, size_t n, AsyncReadCallback done_cb) {
        Enqueue([ =, &c]() {
                    dispatcher_.AsyncRead(c, n, done_cb);
                });
        WakeUpThread();
    }

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    void AsyncWrite(Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = nullptr) {
        // the following captures the move-only buffer in a lambda.
        Enqueue([ =, &c, b = std::move(buffer)]() mutable {
                    dispatcher_.AsyncWrite(c, std::move(b), done_cb);
                });
        WakeUpThread();
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const void* buffer, size_t size,
                        AsyncWriteCallback done_cb = nullptr) {
        return AsyncWrite(c, Buffer(buffer, size), done_cb);
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const std::string& str,
                        AsyncWriteCallback done_cb = nullptr) {
        return AsyncWriteCopy(c, str.data(), str.size(), done_cb);
    }

    //! \}

protected:
    //! Enqueue job in queue for dispatching thread to run at its discretion.
    void Enqueue(Job&& job) {
        return jobqueue_.push(std::move(job));
    }

    //! signal handler: do nothing, but receiving this interrupts the select().
    static void SignalALRM(int) {
        return;
    }

    //! What happens in the dispatcher thread
    void Work() {
        {
            // Set ALRM signal handler
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sa.sa_handler = SignalALRM;

            int r = sigaction(SIGALRM, &sa, NULL);
            die_unless(r == 0);
        }

        running_ = true;

        while (!terminate_) {
            // process jobs in jobqueue_
            {
                Job job;
                while (jobqueue_.try_pop(job)) {
                    job();
                }
            }

            // run one dispatch
            dispatcher_.Dispatch();
        }
    }

    //! wake up select() in dispatching thread.
    void WakeUpThread() {
        // there are multiple very platform-dependent ways to do this. we'll try
        // signals for now. The signal should interrupt the select(), and as we
        // do no processing in the signal, do nothing else.

        // Another way to wake up the blocking select() in the dispatcher is to
        // create a "self-pipe", and write one byte to it.

        if (running_)
            pthread_kill(thread_.native_handle(), SIGALRM);
    }

private:
    //! Queue of jobs to be run by dispatching thread at its discretion.
    common::concurrent_queue<Job> jobqueue_;

    //! thread of dispatcher
    std::thread thread_;

    //! check whether the signal handler was set before issuing signals.
    bool running_ = false;

    //! termination flag of dispatcher thread.
    bool terminate_ = false;

    //! enclosed dispatcher.
    Dispatcher dispatcher_;
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_DISPATCHER_THREAD_HEADER

/******************************************************************************/
