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
#include <c7a/common/sequentializer.hpp>
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
    typedef Dispatcher::TimerCallback TimerCallback;

    //! Signature of async connection readability/writability callbacks.
    typedef Dispatcher::ConnectionCallback ConnectionCallback;

    //! Signature of async read callbacks.
    typedef Dispatcher::AsyncReadCallback AsyncReadCallback;

    //! Signature of async write callbacks.
    typedef Dispatcher::AsyncWriteCallback AsyncWriteCallback;

    //! \}

public:
    DispatcherThread()
        : dispatcher_() {
        sequentializer_.Enqueue(
            [this]() { StartWork(); });
    }

    ~DispatcherThread() {
        // set termination flag.
        sequentializer_.Terminate();
        // interrupt select().
        WakeUpThread();
        // wait for last round to finish.
        sequentializer_.LoopUntilTerminate();
    }

    //! non-copyable: delete copy-constructor
    DispatcherThread(const DispatcherThread&) = delete;
    //! non-copyable: delete assignment operator
    DispatcherThread& operator = (const DispatcherThread&) = delete;

    //! Return internal Dispatcher object
    Dispatcher & dispatcher() { return dispatcher_; }

    //! \name Timeout Callbacks
    //! \{

    //! Register a relative timeout callback
    template <class Rep, class Period>
    void AddRelativeTimeout(const std::chrono::duration<Rep, Period>& timeout,
                            const TimerCallback& cb) {
        sequentializer_.Enqueue([=]() {
                                    dispatcher_.AddRelativeTimeout(timeout, cb);
                                });
        WakeUpThread();
    }

    //! \}

    //! \name Connection Callbacks
    //! \{

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Connection& c, const ConnectionCallback& read_cb) {
        sequentializer_.Enqueue([ =, &c]() {
                                    dispatcher_.AddRead(c, read_cb);
                                });
        WakeUpThread();
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Connection& c, const ConnectionCallback& write_cb) {
        sequentializer_.Enqueue([ =, &c]() {
                                    dispatcher_.AddWrite(c, write_cb);
                                });
        WakeUpThread();
    }

    //! Register a buffered write callback and a default exception callback.
    void AddReadWrite(
        Connection& c,
        const ConnectionCallback& read_cb, const ConnectionCallback& write_cb) {
        sequentializer_.Enqueue([ =, &c]() {
                                    dispatcher_.AddReadWrite(c, read_cb, write_cb);
                                });
        WakeUpThread();
    }

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! asynchronously read n bytes and deliver them to the callback
    void AsyncRead(Connection& c, size_t n, AsyncReadCallback done_cb) {
        sequentializer_.Enqueue([ =, &c]() {
                                    dispatcher_.AsyncRead(c, n, done_cb);
                                });
        WakeUpThread();
    }

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    void AsyncWrite(Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = nullptr) {
        // the following captures the move-only buffer in a lambda.
        sequentializer_.Enqueue([ =, &c, b = std::move(buffer)]() mutable {
                                    dispatcher_.AsyncWrite(c, std::move(b), done_cb);
                                });
        WakeUpThread();
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const void* buffer, size_t size,
                        AsyncWriteCallback done_cb = NULL) {
        return AsyncWrite(c, Buffer(buffer, size), done_cb);
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(Connection& c, const std::string& str,
                        AsyncWriteCallback done_cb = NULL) {
        return AsyncWriteCopy(c, str.data(), str.size(), done_cb);
    }

    //! \}

protected:
    //! signal handler: do nothing, but receiving this interrupts the select().
    static void SignalUSR1(int) {
        return;
    }

    //! What happens in the dispatcher thread
    void StartWork() {
        {
            // Set USR1 signal handler
            struct sigaction sa;
            memset(&sa, 0, sizeof(sa));
            sa.sa_handler = SignalUSR1;

            int r = sigaction(SIGUSR1, &sa, NULL);
            die_unless(r == 0);
        }

        running_ = true;
        Work();
    }

    //! What happens in the dispatcher thread
    void Work() {
        // run one dispatch
        dispatcher_.Dispatch();
        // enqueue next dispatch (process jobs in between). TODO(tb): this is
        // actually stupid: better have a tbb::concurrent_queue of callback jobs
        // to do, and loop over them and the select().
        sequentializer_.Enqueue([this]() { Work(); });
    }

    //! wake up select() in dispatching thread.
    void WakeUpThread() {
        // there are multiple very platform-dependent ways to do this. we'll try
        // signals for now. The signal should interrupt the select(), and as we
        // do no processing in the signal, do nothing else.

        // Another way to wake up the blocking select() in the dispatcher is to
        // create a "self-pipe", and write one byte to it.

        if (running_)
            pthread_kill(sequentializer_.thread(0).native_handle(), SIGUSR1);
    }

private:
    //! Sequentializer for queueing all jobs that work on the dispatcher.
    common::Sequentializer sequentializer_;

    //! enclosed dispatcher.
    Dispatcher dispatcher_;

    //! check whether the signal handler was set before issuing signals.
    bool running_ = false;
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_DISPATCHER_THREAD_HEADER

/******************************************************************************/
