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
#include <c7a/common/thread_pool.hpp>
#include <c7a/common/concurrent_queue.hpp>
#include <c7a/common/delegate.hpp>

#include <csignal>
#include <unistd.h>

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
    DispatcherThread(const std::string& thread_name)
        : dispatcher_(), name_(thread_name) {
        // allocate self-pipe
        int r = ::pipe(self_pipe_);
        die_unless(r == 0);
        // start thread
        thread_ = std::thread(&DispatcherThread::Work, this);
    }

    ~DispatcherThread() {
        Terminate();

        close(self_pipe_[0]);
        close(self_pipe_[1]);
    }

    //! non-copyable: delete copy-constructor
    DispatcherThread(const DispatcherThread&) = delete;
    //! non-copyable: delete assignment operator
    DispatcherThread& operator = (const DispatcherThread&) = delete;

    //! Return internal Dispatcher object
    //Dispatcher & dispatcher() { return dispatcher_; }

    //! Terminate the dispatcher thread (if now already done).
    void Terminate() {
        if (terminate_) return;

        // set termination flags.
        terminate_ = true;
        // interrupt select().
        WakeUpThread();
        // wait for last round to finish.
        thread_.join();
    }

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
        Enqueue([=, &c]() {
                    dispatcher_.AddRead(c, read_cb);
                });
        WakeUpThread();
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Connection& c, const ConnectionCallback& write_cb) {
        Enqueue([=, &c]() {
                    dispatcher_.AddWrite(c, write_cb);
                });
        WakeUpThread();
    }

    //! Cancel all callbacks on a given connection.
    void Cancel(Connection& c) {
        int fd = c.GetSocket().fd();
        Enqueue([this, fd]() {
                    dispatcher_.Cancel(fd);
                });
        WakeUpThread();
    }

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! asynchronously read n bytes and deliver them to the callback
    void AsyncRead(Connection& c, size_t n, AsyncReadCallback done_cb) {
        Enqueue([=, &c]() {
                    dispatcher_.AsyncRead(c, n, done_cb);
                });
        WakeUpThread();
    }

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    void AsyncWrite(Connection& c, Buffer&& buffer1, Buffer&& buffer2,
                    AsyncWriteCallback done_cb = nullptr) {
        // the following captures the move-only buffer in a lambda.
        Enqueue([=, &c,
                  b1 = std::move(buffer1), b2 = std::move(buffer2)]() mutable {
                    dispatcher_.AsyncWrite(c, std::move(b1));
                    dispatcher_.AsyncWrite(c, std::move(b2), done_cb);
                });
        WakeUpThread();
    }

    //! asynchronously write TWO buffers and callback when delivered. The
    //! buffer2 are MOVED into the async writer. This is most useful to write a
    //! header and a payload Buffers that are hereby guaranteed to be written in
    //! order.
    void AsyncWrite(Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = nullptr) {
        // the following captures the move-only buffer in a lambda.
        Enqueue([=, &c, b = std::move(buffer)]() mutable {
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

    //! What happens in the dispatcher thread
    void Work() {
        common::GetThreadDirectory().NameThisThread(name_);

        // Ignore PIPE signals (received when writing to closed sockets)
        signal(SIGPIPE, SIG_IGN);

        // wait interrupts via self-pipe.
        dispatcher_.dispatcher_.AddRead(
            self_pipe_[0], [this]() {
                ssize_t rb;
                while ((rb = read(self_pipe_[0], &self_pipe_buffer_, 1)) == 0) {
                    LOG1 << "Work: error reading from self-pipe: " << errno;
                }
                die_unless(rb == 1);
                return true;
            });

        while (!terminate_ ||
               dispatcher_.HasAsyncWrites() || !jobqueue_.empty())
        {
            // process jobs in jobqueue_
            {
                Job job;
                while (jobqueue_.try_pop(job))
                    job();
            }

            // run one dispatch
            dispatcher_.Dispatch();
        }
    }

    //! wake up select() in dispatching thread.
    void WakeUpThread() {
        // there are multiple very platform-dependent ways to do this. we'll try
        // to use the self-pipe trick for now. The select() method waits on
        // another fd, which we write one byte to when we need to interrupt the
        // select().

        // another method would be to send a signal() via pthread_kill() to the
        // select thread, but that had a race condition for waking up the other
        // thread. -tb

        // send one byte to wake up the select() handler.
        ssize_t wb;
        while ((wb = write(self_pipe_[1], this, 1)) == 0) {
            LOG1 << "WakeUp: error sending to self-pipe: " << errno;
        }
        die_unless(wb == 1);
    }

private:
    //! Queue of jobs to be run by dispatching thread at its discretion.
    common::ConcurrentQueue<Job> jobqueue_;

    //! thread of dispatcher
    std::thread thread_;

    //! enclosed dispatcher.
    Dispatcher dispatcher_;

    //! termination flag
    std::atomic<bool> terminate_ { false };

    //! thread name for logging
    std::string name_;

    //! self-pipe to wake up thread.
    int self_pipe_[2];

    //! buffer to receive one byte from self-pipe
    int self_pipe_buffer_;
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_DISPATCHER_THREAD_HEADER

/******************************************************************************/
