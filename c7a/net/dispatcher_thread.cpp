/*******************************************************************************
 * c7a/net/dispatcher_thread.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/dispatcher.hpp>
#include <c7a/net/dispatcher_thread.hpp>

#include <unistd.h>

#include <csignal>
#include <deque>
#include <vector>

namespace c7a {
namespace net {

DispatcherThread::DispatcherThread(const std::string& thread_name)
    : dispatcher_(new Dispatcher),
      name_(thread_name) {
    // allocate self-pipe
    int r = ::pipe(self_pipe_);
    die_unless(r == 0);
    // start thread
    thread_ = std::thread(&DispatcherThread::Work, this);
}

DispatcherThread::~DispatcherThread() {
    Terminate();

    close(self_pipe_[0]);
    close(self_pipe_[1]);

    delete dispatcher_;
}

//! Terminate the dispatcher thread (if now already done).
void DispatcherThread::Terminate() {
    if (terminate_) return;

    // set termination flags.
    terminate_ = true;
    // interrupt select().
    WakeUpThread();
    // wait for last round to finish.
    thread_.join();
}

//! Register a relative timeout callback
void DispatcherThread::AddTimer(
    const std::chrono::milliseconds& timeout, const TimerCallback& cb) {
    Enqueue([=]() {
                dispatcher_->AddTimer(timeout, cb);
            });
    WakeUpThread();
}

//! Register a buffered read callback and a default exception callback.
void DispatcherThread::AddRead(
    Connection& c, const ConnectionCallback& read_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AddRead(c, read_cb);
            });
    WakeUpThread();
}

//! Register a buffered write callback and a default exception callback.
void DispatcherThread::AddWrite(
    Connection& c, const ConnectionCallback& write_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AddWrite(c, write_cb);
            });
    WakeUpThread();
}

//! Cancel all callbacks on a given connection.
void DispatcherThread::Cancel(Connection& c) {
    int fd = c.GetSocket().fd();
    Enqueue([this, fd]() {
                dispatcher_->Cancel(fd);
            });
    WakeUpThread();
}

//! asynchronously read n bytes and deliver them to the callback
void DispatcherThread::AsyncRead(
    Connection& c, size_t n, AsyncReadCallback done_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AsyncRead(c, n, done_cb);
            });
    WakeUpThread();
}

//! asynchronously write TWO buffers and callback when delivered. The
//! buffer2 are MOVED into the async writer. This is most useful to write a
//! header and a payload Buffers that are hereby guaranteed to be written in
//! order.
void DispatcherThread::AsyncWrite(
    Connection& c, Buffer&& buffer, AsyncWriteCallback done_cb) {
    // the following captures the move-only buffer in a lambda.
    Enqueue([=, &c, b = std::move(buffer)]() mutable {
                dispatcher_->AsyncWrite(c, std::move(b), done_cb);
            });
    WakeUpThread();
}

//! asynchronously write buffer and callback when delivered. The buffer is
//! MOVED into the async writer.
void DispatcherThread::AsyncWrite(Connection& c, Buffer&& buffer,
                                  const data::Block& block,
                                  AsyncWriteCallback done_cb) {
    // the following captures the move-only buffer in a lambda.
    Enqueue([=, &c,
              b1 = std::move(buffer), b2 = block]() mutable {
                dispatcher_->AsyncWrite(c, std::move(b1));
                dispatcher_->AsyncWrite(c, b2, done_cb);
            });
    WakeUpThread();
}

//! asynchronously write buffer and callback when delivered. COPIES the data
//! into a Buffer!
void DispatcherThread::AsyncWriteCopy(Connection& c, const void* buffer, size_t size,
                                      AsyncWriteCallback done_cb) {
    return AsyncWrite(c, Buffer(buffer, size), done_cb);
}

//! asynchronously write buffer and callback when delivered. COPIES the data
//! into a Buffer!
void DispatcherThread::AsyncWriteCopy(Connection& c, const std::string& str,
                                      AsyncWriteCallback done_cb) {
    return AsyncWriteCopy(c, str.data(), str.size(), done_cb);
}

//! Enqueue job in queue for dispatching thread to run at its discretion.
void DispatcherThread::Enqueue(Job&& job) {
    return jobqueue_.push(std::move(job));
}

//! What happens in the dispatcher thread
void DispatcherThread::Work() {
    common::NameThisThread(name_);

    // Ignore PIPE signals (received when writing to closed sockets)
    signal(SIGPIPE, SIG_IGN);

    // wait interrupts via self-pipe.
    dispatcher_->dispatcher_.AddRead(
        self_pipe_[0], [this]() {
            ssize_t rb;
            while ((rb = read(self_pipe_[0], &self_pipe_buffer_, 1)) == 0) {
                LOG1 << "Work: error reading from self-pipe: " << errno;
            }
            die_unless(rb == 1);
            return true;
        });

    while (!terminate_ ||
           dispatcher_->HasAsyncWrites() || !jobqueue_.empty())
    {
        // process jobs in jobqueue_
        {
            Job job;
            while (jobqueue_.try_pop(job))
                job();
        }

        // run one dispatch
        dispatcher_->Dispatch();
    }
}

//! wake up select() in dispatching thread.
void DispatcherThread::WakeUpThread() {
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

} // namespace net
} // namespace c7a

/******************************************************************************/
