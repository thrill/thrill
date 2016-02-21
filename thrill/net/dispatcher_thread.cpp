/*******************************************************************************
 * thrill/net/dispatcher_thread.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/net/dispatcher.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/group.hpp>

#include <deque>
#include <string>
#include <vector>

namespace thrill {
namespace net {

DispatcherThread::DispatcherThread(
    mem::Manager& mem_manager,
    mem::unique_ptr<class Dispatcher>&& dispatcher,
    const mem::by_string& thread_name)
    : mem_manager_(&mem_manager, "DispatcherThread"),
      dispatcher_(std::move(dispatcher)),
      name_(thread_name) {
    // start thread
    thread_ = std::thread(&DispatcherThread::Work, this);
}

DispatcherThread::DispatcherThread(
    mem::Manager& mem_manager, Group& group, const mem::by_string& thread_name)
    : DispatcherThread(mem_manager,
                       group.ConstructDispatcher(mem_manager), thread_name) { }

DispatcherThread::~DispatcherThread() {
    Terminate();
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
    Connection& c, const AsyncCallback& read_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AddRead(c, read_cb);
            });
    WakeUpThread();
}

//! Register a buffered write callback and a default exception callback.
void DispatcherThread::AddWrite(
    Connection& c, const AsyncCallback& write_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AddWrite(c, write_cb);
            });
    WakeUpThread();
}

//! Cancel all callbacks on a given connection.
void DispatcherThread::Cancel(Connection& c) {
    Enqueue([this, &c]() {
                dispatcher_->Cancel(c);
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

//! asynchronously read the full ByteBlock and deliver it to the callback
void DispatcherThread::AsyncRead(
    Connection& c, size_t n, data::PinnedByteBlockPtr&& block,
    AsyncReadByteBlockCallback done_cb) {
    assert(block.valid());
    Enqueue([=, &c, b = std::move(block)]() mutable {
                dispatcher_->AsyncRead(c, n, std::move(b), done_cb);
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
                                  const data::PinnedBlock& block,
                                  AsyncWriteCallback done_cb) {
    assert(block.IsValid());
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
    dispatcher_->Interrupt();
}

} // namespace net
} // namespace thrill

/******************************************************************************/
