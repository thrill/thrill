/*******************************************************************************
 * thrill/net/dispatcher_thread.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/net/dispatcher.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/lowlevel/select_dispatcher.hpp>

#include <unistd.h>

#include <deque>
#include <string>
#include <vector>

namespace thrill {
namespace net {

DispatcherThread::DispatcherThread(const mem::by_string& thread_name)
    : dispatcher_(
          mem::mm_new<lowlevel::SelectDispatcher>(mem_manager_, mem_manager_),
          mem::Deleter<Dispatcher>(mem_manager_)
          ),
      name_(thread_name) {
    // start thread
    thread_ = std::thread(&DispatcherThread::Work, this);
}

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
void DispatcherThread::AsyncRead(Connection& c, const data::ByteBlockPtr& block,
                                 AsyncReadByteBlockCallback done_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AsyncRead(c, block, done_cb);
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
