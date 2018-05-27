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
    std::unique_ptr<class Dispatcher> dispatcher, size_t host_rank)
    : dispatcher_(std::move(dispatcher)),
      host_rank_(host_rank) {
    // start thread
    thread_ = std::thread(&DispatcherThread::Work, this);
}

DispatcherThread::~DispatcherThread() {
    Terminate();
}

void DispatcherThread::Terminate() {
    if (terminate_) return;

    // set termination flags.
    terminate_ = true;
    // interrupt select().
    WakeUpThread();
    // wait for last round to finish.
    thread_.join();
}

void DispatcherThread::RunInThread(const AsyncDispatcherThreadCallback& cb) {
    Enqueue([this, cb = std::move(cb)]() {
                cb(*dispatcher_);
            });
    WakeUpThread();
}

void DispatcherThread::AddTimer(
    std::chrono::milliseconds timeout, const TimerCallback& cb) {
    Enqueue([=]() {
                dispatcher_->AddTimer(timeout, cb);
            });
    WakeUpThread();
}

void DispatcherThread::AddRead(Connection& c, const AsyncCallback& read_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AddRead(c, read_cb);
            });
    WakeUpThread();
}

void DispatcherThread::AddWrite(Connection& c, const AsyncCallback& write_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AddWrite(c, write_cb);
            });
    WakeUpThread();
}

void DispatcherThread::Cancel(Connection& c) {
    Enqueue([=, &c]() {
                dispatcher_->Cancel(c);
            });
    WakeUpThread();
}

void DispatcherThread::AsyncRead(
    Connection& c, uint32_t seq, size_t size,
    const AsyncReadCallback& done_cb) {
    Enqueue([=, &c]() {
                dispatcher_->AsyncRead(c, seq, size, done_cb);
            });
    WakeUpThread();
}

void DispatcherThread::AsyncRead(
    Connection& c, uint32_t seq, size_t size, data::PinnedByteBlockPtr&& block,
    const AsyncReadByteBlockCallback& done_cb) {
    assert(block.valid());
    Enqueue([=, &c, b = std::move(block)]() mutable {
                dispatcher_->AsyncRead(c, seq, size, std::move(b), done_cb);
            });
    WakeUpThread();
}

void DispatcherThread::AsyncWrite(
    Connection& c, uint32_t seq, Buffer&& buffer, const AsyncWriteCallback& done_cb) {
    // the following captures the move-only buffer in a lambda.
    Enqueue([=, &c, b = std::move(buffer)]() mutable {
                dispatcher_->AsyncWrite(c, seq, std::move(b), done_cb);
            });
    WakeUpThread();
}

void DispatcherThread::AsyncWrite(
    Connection& c, uint32_t seq, Buffer&& buffer, data::PinnedBlock&& block,
    const AsyncWriteCallback& done_cb) {
    assert(block.IsValid());
    // the following captures the move-only buffer in a lambda.
    Enqueue([=, &c,
             b1 = std::move(buffer), b2 = std::move(block)]() mutable {
                dispatcher_->AsyncWrite(c, seq, std::move(b1));
                dispatcher_->AsyncWrite(c, seq + 1, std::move(b2), done_cb);
            });
    WakeUpThread();
}

void DispatcherThread::AsyncWriteCopy(
    Connection& c, uint32_t seq, const void* buffer, size_t size,
    const AsyncWriteCallback& done_cb) {
    return AsyncWrite(c, seq, Buffer(buffer, size), done_cb);
}

void DispatcherThread::AsyncWriteCopy(
    Connection& c, uint32_t seq,
    const std::string& str, const AsyncWriteCallback& done_cb) {
    return AsyncWriteCopy(c, seq, str.data(), str.size(), done_cb);
}

void DispatcherThread::Enqueue(Job&& job) {
    return jobqueue_.push(std::move(job));
}

void DispatcherThread::Work() {
    common::NameThisThread(
        "host " + std::to_string(host_rank_) + " dispatcher");
    // pin DispatcherThread to last core
    common::SetCpuAffinity(std::thread::hardware_concurrency() - 1);

    while (!terminate_ ||
           dispatcher_->HasAsyncWrites() || !jobqueue_.empty())
    {
        // process jobs in jobqueue_
        {
            Job job;
            while (jobqueue_.try_pop(job))
                job();
        }

        // set busy flag, but check once again for jobs.
        busy_ = true;
        {
            Job job;
            if (jobqueue_.try_pop(job)) {
                busy_ = false;
                job();
                continue;
            }
        }

        // run one dispatch
        dispatcher_->Dispatch();

        busy_ = false;
    }

    LOG << "DispatcherThread finished.";
}

void DispatcherThread::WakeUpThread() {
    if (busy_)
        dispatcher_->Interrupt();
}

} // namespace net
} // namespace thrill

/******************************************************************************/
