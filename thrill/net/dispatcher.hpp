/*******************************************************************************
 * thrill/net/dispatcher.hpp
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
#ifndef C7A_NET_DISPATCHER_HEADER
#define C7A_NET_DISPATCHER_HEADER

#include <thrill/data/block.hpp>
#include <thrill/net/buffer.hpp>
#include <thrill/net/connection.hpp>
#include <thrill/net/lowlevel/select_dispatcher.hpp>
#include <thrill/net/lowlevel/socket.hpp>

// TODO(tb) can we use a os switch? Do we want that? -tb: yes, later.
// #include <thrill/net/lowlevel/epoll-dispatcher.hpp>

#if defined(_LIBCPP_VERSION) || defined(__clang__)
#include <thrill/common/delegate.hpp>
#endif

#include <atomic>
#include <chrono>
#include <ctime>
#include <deque>
#include <queue>
#include <string>

namespace c7a {
namespace net {

//! \addtogroup net Network Communication
//! \{

/**
 * Dispatcher is a high level wrapper for asynchronous callback
 * processing.. One can register Connection objects for readability and
 * writability checks, buffered reads and writes with completion callbacks, and
 * also timer functions.
 */
class Dispatcher
{
    static const bool debug = false;

protected:
    //! switch between different low-level dispatchers
    using SubDispatcher = lowlevel::SelectDispatcher;
    // using SubDispatcher =  lowlevel::EPollDispatcher ;

    //! import into class namespace
    using Socket = lowlevel::Socket;

    //! import into class namespace
    using steady_clock = std::chrono::steady_clock;

    //! import into class namespace
    using milliseconds = std::chrono::milliseconds;

#if defined(_LIBCPP_VERSION) || defined(__clang__)
    template <typename Signature>
    using function = common::delegate<Signature>;
#else
    template <typename Signature>
    using function = std::function<Signature>;
#endif

    //! for access to terminate_
    friend class DispatcherThread;

public:
    //! default constructor
    Dispatcher() { }

    //! non-copyable: delete copy-constructor
    Dispatcher(const Dispatcher&) = delete;
    //! non-copyable: delete assignment operator
    Dispatcher& operator = (const Dispatcher&) = delete;

    //! \name Timeout Callbacks
    //! \{

    //! callback signature for timer events
    using TimerCallback = function<bool()>;

    //! Register a relative timeout callback
    void AddTimer(const std::chrono::milliseconds& timeout,
                  const TimerCallback& cb) {
        timer_pq_.emplace(steady_clock::now() + timeout,
                          std::chrono::duration_cast<milliseconds>(timeout),
                          cb);
    }

    //! \}

    //! \name Connection Callbacks
    //! \{

    //! callback signature for socket readable/writable events
    using ConnectionCallback = function<bool()>;

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Connection& c, const ConnectionCallback& read_cb) {
        return dispatcher_.AddRead(c.GetSocket().fd(), read_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Connection& c, const ConnectionCallback& write_cb) {
        return dispatcher_.AddWrite(c.GetSocket().fd(), write_cb);
    }

    //! Cancel all callbacks on a given connection.
    void Cancel(int fd) {
        return dispatcher_.Cancel(fd);
    }

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! callback signature for async read callbacks, they may acquire the buffer
    using AsyncReadCallback = function<void(Connection& c, Buffer&& buffer)>;

    //! asynchronously read n bytes and deliver them to the callback
    void AsyncRead(Connection& c, size_t n, AsyncReadCallback done_cb) {
        assert(c.GetSocket().IsValid());

        LOG << "async read on read dispatcher";
        if (n == 0) {
            if (done_cb) done_cb(c, Buffer());
            return;
        }

        // add new async reader object
        async_read_.emplace_back(n, done_cb);

        // register read callback
        AsyncReadBuffer& arb = async_read_.back();
        AddRead(c, [&arb, &c]() { return arb(c); });
    }

    //! callback signature for async read callbacks, they may acquire the buffer
    using AsyncReadByteBlockCallback = function<void(Connection& c)>;

    //! asynchronously read the full ByteBlock and deliver it to the callback
    void AsyncRead(Connection& c, const data::ByteBlockPtr& block,
                   AsyncReadByteBlockCallback done_cb) {
        assert(c.GetSocket().IsValid());

        LOG << "async read on read dispatcher";
        if (block->size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        // add new async reader object
        async_read_block_.emplace_back(block, done_cb);

        // register read callback
        AsyncReadByteBlock& arbb = async_read_block_.back();
        AddRead(c, [&arbb, &c]() { return arbb(c); });
    }

    //! callback signature for async write callbacks
    using AsyncWriteCallback = function<void(Connection&)>;

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    void AsyncWrite(Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = nullptr) {
        assert(c.GetSocket().IsValid());

        if (buffer.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        // add new async writer object
        async_write_.emplace_back(std::move(buffer), done_cb);

        // register write callback
        AsyncWriteBuffer& awb = async_write_.back();
        AddWrite(c, [&awb, &c]() { return awb(c); });
    }

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    void AsyncWrite(Connection& c, const data::Block& block,
                    AsyncWriteCallback done_cb = nullptr) {
        assert(c.GetSocket().IsValid());

        if (block.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        // add new async writer object
        async_write_block_.emplace_back(block, done_cb);

        // register write callback
        AsyncWriteBlock& awb = async_write_block_.back();
        AddWrite(c, [&awb, &c]() { return awb(c); });
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

    //! \name Dispatch
    //! \{

    //! Dispatch one or more events
    void Dispatch() {
        // process timer events that lie in the past
        steady_clock::time_point now = steady_clock::now();

        while (!terminate_ &&
               !timer_pq_.empty() &&
               timer_pq_.top().next_timeout <= now)
        {
            const Timer& top = timer_pq_.top();
            if (top.cb()) {
                // requeue timeout event again.
                timer_pq_.emplace(top.next_timeout + top.timeout,
                                  top.timeout, top.cb);
            }
            timer_pq_.pop();
        }

        if (terminate_) return;

        // calculate time until next timer event
        if (timer_pq_.empty()) {
            LOG << "Dispatch(): empty timer queue - selecting for 10s";
            dispatcher_.Dispatch(milliseconds(10000));
        }
        else {
            auto diff = std::chrono::duration_cast<milliseconds>(
                timer_pq_.top().next_timeout - now);

            if (diff < milliseconds(1)) diff = milliseconds(1);

            sLOG << "Dispatch(): waiting" << diff.count() << "ms";
            dispatcher_.Dispatch(diff);
        }

        // clean up finished AsyncRead/Writes
        while (async_read_.size() && async_read_.front().IsDone()) {
            async_read_.pop_front();
        }
        while (async_write_.size() && async_write_.front().IsDone()) {
            async_write_.pop_front();
        }

        while (async_read_block_.size() && async_read_block_.front().IsDone()) {
            async_read_block_.pop_front();
        }
        while (async_write_block_.size() && async_write_block_.front().IsDone()) {
            async_write_block_.pop_front();
        }
    }

    //! Loop over Dispatch() until terminate_ flag is set.
    void Loop() {
        while (!terminate_) {
            Dispatch();
        }
    }

    //! Causes the dispatcher to break out after the next timeout occurred
    //! Does not interrupt the currently running read/write operation, but
    //! breaks after the operation finished or timed out.
    void Terminate() {
        terminate_ = true;
    }

    //! Check whether there are still AsyncWrite()s in the queue.
    bool HasAsyncWrites() const {
        return (async_write_.size() != 0) || (async_write_block_.size() != 0);
    }

    //! \}

protected:
    //! low-level file descriptor async processing
    SubDispatcher dispatcher_;

    //! true if dispatcher needs to stop
    std::atomic<bool> terminate_ { false };

    //! struct for timer callbacks
    struct Timer
    {
        //! timepoint of next timeout
        steady_clock::time_point next_timeout;
        //! relative timeout for restarting
        milliseconds             timeout;
        //! callback
        TimerCallback            cb;

        Timer(const steady_clock::time_point& _next_timeout,
              const milliseconds& _timeout,
              const TimerCallback& _cb)
            : next_timeout(_next_timeout),
              timeout(_timeout),
              cb(_cb)
        { }

        bool operator < (const Timer& b) const
        { return next_timeout > b.next_timeout; }
    };

    //! priority queue of timer callbacks
    using TimerPQ = std::priority_queue<Timer>;

    //! priority queue of timer callbacks, obviously kept in timeout
    //! order. Currently not addressable.
    TimerPQ timer_pq_;

    /**************************************************************************/

    class AsyncReadBuffer
    {
    public:
        //! Construct buffered reader with callback
        AsyncReadBuffer(size_t buffer_size, const AsyncReadCallback& callback)
            : buffer_(buffer_size),
              callback_(callback)
        { }

        //! Should be called when the socket is readable
        bool operator () (Connection& c) {
            int r = c.GetSocket().recv_one(
                buffer_.data() + size_, buffer_.size() - size_);

            if (r <= 0) {
                // these errors are acceptable: just redo the recv later.
                if (errno == EINTR || errno == EAGAIN) return true;

                // signal artificial IsDone, for clean up.
                size_ = buffer_.size();

                // these errors are end-of-file indications (both good and bad)
                if (errno == 0 || errno == EPIPE || errno == ECONNRESET) {
                    if (callback_) callback_(c, Buffer());
                    return false;
                }
                throw Exception("AsyncReadBuffer() error in recv", errno);
            }

            size_ += r;

            if (size_ == buffer_.size()) {
                if (callback_) callback_(c, std::move(buffer_));
                return false;
            }
            else {
                return true;
            }
        }

        bool IsDone() const { return size_ == buffer_.size(); }

    private:
        //! Receive buffer
        Buffer buffer_;

        //! total size currently read
        size_t size_ = 0;

        //! functional object to call once data is complete
        AsyncReadCallback callback_;
    };

    //! deque of asynchronous readers
    std::deque<AsyncReadBuffer> async_read_;

    /**************************************************************************/

    class AsyncWriteBuffer
    {
    public:
        //! Construct buffered writer with callback
        AsyncWriteBuffer(Buffer&& buffer,
                         const AsyncWriteCallback& callback)
            : buffer_(std::move(buffer)),
              callback_(callback)
        { }

        //! Should be called when the socket is writable
        bool operator () (Connection& c) {
            int r = c.GetSocket().send_one(
                buffer_.data() + size_, buffer_.size() - size_);

            if (r <= 0) {
                if (errno == EINTR || errno == EAGAIN) return true;

                // signal artificial IsDone, for clean up.
                size_ = buffer_.size();

                if (errno == EPIPE) {
                    LOG1 << "AsyncWriteBuffer() got SIGPIPE";
                    if (callback_) callback_(c);
                    return false;
                }
                throw Exception("AsyncWriteBuffer() error in send", errno);
            }

            size_ += r;

            if (size_ == buffer_.size()) {
                if (callback_) callback_(c);
                return false;
            }
            else {
                return true;
            }
        }

        bool IsDone() const { return size_ == buffer_.size(); }

    private:
        //! Send buffer (owned by this writer)
        Buffer buffer_;

        //! total size currently written
        size_t size_ = 0;

        //! functional object to call once data is complete
        AsyncWriteCallback callback_;
    };

    //! deque of asynchronous writers
    std::deque<AsyncWriteBuffer> async_write_;

    /**************************************************************************/

    class AsyncReadByteBlock
    {
    public:
        //! Construct block reader with callback
        AsyncReadByteBlock(const data::ByteBlockPtr& block,
                           const AsyncReadByteBlockCallback& callback)
            : block_(block),
              callback_(callback)
        { }

        //! Should be called when the socket is readable
        bool operator () (Connection& c) {
            int r = c.GetSocket().recv_one(
                block_->data() + size_, block_->size() - size_);

            if (r <= 0) {
                // these errors are acceptable: just redo the recv later.
                if (errno == EINTR || errno == EAGAIN) return true;

                // signal artificial IsDone, for clean up.
                size_ = block_->size();

                // these errors are end-of-file indications (both good and bad)
                if (errno == 0 || errno == EPIPE || errno == ECONNRESET) {
                    if (callback_) callback_(c);
                    return false;
                }
                throw Exception("AsyncReadBlock() error in recv", errno);
            }

            size_ += r;

            if (size_ == block_->size()) {
                if (callback_) callback_(c);
                return false;
            }
            else {
                return true;
            }
        }

        bool IsDone() const { return size_ == block_->size(); }

    private:
        //! Receive block
        data::ByteBlockPtr block_;

        //! total size currently read
        size_t size_ = 0;

        //! functional object to call once data is complete
        AsyncReadByteBlockCallback callback_;
    };

    //! deque of asynchronous readers
    std::deque<AsyncReadByteBlock> async_read_block_;

    /**************************************************************************/

    class AsyncWriteBlock
    {
    public:
        //! Construct block writer with callback
        AsyncWriteBlock(const data::Block& block,
                        const AsyncWriteCallback& callback)

            : block_(block),
              callback_(callback)
        { }

        //! Should be called when the socket is writable
        bool operator () (Connection& c) {
            int r = c.GetSocket().send_one(
                block_.data_begin() + size_,
                block_.size() - size_);

            if (r <= 0) {
                if (errno == EINTR || errno == EAGAIN) return true;

                // signal artificial IsDone, for clean up.
                size_ = block_.size();

                if (errno == EPIPE) {
                    LOG1 << "AsyncWriteBlock() got SIGPIPE";
                    if (callback_) callback_(c);
                    return false;
                }
                throw Exception("AsyncWriteBlock() error in send", errno);
            }

            size_ += r;

            if (size_ == block_.size()) {
                if (callback_) callback_(c);
                return false;
            }
            else {
                return true;
            }
        }

        bool IsDone() const { return size_ == block_.size(); }

    private:
        //! Send block (holds a reference count to the underlying ByteBlock)
        data::Block block_;

        //! total size currently written
        size_t size_ = 0;

        //! functional object to call once data is complete
        AsyncWriteCallback callback_;
    };

    //! deque of asynchronous writers
    std::deque<AsyncWriteBlock> async_write_block_;

    /**************************************************************************/

    //! Default exception handler
    static bool ExceptionCallback(Connection& s) {
        // exception on listen socket ?
        throw Exception(
                  "Dispatcher() exception on socket fd "
                  + std::to_string(s.GetSocket().fd()) + "!", errno);
    }
};

//! \}

} // namespace net
} // namespace c7a

#endif // !C7A_NET_DISPATCHER_HEADER

/******************************************************************************/