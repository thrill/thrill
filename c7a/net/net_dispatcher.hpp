/*******************************************************************************
 * c7a/net/net_dispatcher.hpp
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
#ifndef C7A_NET_NET_DISPATCHER_HEADER
#define C7A_NET_NET_DISPATCHER_HEADER

#include <c7a/net/net_connection.hpp>
#include <c7a/net/buffer.hpp>
#include <c7a/net/lowlevel/socket.hpp>
#include <c7a/net/lowlevel/select_dispatcher.hpp>
//#include <c7a/net/lowlevel/epoll-dispatcher.hpp>

#include <string>
#include <deque>
#include <queue>
#include <ctime>

namespace c7a {
namespace net {
//! \addtogroup net Network Communication
//! \{

/**
 * NetDispatcher is a high level wrapper for asynchronous callback
 * processing.. One can register NetConnection objects for readability and
 * writability checks, buffered reads and writes with completion callbacks, and
 * also timer functions.
 */
class NetDispatcher
{
    static const bool debug = false;

protected:
    //! switch between different low-level dispatchers
    typedef lowlevel::SelectDispatcher<NetConnection&> Dispatcher;
    //typedef lowlevel::EPollDispatcher Dispatcher;

    //! import into class namespace
    typedef lowlevel::Socket Socket;

public:
    //! \name Timeout Callbacks
    //! \{

    //! callback signature for timer events
    typedef std::function<bool ()> TimerCallback;

    //! Register a relative timeout callback
    void AddRelativeTimeout(double timeout, const TimerCallback& cb)
    {
        timer_pq_.emplace(GetClock() + timeout, timeout, cb);
    }

    //! \}

    //! \name NetConnection Callbacks
    //! \{

    //! callback signature for socket readable/writable events
    typedef std::function<bool (NetConnection&)> ConnectionCallback;

    //! Register a buffered read callback and a default exception callback.
    void AddRead(NetConnection& c, const ConnectionCallback& read_cb)
    {
        return dispatcher_.AddRead(c.GetSocket().fd(), c, read_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(NetConnection& c, const ConnectionCallback& write_cb)
    {
        return dispatcher_.AddWrite(c.GetSocket().fd(), c, write_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddReadWrite(
        NetConnection& c,
        const ConnectionCallback& read_cb, const ConnectionCallback& write_cb)
    {
        return dispatcher_.AddReadWrite(
                   c.GetSocket().fd(), c, read_cb, write_cb);
    }

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! callback signature for async read callbacks, they may acquire the buffer
    typedef std::function<void (NetConnection& c,
                                Buffer&& buffer)> AsyncReadCallback;

    //! asynchronously read n bytes and deliver them to the callback
    virtual void AsyncRead(NetConnection& c, size_t n,
                           AsyncReadCallback done_cb)
    {
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
        AddRead(c, [&arb](NetConnection & c) { return arb(c); });
    }

    //! callback signature for async write callbacks
    typedef std::function<void (NetConnection&)> AsyncWriteCallback;

    //! asynchronously write buffer and callback when delivered. The buffer is
    //! MOVED into the async writer.
    void AsyncWrite(NetConnection& c, Buffer && buffer,
                    AsyncWriteCallback done_cb = nullptr)
    {
        assert(c.GetSocket().IsValid());

        if (buffer.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        // add new async writer object
        async_write_.emplace_back(std::move(buffer), done_cb);

        // register write callback
        AsyncWriteBuffer& awb = async_write_.back();
        AddWrite(c, [&awb](NetConnection & c) { return awb(c); });
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(NetConnection& c, const void* buffer, size_t size,
                        AsyncWriteCallback done_cb = NULL)
    {
        return AsyncWrite(c, Buffer(buffer, size), done_cb);
    }

    //! asynchronously write buffer and callback when delivered. COPIES the data
    //! into a Buffer!
    void AsyncWriteCopy(NetConnection& c, const std::string& str,
                        AsyncWriteCallback done_cb = NULL)
    {
        return AsyncWriteCopy(c, str.data(), str.size(), done_cb);
    }

    //! \}

    //! \name Dispatch
    //! \{

    //! dispatch one or more events
    void Dispatch()
    {
        // process timer events that lie in the past
        double now = GetClock();

        while (!timer_pq_.empty() && timer_pq_.top().next_timeout <= now)
        {
            const Timer& top = timer_pq_.top();
            if (top.cb()) {
                // requeue timeout event again.
                timer_pq_.emplace(top.next_timeout + top.timeout,
                                  top.timeout, top.cb);
            }
            timer_pq_.pop();
        }

        // calculate time until next timer event
        if (timer_pq_.empty()) {
            dispatcher_.Dispatch(INFINITY);
        }
        else {
            dispatcher_.Dispatch(timer_pq_.top().next_timeout - now);
        }
    }

    //! \}

protected:
    //! get a current monotonic clock
    static double GetClock()
    {
        struct timespec ts;
        ::clock_gettime(CLOCK_MONOTONIC, &ts);
        return ts.tv_sec + ts.tv_nsec * 1e-9;
    }

    //! low-level file descriptor async processing
    Dispatcher dispatcher_;

    //! struct for timer callbacks
    struct Timer
    {
        //! timepoint of next timeout
        double        next_timeout;
        //! relative timeout for restarting
        double        timeout;
        //! callback
        TimerCallback cb;

        Timer(double _next_timeout, double _timeout, const TimerCallback& _cb)
            : next_timeout(_next_timeout), timeout(_timeout), cb(_cb)
        { }

        bool operator < (const Timer& b) const
        { return next_timeout < b.next_timeout; }
    };

    //! priority queue of timer callbacks
    typedef std::priority_queue<Timer> TimerPQ;

    //! priority queue of timer callbacks, obviously kept in timeout
    //! order. Currently not addressable.
    TimerPQ timer_pq_;

    /**************************************************************************/

    class AsyncReadBuffer
    {
    public:
        //! Construct buffered reader with callback
        AsyncReadBuffer(size_t buffer_size, const AsyncReadCallback& callback)
            : callback_(callback),
              buffer_(buffer_size)
        { }

        //! Should be called when the socket is readable
        bool operator () (NetConnection& c)
        {
            int r = c.GetSocket().recv_one(
                buffer_.data() + size_, buffer_.size() - size_);

            if (r < 0)
                throw Exception("AsyncReadBuffer() error in recv", errno);

            size_ += r;

            if (size_ == buffer_.size()) {
                if (callback_) callback_(c, std::move(buffer_));
                return false;
            }
            else {
                return true;
            }
        }

    private:
        //! total size currently read
        size_t size_ = 0;

        //! functional object to call once data is complete
        AsyncReadCallback callback_;

        //! Receive buffer
        Buffer buffer_;
    };

    //! deque of asynchronous readers
    std::deque<AsyncReadBuffer> async_read_;

    /**************************************************************************/

    class AsyncWriteBuffer
    {
    public:
        //! Construct buffered writer with callback
        AsyncWriteBuffer(Buffer && buffer,
                         const AsyncWriteCallback& callback)
            : callback_(callback),
              buffer_(std::move(buffer))
        { }

        //! Should be called when the socket is writable
        bool operator () (NetConnection& c)
        {
            int r = c.GetSocket().send_one(
                buffer_.data() + size_, buffer_.size() - size_);

            if (r < 0)
                throw Exception("AsyncWriteBuffer() error in send", errno);

            size_ += r;

            if (size_ == buffer_.size()) {
                if (callback_) callback_(c);
                return false;
            }
            else {
                return true;
            }
        }

    private:
        //! total size currently written
        size_t size_ = 0;

        //! functional object to call once data is complete
        AsyncWriteCallback callback_;

        //! Receive buffer
        Buffer buffer_;
    };

    //! deque of asynchronous writers
    std::deque<AsyncWriteBuffer> async_write_;

    /**************************************************************************/

    //! Default exception handler
    static bool ExceptionCallback(NetConnection& s)
    {
        // exception on listen socket ?
        throw Exception(
                  "NetDispatcher() exception on socket fd "
                  + std::to_string(s.GetSocket().fd()) + "!", errno);
    }
};

//! \}
} // namespace net
} // namespace c7a

#endif // !C7A_NET_NET_DISPATCHER_HEADER

/******************************************************************************/
