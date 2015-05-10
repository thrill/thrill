/*******************************************************************************
 * c7a/net/net-dispatcher.hpp
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
#ifndef C7A_NET_NET_DISPATCHER_HEADER
#define C7A_NET_NET_DISPATCHER_HEADER

#include <c7a/net/net-connection.hpp>
#include <c7a/net/lowlevel/socket.hpp>
#include <c7a/net/lowlevel/select-dispatcher.hpp>
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
    typedef lowlevel::SelectDispatcher Dispatcher;
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
    typedef std::function<bool (const NetConnection&)> ConnectionCallback;

    //! Register a buffered read callback and a default exception callback.
    void AddRead(const NetConnection& s, const ConnectionCallback& read_cb)
    {
        return dispatcher_.AddRead(
            s.GetSocket(),
            [ = ](Socket& s) { return read_cb(NetConnection(s)); });
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(const NetConnection& s, const ConnectionCallback& write_cb)
    {
        return dispatcher_.AddWrite(
            s.GetSocket(),
            [ = ](Socket& s) { return write_cb(NetConnection(s)); });
    }

    //! Register a buffered write callback and a default exception callback.
    void AddReadWrite(
        const NetConnection& s,
        const ConnectionCallback& read_cb, const ConnectionCallback& write_cb)
    {
        return dispatcher_.AddReadWrite(
            s.GetSocket(),
            [ = ](Socket& s) { return read_cb(NetConnection(s)); },
            [ = ](Socket& s) { return write_cb(NetConnection(s)); });
    }

    //! \}

    //! \name Asynchronous Data Reader/Writer Callbacks
    //! \{

    //! callback signature for async read callbacks
    typedef std::function<void (const NetConnection& s,
                                const std::string& buffer)> AsyncReadCallback;

    //! asynchronously read n bytes and deliver them to the callback
    virtual void AsyncRead(const NetConnection& s, size_t n,
                           AsyncReadCallback done_cb)
    {
        LOG << "async read on read dispatcher";
        if (n == 0) {
            if (done_cb) done_cb(s, std::string());
            return;
        }

        // add new async reader object
        async_read_.emplace_back(n, done_cb);

        // register read callback
        dispatcher_.AddRead(s.GetSocket(), async_read_.back());
    }

    //! callback signature for async write callbacks
    typedef std::function<void (const NetConnection& s)> AsyncWriteCallback;

    //! asynchronously write buffer and callback when delivered
    void AsyncWrite(const NetConnection& s, const std::string& buffer,
                    AsyncWriteCallback done_cb = nullptr)
    {
        if (buffer.size() == 0) {
            if (done_cb) done_cb(s);
            return;
        }

        // add new async writeer object
        async_write_.emplace_back(buffer, done_cb);

        // register write callback
        dispatcher_.AddWrite(s.GetSocket(), async_write_.back());
    }

    //! asynchronously write buffer and callback when delivered. Copies the data
    //! into a std::string buffer!
    void AsyncWrite(const NetConnection& s, const void* buffer, size_t size,
                    AsyncWriteCallback done_cb = NULL)
    {
        return AsyncWrite(
            s, std::string(reinterpret_cast<const char*>(buffer), size),
            done_cb);
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
              buffer_(buffer_size, 0)
        { }

        //! Should be called when the socket is readable
        bool operator () (Socket& s)
        {
            int r = s.recv_one(const_cast<char*>(buffer_.data() + size_),
                               buffer_.size() - size_);

            if (r < 0)
                throw NetException("AsyncReadBuffer() error in recv", errno);

            size_ += r;

            if (size_ == buffer_.size()) {
                if (callback_) callback_(NetConnection(s), buffer_);
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
        std::string buffer_;
    };

    //! deque of asynchronous readers
    std::deque<AsyncReadBuffer> async_read_;

    /**************************************************************************/

    class AsyncWriteBuffer
    {
    public:
        //! Construct buffered writer with callback
        AsyncWriteBuffer(const std::string& buffer,
                         const AsyncWriteCallback& callback)
            : callback_(callback),
              buffer_(buffer)
        { }

        //! Should be called when the socket is writable
        bool operator () (Socket& s)
        {
            int r = s.send_one(const_cast<char*>(buffer_.data() + size_),
                               buffer_.size() - size_);

            if (r < 0)
                throw NetException("AsyncWriteBuffer() error in send", errno);

            size_ += r;

            if (size_ == buffer_.size()) {
                if (callback_) callback_(NetConnection(s));
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
        std::string buffer_;
    };

    //! deque of asynchronous writers
    std::deque<AsyncWriteBuffer> async_write_;

    /**************************************************************************/

    //! Default exception handler
    static bool ExceptionCallback(NetConnection& s)
    {
        // exception on listen socket ?
        throw NetException(
                  "NetDispatcher() exception on socket fd "
                  + std::to_string(s.GetSocket().GetFileDescriptor()) + "!", errno);
    }
};

//! \}

} // namespace net

} // namespace c7a

#endif // !C7A_NET_NET_DISPATCHER_HEADER

/******************************************************************************/
