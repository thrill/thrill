/*******************************************************************************
 * c7a/net/lowlevel/epoll_dispatcher.hpp
 *
 * Asynchronous callback wrapper around epoll()
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_LOWLEVEL_EPOLL_DISPATCHER_HEADER
#define C7A_NET_LOWLEVEL_EPOLL_DISPATCHER_HEADER

#include <c7a/net/lowlevel/socket.hpp>

#include <sys/epoll.h>
#include <map>

namespace c7a {
namespace net {
namespace lowlevel {
//! \addtogroup netsock Low Level Socket API
//! \{

/**
 * EPollDispatcher is a higher level wrapper for epoll(). One can register
 * Socket objects for readability and writability checks, buffered reads and
 * writes with completion callbacks, and also timer functions.
 */
class EPollDispatcher
{
    static const bool debug = false;

public:
    //! construct epoll() dispatcher
    EPollDispatcher()
    {
        epollfd_ = epoll_create1(0);

        if (epollfd_ == -1)
            throw NetException("EPollDispatcher() could not get epoll() handle",
                               errno);
    }

    //! free epoll() dispatcher
    ~EPollDispatcher()
    {
        if (epollfd_ >= 0)
            close(epollfd_);
    }

    typedef std::function<bool (Socket&)> Callback;

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Socket& s, const Callback& read_cb)
    {
        int fd = s.fd();

        WatchMap::iterator it = watch_.find(fd);
        if (it != watch_.end())
        {
            Watch& w = it->second;

            if (w.read_cb)
                throw NetException("EPollDispatcher() fd " + std::to_string(fd)
                                   + " already has read callback");

            w.read_cb = read_cb;
            w.events |= EPOLLIN;

            struct epoll_event ev;
            ev.events = w.events;
            ev.data.ptr = &(*it);

            if (epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &ev) == -1)
                throw NetException("EPollDispatcher() error in epoll_ctl()",
                                   errno);
        }
        else
        {
            Watch w(EPOLLIN, s, read_cb, nullptr, ExceptionCallback);
            it = watch_.insert(std::make_pair(fd, w)).first;

            struct epoll_event ev;
            ev.events = it->second.events;
            ev.data.ptr = &(*it);

            if (epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &ev) == -1)
                throw NetException("EPollDispatcher() error in epoll_ctl()",
                                   errno);
        }
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Socket& s, const Callback& write_cb)
    {
        int fd = s.fd();

        WatchMap::iterator it = watch_.find(fd);
        if (it != watch_.end())
        {
            Watch& w = it->second;

            if (w.write_cb)
                throw NetException("EPollDispatcher() fd " + std::to_string(fd)
                                   + " already has write callback");

            w.write_cb = write_cb;
            w.events |= EPOLLOUT;

            struct epoll_event ev;
            ev.events = w.events;
            ev.data.ptr = &(*it);

            if (epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &ev) == -1)
                throw NetException("EPollDispatcher() error in epoll_ctl()",
                                   errno);
        }
        else
        {
            Watch w(EPOLLOUT, s, nullptr, write_cb, ExceptionCallback);
            it = watch_.insert(std::make_pair(fd, w)).first;

            struct epoll_event ev;
            ev.events = it->second.events;
            ev.data.ptr = &(*it);

            if (epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &ev) == -1)
                throw NetException("EPollDispatcher() error in epoll_ctl()",
                                   errno);
        }
    }

    //! Register a buffered write callback and a default exception callback.
    void AddReadWrite(Socket& s,
                      const Callback& read_cb, const Callback& write_cb)
    {
        int fd = s.fd();

        WatchMap::iterator it = watch_.find(fd);
        if (it != watch_.end())
        {
            Watch& w = it->second;

            if (w.read_cb)
                throw NetException("EPollDispatcher() fd " + std::to_string(fd)
                                   + " already has read callback");

            if (w.write_cb)
                throw NetException("EPollDispatcher() fd " + std::to_string(fd)
                                   + " already has write callback");

            w.read_cb = write_cb;
            w.write_cb = write_cb;
            w.events |= EPOLLOUT;

            struct epoll_event ev;
            ev.events = w.events;
            ev.data.ptr = &(*it);

            if (epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &ev) == -1)
                throw NetException("EPollDispatcher() error in epoll_ctl()",
                                   errno);
        }
        else
        {
            Watch w(EPOLLOUT, s, read_cb, write_cb, ExceptionCallback);
            it = watch_.insert(std::make_pair(fd, w)).first;

            struct epoll_event ev;
            ev.events = it->second.events;
            ev.data.ptr = &(*it);

            if (epoll_ctl(epollfd_, EPOLL_CTL_ADD, fd, &ev) == -1)
                throw NetException("EPollDispatcher() error in epoll_ctl()",
                                   errno);
        }
    }

    void Dispatch(double timeout)
    {
        if (watch_.size() == 0)
            LOG << "EPollDispatcher() called without any file descriptor to wait on";

        static const size_t max_events = 16;
        struct epoll_event events[max_events];

        int tm_msec = (timeout == INFINITY ? -1 : timeout * 1e6);
        int nfds = epoll_wait(epollfd_, events, max_events, tm_msec);

        if (nfds == -1)
            throw NetException("EPollDispatcher() error in epoll_wait()", errno);

        for (int i = 0; i < nfds; ++i)
        {
            struct epoll_event& ev = events[i];

            WatchMap::value_type* wm
                = reinterpret_cast<WatchMap::value_type*>(ev.data.ptr);

            int fd = wm->first;
            Watch& w = wm->second;

            if (ev.events & EPOLLIN)
            {
                if (w.read_cb) {
                    // copy read callback, may be replace
                    Callback cb = w.read_cb;
                    w.read_cb = NULL;

                    // execute callback -> true requeue/false dequeue
                    if (!cb(w.socket))
                    {
                        if (!w.read_cb)
                        {
                            w.events &= ~EPOLLIN;

                            if (w.events == 0) {
                                if (epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, &ev) == -1)
                                    throw NetException("EPollDispatcher() error in epoll_ctl()",
                                                       errno);

                                watch_.erase(fd);
                                continue;
                            }
                            else {
                                struct epoll_event evn;
                                evn.events = w.events;
                                evn.data.ptr = wm;

                                if (epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &evn) == -1)
                                    throw NetException("EPollDispatcher() error in epoll_ctl()",
                                                       errno);
                            }
                        }
                    }
                    else
                    {
                        if (w.read_cb)
                            throw NetException("EPollDispatcher() fd " + std::to_string(fd)
                                               + " already has read callback");
                        w.read_cb = cb;
                    }
                }
                else {
                    LOG << "EPollDispatcher: got read event for fd "
                        << fd << " without a read handler.";
                }
            }

            if (ev.events & EPOLLOUT)
            {
                if (w.write_cb) {
                    // copy write callback, may be replace
                    Callback cb = w.write_cb;
                    w.write_cb = NULL;

                    // execute callback -> true requeue/false skip
                    if (!cb(w.socket))
                    {
                        if (!w.write_cb)
                        {
                            w.events &= ~EPOLLOUT;

                            if (w.events == 0) {
                                if (epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, &ev) == -1)
                                    throw NetException("EPollDispatcher() error in epoll_ctl()",
                                                       errno);

                                watch_.erase(fd);
                                continue;
                            }
                            else {
                                struct epoll_event evn;
                                evn.events = w.events;
                                evn.data.ptr = wm;

                                if (epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &evn) == -1)
                                    throw NetException("EPollDispatcher() error in epoll_ctl()",
                                                       errno);
                            }
                        }
                    }
                    else
                    {
                        if (w.write_cb)
                            throw NetException("EPollDispatcher() fd " + std::to_string(fd)
                                               + " already has write callback");
                        w.write_cb = cb;
                    }
                }
                else {
                    LOG << "EPollDispatcher: got write event for fd "
                        << fd << " without a write handler.";
                }
            }

            if (ev.events & EPOLLERR)
            {
                if (w.except_cb) {
                    // copy except callback, may be replace
                    Callback cb = w.except_cb;
                    w.except_cb = NULL;

                    // execute callback -> true requeue/false skip
                    if (!cb(w.socket))
                    {
                        if (!w.except_cb)
                        {
                            w.events &= ~EPOLLERR;

                            if (w.events == 0) {
                                if (epoll_ctl(epollfd_, EPOLL_CTL_DEL, fd, &ev) == -1)
                                    throw NetException("EPollDispatcher() error in epoll_ctl()",
                                                       errno);

                                watch_.erase(fd);
                                continue;
                            }
                            else {
                                struct epoll_event evn;
                                evn.events = w.events;
                                evn.data.ptr = wm;

                                if (epoll_ctl(epollfd_, EPOLL_CTL_MOD, fd, &evn) == -1)
                                    throw NetException("EPollDispatcher() error in epoll_ctl()",
                                                       errno);
                            }
                        }
                    }
                    else
                    {
                        if (w.except_cb)
                            throw NetException("EPollDispatcher() fd " + std::to_string(fd)
                                               + " already has except callback");
                        w.except_cb = cb;
                    }
                }
                else {
                    LOG << "EPollDispatcher: got except event for fd "
                        << fd << " without a except handler.";
                }
            }
        }
    }

private:
    //! epoll descriptor
    int epollfd_ = -1;

    //! struct to entries per watched file descriptor
    struct Watch
    {
        uint32_t events;
        Socket   socket;
        Callback read_cb, write_cb, except_cb;

        Watch(uint32_t _events, Socket& _socket,
              const Callback& _read_cb, const Callback& _write_cb,
              const Callback& _except_cb)
            : events(_events),
              socket(_socket),
              read_cb(_read_cb),
              write_cb(_write_cb),
              except_cb(_except_cb)
        { }
    };

    typedef std::map<int, Watch> WatchMap;

    //! handlers for all registered file descriptors, we have to keep them
    //! address local.
    WatchMap watch_;

    //! Default exception handler
    static bool ExceptionCallback(Socket& s)
    {
        // exception on listen socket ?
        throw NetException("EPollDispatcher() exception on socket fd "
                           + std::to_string(s.fd()) + "!",
                           errno);
    }
};

//! \}
} // namespace lowlevel
} // namespace net
} // namespace c7a

#endif // !C7A_NET_LOWLEVEL_EPOLL_DISPATCHER_HEADER

/******************************************************************************/
