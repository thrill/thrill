/*******************************************************************************
 * c7a/net/lowlevel/select_dispatcher.hpp
 *
 * Asynchronous callback wrapper around select()
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_LOWLEVEL_SELECT_DISPATCHER_HEADER
#define C7A_NET_LOWLEVEL_SELECT_DISPATCHER_HEADER

#include <c7a/net/lowlevel/socket.hpp>
#include <c7a/net/lowlevel/select.hpp>
#include <c7a/net/exception.hpp>
#include <c7a/common/config.hpp>

#include <deque>

namespace c7a {
namespace net {
namespace lowlevel {

//! \addtogroup netsock Low Level Socket API
//! \{

/**
 * SelectDispatcher is a higher level wrapper for select(). One can register
 * Socket objects for readability and writability checks, buffered reads and
 * writes with completion callbacks, and also timer functions.
 */
class SelectDispatcher : protected Select
{
    static const bool debug = false;

    static const bool self_verify_ = common::g_self_verify;

public:
    //! type for file descriptor readiness callbacks
    typedef std::function<bool ()> Callback;

    //! Grow table if needed
    void CheckSize(int fd) {
        assert(fd >= 0);
        assert(fd <= 32000); // this is an arbitrary limit to catch errors.
        if (static_cast<size_t>(fd) >= watch_.size())
            watch_.resize(fd + 1);
    }

    //! Register a buffered read callback and a default exception callback.
    void AddRead(int fd, const Callback& read_cb) {
        CheckSize(fd);
        if (!watch_[fd].read_cb.size()) {
            Select::SetRead(fd);
            Select::SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].read_cb.emplace_back(read_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(int fd, const Callback& write_cb) {
        CheckSize(fd);
        if (!watch_[fd].write_cb.size()) {
            Select::SetWrite(fd);
            Select::SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].write_cb.emplace_back(write_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void SetExcept(int fd, const Callback& except_cb) {
        CheckSize(fd);
        if (!watch_[fd].except_cb) {
            Select::SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].except_cb = except_cb;
    }

    //! Cancel all callbacks on a given fd.
    void Cancel(int fd) {
        CheckSize(fd);

        if (watch_[fd].read_cb.size() == 0 &&
            watch_[fd].write_cb.size() == 0)
            LOG << "SelectDispatcher::Cancel() fd=" << fd
                << " called with no callbacks registered.";

        Select::ClearRead(fd);
        Select::ClearWrite(fd);
        Select::ClearException(fd);

        Watch& w = watch_[fd];
        w.read_cb.clear();
        w.write_cb.clear();
        w.except_cb = nullptr;
        w.active = false;
    }

    void Dispatch(const std::chrono::milliseconds& timeout) {

        // copy select fdset
        Select fdset = *this;

        if (self_verify_ || debug)
        {
            std::ostringstream oss;
            oss << "| ";

            for (size_t fd = 3; fd < watch_.size(); ++fd) {
                Watch& w = watch_[fd];

                if (!w.active) continue;

                assert((w.read_cb.size() == 0) != Select::InRead(fd));
                assert((w.write_cb.size() == 0) != Select::InWrite(fd));

                if (Select::InRead(fd))
                    oss << "r" << fd << " ";
                if (Select::InWrite(fd))
                    oss << "w" << fd << " ";
                if (Select::InException(fd))
                    oss << "e" << fd << " ";
            }

            LOG << "Performing select() on " << oss.str();
        }

        int r = fdset.select_timeout(timeout.count());

        if (r < 0) {
            // if we caught a signal, this is intended to interrupt a select().
            if (errno == EINTR) {
                LOG << "Dispatch(): select() was interrupted due to a signal.";
                return;
            }

            throw Exception("Dispatch::Select() failed!", errno);
        }
        if (r == 0) return;

        // start running through the table at fd 3. 0 = stdin, 1 = stdout, 2 =
        // stderr.

        for (size_t fd = 3; fd < watch_.size(); ++fd)
        {
            // we use a pointer into the watch_ table. however, since the
            // std::vector may regrow when callback handlers are called, this
            // pointer is reset a lot of times.
            Watch* w = &watch_[fd];

            if (!w->active) continue;

            if (fdset.InRead(fd))
            {
                if (w->read_cb.size()) {
                    // run read callbacks until one returns true (in which case
                    // it wants to be called again), or the read_cb list is
                    // empty.
                    while (w->read_cb.size() && w->read_cb.front()() == false) {
                        w = &watch_[fd];
                        w->read_cb.pop_front();
                    }
                    w = &watch_[fd];

                    if (w->read_cb.size() == 0) {
                        // if all read callbacks are done, listen no longer.
                        Select::ClearRead(fd);
                        if (w->write_cb.size() == 0 && !w->except_cb) {
                            // if also all write callbacks are done, stop
                            // listening.
                            Select::ClearWrite(fd);
                            Select::ClearException(fd);
                            w->active = false;
                        }
                    }
                }
                else {
                    LOG << "SelectDispatcher: got read event for fd "
                        << fd << " without a read handler.";

                    Select::ClearRead(fd);
                }
            }

            if (fdset.InWrite(fd))
            {
                if (w->write_cb.size()) {
                    // run write callbacks until one returns true (in which case
                    // it wants to be called again), or the write_cb list is
                    // empty.
                    while (w->write_cb.size() && w->write_cb.front()() == false) {
                        w = &watch_[fd];
                        w->write_cb.pop_front();
                    }
                    w = &watch_[fd];

                    if (w->write_cb.size() == 0) {
                        // if all write callbacks are done, listen no longer.
                        Select::ClearWrite(fd);
                        if (w->read_cb.size() == 0 && !w->except_cb) {
                            // if also all write callbacks are done, stop
                            // listening.
                            Select::ClearRead(fd);
                            Select::ClearException(fd);
                            w->active = false;
                        }
                    }
                }
                else {
                    LOG << "SelectDispatcher: got write event for fd "
                        << fd << " without a write handler.";

                    Select::ClearWrite(fd);
                }
            }

            if (fdset.InException(fd))
            {
                if (w->except_cb) {
                    if (!w->except_cb()) {
                        w = &watch_[fd];
                        // callback returned false: remove fd from set
                        Select::ClearException(fd);
                    }
                    w = &watch_[fd];
                }
                else {
                    DefaultExceptionCallback();
                }
            }
        }
    }

private:
    //! callback vectors per watched file descriptor
    struct Watch
    {
        //! boolean check whether any callbacks are registered
        bool                 active;
        //! queue of callbacks for fd.
        std::deque<Callback> read_cb, write_cb;
        //! only one exception callback for the fd.
        Callback             except_cb = nullptr;
    };

    //! handlers for all registered file descriptors. the fd integer range
    //! should be small enough, otherwise a more complicated data structure is
    //! needed.
    std::vector<Watch> watch_;

    //! Default exception handler
    static bool DefaultExceptionCallback() {
        throw Exception("SelectDispatcher() exception on socket!", errno);
    }
};

//! \}

} // namespace lowlevel
} // namespace net
} // namespace c7a

#endif // !C7A_NET_LOWLEVEL_SELECT_DISPATCHER_HEADER

/******************************************************************************/
