/*******************************************************************************
 * thrill/net/tcp/select_dispatcher.cpp
 *
 * Lightweight wrapper around BSD socket API.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/net/tcp/select_dispatcher.hpp>

#include <sstream>

namespace thrill {
namespace net {
namespace tcp {

//! Run one iteration of dispatching select().
void SelectDispatcher::DispatchOne(const std::chrono::milliseconds& timeout) {

    // copy select fdset
    Select fdset = select_;

    if (self_verify_)
    {
        for (size_t fd = 3; fd < watch_.size(); ++fd) {
            Watch& w = watch_[fd];

            if (!w.active) continue;

            assert((w.read_cb.size() == 0) != select_.InRead(fd));
            assert((w.write_cb.size() == 0) != select_.InWrite(fd));
        }
    }

    if (debug)
    {
        std::ostringstream oss;
        oss << "| ";

        for (int fd = 3; fd < static_cast<int>(watch_.size()); ++fd) {
            Watch& w = watch_[fd];

            if (!w.active) continue;

            if (select_.InRead(fd))
                oss << "r" << fd << " ";
            if (select_.InWrite(fd))
                oss << "w" << fd << " ";
            if (select_.InException(fd))
                oss << "e" << fd << " ";
        }

        LOG << "Performing select() on " << oss.str();
    }

    int r = fdset.select_timeout(static_cast<double>(timeout.count()));

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

    for (int fd = 3; fd < static_cast<int>(watch_.size()); ++fd)
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
                    select_.ClearRead(fd);
                    if (w->write_cb.size() == 0 && !w->except_cb) {
                        // if also all write callbacks are done, stop
                        // listening.
                        select_.ClearWrite(fd);
                        select_.ClearException(fd);
                        w->active = false;
                    }
                }
            }
            else {
                LOG << "SelectDispatcher: got read event for fd "
                    << fd << " without a read handler.";

                select_.ClearRead(fd);
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
                    select_.ClearWrite(fd);
                    if (w->read_cb.size() == 0 && !w->except_cb) {
                        // if also all write callbacks are done, stop
                        // listening.
                        select_.ClearRead(fd);
                        select_.ClearException(fd);
                        w->active = false;
                    }
                }
            }
            else {
                LOG << "SelectDispatcher: got write event for fd "
                    << fd << " without a write handler.";

                select_.ClearWrite(fd);
            }
        }

        if (fdset.InException(fd))
        {
            if (w->except_cb) {
                if (!w->except_cb()) {
                    w = &watch_[fd];
                    // callback returned false: remove fd from set
                    select_.ClearException(fd);
                }
                w = &watch_[fd];
            }
            else {
                DefaultExceptionCallback();
            }
        }
    }
}

} // namespace tcp
} // namespace net
} // namespace thrill

/******************************************************************************/
