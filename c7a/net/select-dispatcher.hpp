/*******************************************************************************
 * c7a/net/select-dispatcher.hpp
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
#ifndef C7A_NET_SELECT_DISPATCHER_HEADER
#define C7A_NET_SELECT_DISPATCHER_HEADER

#include <c7a/net/socket.hpp>
#include <c7a/net/select.hpp>
#include <c7a/net/net-exception.hpp>

#include <deque>

namespace c7a {

namespace net {

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

public:
    typedef std::function<bool (Socket&)> Callback;

    //! Register a buffered read callback and a default exception callback.
    void AddRead(Socket& s, const Callback& read_cb)
    {
        Select::SetRead(s.GetFileDescriptor());
        Select::SetException(s.GetFileDescriptor());
        watch_.emplace_back(s.GetFileDescriptor(), s,
                            read_cb, nullptr, ExceptionCallback);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(Socket& s, const Callback& write_cb)
    {
        Select::SetWrite(s.GetFileDescriptor());
        Select::SetException(s.GetFileDescriptor());
        watch_.emplace_back(s.GetFileDescriptor(), s,
                            nullptr, write_cb, ExceptionCallback);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddReadWrite(Socket& s,
                      const Callback& read_cb, const Callback& write_cb)
    {
        Select::SetRead(s.GetFileDescriptor());
        Select::SetWrite(s.GetFileDescriptor());
        Select::SetException(s.GetFileDescriptor());
        watch_.emplace_back(s.GetFileDescriptor(), s,
                            read_cb, write_cb, ExceptionCallback);
    }

    void Dispatch(double timeout)
    {
        // copy select fdset
        Select fdset = *this;

        if (debug)
        {
            std::ostringstream oss;
            for (Watch& w : watch_) {
                oss << w.fd << " ";
            }
            oss << "| ";
            for (int i = 0; i < Select::max_fd_ + 1; ++i) {
                if (Select::InRead(i))
                    oss << "r" << i << " ";
                if (Select::InWrite(i))
                    oss << "w" << i << " ";
                if (Select::InException(i))
                    oss << "e" << i << " ";
            }
            LOG << "Performing select() on " << oss.str();
        }

        int r = fdset.select_timeout(timeout);

        if (r < 0) {
            throw NetException("OpenConnections() select() failed!", errno);
        }
        if (r == 0) return;

        // save _current_ size, as it may change.
        size_t watch_size = watch_.size();

        for (size_t i = 0; i != watch_size; ++i)
        {
            Watch& w = watch_[i];

            if (w.fd < 0) continue;

            if (fdset.InRead(w.fd))
            {
                if (w.read_cb) {
                    // have to clear the read flag since the callback may add a
                    // new (other) callback for the same fd.
                    Select::ClearRead(w.fd);
                    Select::ClearException(w.fd);

                    if (!w.read_cb(w.socket)) {
                        // callback returned false: remove fd from set
                        w.fd = -1;
                    }
                    else {
                        Select::SetRead(w.fd);
                        Select::SetException(w.fd);
                    }
                }
                else {
                    LOG << "SelectDispatcher: got read event for fd "
                        << w.fd << " without a read handler.";

                    Select::ClearRead(w.fd);
                }
            }

            if (w.fd < 0) continue;

            if (fdset.InWrite(w.fd))
            {
                if (w.write_cb) {
                    // have to clear the read flag since the callback may add a
                    // new (other) callback for the same fd.
                    Select::ClearWrite(w.fd);
                    Select::ClearException(w.fd);

                    if (!w.write_cb(w.socket)) {
                        // callback returned false: remove fd from set
                        w.fd = -1;
                    }
                    else {
                        Select::SetWrite(w.fd);
                        Select::SetException(w.fd);
                    }
                }
                else {
                    LOG << "SelectDispatcher: got write event for fd "
                        << w.fd << " without a write handler.";

                    Select::ClearWrite(w.fd);
                }
            }

            if (w.fd < 0) continue;

            if (fdset.InException(w.fd))
            {
                if (w.except_cb) {
                    Select::ClearException(w.fd);

                    if (!w.except_cb(w.socket)) {
                        // callback returned false: remove fd from set
                        w.fd = -1;
                    }
                    else {
                        Select::SetException(w.fd);
                    }
                }
                else {
                    LOG << "SelectDispatcher: got exception event for fd "
                        << w.fd << " without an exception handler.";

                    Select::ClearException(w.fd);
                }
            }
        }
    }

private:
    //! struct to entries per watched file descriptor
    struct Watch
    {
        int      fd;
        Socket   socket;
        Callback read_cb, write_cb, except_cb;

        Watch(int _fd, Socket& _socket,
              const Callback& _read_cb, const Callback& _write_cb,
              const Callback& _except_cb)
            : fd(_fd),
              socket(_socket),
              read_cb(_read_cb),
              write_cb(_write_cb),
              except_cb(_except_cb)
        { }
    };

    //! handlers for all registered file descriptors, we have to keep them
    //! address local.
    std::deque<Watch> watch_;

    //! Default exception handler
    static bool ExceptionCallback(Socket& s)
    {
        // exception on listen socket ?
        throw NetException("SelectDispatcher() exception on socket fd "
                           + std::to_string(s.GetFileDescriptor()) + "!",
                           errno);
    }
};

//! \}

} // namespace net

} // namespace c7a

#endif // !C7A_NET_SELECT_DISPATCHER_HEADER

/******************************************************************************/
