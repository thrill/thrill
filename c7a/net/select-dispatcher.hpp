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

#ifndef C7A_NET_SELECT_DISPATCHER_HEADER
#define C7A_NET_SELECT_DISPATCHER_HEADER

#include <c7a/net/socket.hpp>
#include <c7a/net/select.hpp>

#include <vector>

namespace c7a {

//! \addtogroup netsock Low Level Socket API
//! \{

/**
 * SelectSocket is a higher level wrapper for select(). One can register Socket
 * objects for readability and writability checks, buffered reads and writes
 * with completion callbacks, and also timer functions.
 */
class SelectDispatcher : protected Select
{
public:
    typedef std::function<bool (Socket&)> Callback;

    //! Register a socket for readability and exception checks
    void AddRead(Socket& s)
    {
        Select::SetRead(s.GetFileDescriptor());
        Select::SetException(s.GetFileDescriptor());
        watch_.push_back(
            Watch { s.GetFileDescriptor(), s, nullptr, nullptr, nullptr });
    }

    //! Register a socket for writability and exception checks
    void AddWrite(Socket& s)
    {
        Select::SetWrite(s.GetFileDescriptor());
        Select::SetException(s.GetFileDescriptor());
        watch_.push_back(
            Watch { s.GetFileDescriptor(), s, nullptr, nullptr, nullptr });
    }

    //! Register a buffered read callback and a default exception callback.
    void HookRead(Socket& s, const Callback& read_cb)
    {
        Select::SetRead(s.GetFileDescriptor());
        Select::SetException(s.GetFileDescriptor());
        watch_.push_back(
            Watch { s.GetFileDescriptor(), s,
                    read_cb, nullptr, ExceptionCallback });
    }

    //! Register a buffered write callback and a default exception callback.
    void HookWrite(Socket& s, const Callback& write_cb)
    {
        Select::SetRead(s.GetFileDescriptor());
        Select::SetException(s.GetFileDescriptor());
        watch_.push_back(
            Watch { s.GetFileDescriptor(), s,
                    write_cb, nullptr, ExceptionCallback });
    }

    int Dispatch(std::vector<Socket*>& read_set,
                 std::vector<Socket*>& write_set,
                 std::vector<Socket*>& except_set)
    {
        // copy select fdset
        Select fdset = *this;

        int r = fdset.select(10 * 1000);

        if (r < 0) {
            throw NetException("OpenConnections() select() failed!", errno);
        }
        if (r == 0) {
            throw NetException("OpenConnections() timeout in select().", errno);
        }

        for (Watch& w : watch_)
        {
            if (w.fd < 0) continue;

            if (fdset.InRead(w.fd))
            {
                if (w.read_cb) {
                    // have to clear the read flag since the callback may add a new
                    // (other) callback for the same fd.
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
                    read_set.push_back(&w.socket);
                }
            }

            if (fdset.InWrite(w.fd))
            {
                if (w.write_cb) {
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
                    write_set.push_back(&w.socket);
                }
            }

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
                    except_set.push_back(&w.socket);
                }
            }
        }
    }

private:
    //! struct to entries per watched file descriptor
    struct Watch
    {
        int      fd;
        Socket&  socket;
        Callback read_cb, write_cb, except_cb;
    };

    //! Handlers for all registered file descriptors
    std::vector<Watch> watch_;

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

} // namespace c7a

#endif // !C7A_NET_SELECT_DISPATCHER_HEADER

/******************************************************************************/
