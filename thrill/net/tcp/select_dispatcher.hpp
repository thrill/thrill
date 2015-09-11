/*******************************************************************************
 * thrill/net/tcp/select_dispatcher.hpp
 *
 * Asynchronous callback wrapper around select()
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_TCP_SELECT_DISPATCHER_HEADER
#define THRILL_NET_TCP_SELECT_DISPATCHER_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/delegate.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/porting.hpp>
#include <thrill/mem/allocator.hpp>
#include <thrill/net/connection.hpp>
#include <thrill/net/dispatcher.hpp>
#include <thrill/net/exception.hpp>
#include <thrill/net/tcp/connection.hpp>
#include <thrill/net/tcp/select.hpp>
#include <thrill/net/tcp/socket.hpp>

#include <cerrno>
#include <chrono>
#include <csignal>
#include <deque>
#include <functional>
#include <vector>

namespace thrill {
namespace net {
namespace tcp {

//! \addtogroup net_tcp TCP Socket API
//! \{

/**
 * SelectDispatcher is a higher level wrapper for select(). One can register
 * Socket objects for readability and writability checks, buffered reads and
 * writes with completion callbacks, and also timer functions.
 */
class SelectDispatcher final : public net::Dispatcher
{
    static const bool debug = false;

    static const bool self_verify_ = common::g_self_verify;

public:
    //! type for file descriptor readiness callbacks
    using Callback = AsyncCallback;

    //! constructor
    explicit SelectDispatcher(mem::Manager& mem_manager)
        : net::Dispatcher(mem_manager) {
        // allocate self-pipe
        common::make_pipe(self_pipe_);

        // Ignore PIPE signals (received when writing to closed sockets)
        signal(SIGPIPE, SIG_IGN);

        // wait interrupts via self-pipe.
        AddRead(self_pipe_[0],
                Callback::from<SelectDispatcher,
                               & SelectDispatcher::SelfPipeCallback>(this));
    }

    ~SelectDispatcher() {
        ::close(self_pipe_[0]);
        ::close(self_pipe_[1]);
    }

    //! Grow table if needed
    void CheckSize(int fd) {
        assert(fd >= 0);
        assert(fd <= 32000); // this is an arbitrary limit to catch errors.
        if (static_cast<size_t>(fd) >= watch_.size())
            watch_.resize(fd + 1, Watch(mem_manager_));
    }

    //! Register a buffered read callback and a default exception callback.
    void AddRead(int fd, const Callback& read_cb) {
        CheckSize(fd);
        if (!watch_[fd].read_cb.size()) {
            select_.SetRead(fd);
            select_.SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].read_cb.emplace_back(read_cb);
    }

    //! Register a buffered read callback and a default exception callback.
    void AddRead(net::Connection& c, const Callback& read_cb) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& tc = static_cast<Connection&>(c);
        int fd = tc.GetSocket().fd();
        return AddRead(fd, read_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(net::Connection& c, const Callback& write_cb) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& tc = static_cast<Connection&>(c);
        int fd = tc.GetSocket().fd();
        CheckSize(fd);
        if (!watch_[fd].write_cb.size()) {
            select_.SetWrite(fd);
            select_.SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].write_cb.emplace_back(write_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void SetExcept(net::Connection& c, const Callback& except_cb) {
        assert(dynamic_cast<Connection*>(&c));
        Connection& tc = static_cast<Connection&>(c);
        int fd = tc.GetSocket().fd();
        CheckSize(fd);
        if (!watch_[fd].except_cb) {
            select_.SetException(fd);
        }
        watch_[fd].active = true;
        watch_[fd].except_cb = except_cb;
    }

    //! Cancel all callbacks on a given fd.
    void Cancel(net::Connection& c) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& tc = static_cast<Connection&>(c);
        int fd = tc.GetSocket().fd();
        CheckSize(fd);

        if (watch_[fd].read_cb.size() == 0 &&
            watch_[fd].write_cb.size() == 0)
            LOG << "SelectDispatcher::Cancel() fd=" << fd
                << " called with no callbacks registered.";

        select_.ClearRead(fd);
        select_.ClearWrite(fd);
        select_.ClearException(fd);

        Watch& w = watch_[fd];
        w.read_cb.clear();
        w.write_cb.clear();
        w.except_cb = Callback();
        w.active = false;
    }

    //! Run one iteration of dispatching select().
    void DispatchOne(const std::chrono::milliseconds& timeout) final;

    //! Interrupt the current select via self-pipe
    void Interrupt() final {
        // there are multiple very platform-dependent ways to do this. we'll try
        // to use the self-pipe trick for now. The select() method waits on
        // another fd, which we write one byte to when we need to interrupt the
        // select().

        // another method would be to send a signal() via pthread_kill() to the
        // select thread, but that had a race condition for waking up the other
        // thread. -tb

        // send one byte to wake up the select() handler.
        ssize_t wb;
        while ((wb = write(self_pipe_[1], this, 1)) == 0) {
            LOG1 << "WakeUp: error sending to self-pipe: " << errno;
        }
        die_unless(wb == 1);
    }

protected:
    //! select() manager object
    Select select_;

    //! self-pipe to wake up select.
    int self_pipe_[2];

    //! buffer to receive one byte from self-pipe
    int self_pipe_buffer_;

    //! callback vectors per watched file descriptor
    struct Watch
    {
        //! boolean check whether any callbacks are registered
        bool                    active = false;
        //! queue of callbacks for fd.
        mem::mm_deque<Callback> read_cb, write_cb;
        //! only one exception callback for the fd.
        Callback                except_cb;

        explicit Watch(mem::Manager& mem_manager)
            : read_cb(mem::Allocator<Callback>(mem_manager)),
              write_cb(mem::Allocator<Callback>(mem_manager)) { }
    };

    //! handlers for all registered file descriptors. the fd integer range
    //! should be small enough, otherwise a more complicated data structure is
    //! needed.
    mem::mm_vector<Watch> watch_ { mem::Allocator<Watch>(mem_manager_) };

    //! Default exception handler
    static bool DefaultExceptionCallback() {
        throw Exception("SelectDispatcher() exception on socket!", errno);
    }

    //! Self-pipe callback
    bool SelfPipeCallback() {
        ssize_t rb;
        while ((rb = read(self_pipe_[0], &self_pipe_buffer_, 1)) == 0) {
            LOG1 << "Work: error reading from self-pipe: " << errno;
        }
        die_unless(rb == 1);
        return true;
    }
};

//! \}

} // namespace tcp
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_TCP_SELECT_DISPATCHER_HEADER

/******************************************************************************/
