/*******************************************************************************
 * thrill/net/lowlevel/select_dispatcher.hpp
 *
 * Asynchronous callback wrapper around select()
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_LOWLEVEL_SELECT_DISPATCHER_HEADER
#define THRILL_NET_LOWLEVEL_SELECT_DISPATCHER_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/delegate.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/mem/allocator.hpp>
#include <thrill/net/exception.hpp>
#include <thrill/net/lowlevel/select.hpp>
#include <thrill/net/lowlevel/socket.hpp>

#include <cerrno>
#include <chrono>
#include <deque>
#include <functional>
#include <vector>

namespace thrill {
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
    //! constructor
    SelectDispatcher(mem::Manager& mem_manager)
        : mem_manager_(mem_manager) { }

    //! type for file descriptor readiness callbacks
    using Callback = common::delegate<bool()>;

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

    //! Run one iteration of dispatching select().
    void Dispatch(const std::chrono::milliseconds& timeout);

private:
    //! reference to memory manager
    mem::Manager& mem_manager_;

    //! callback vectors per watched file descriptor
    struct Watch
    {
        //! boolean check whether any callbacks are registered
        bool                    active = false;
        //! queue of callbacks for fd.
        mem::mm_deque<Callback> read_cb, write_cb;
        //! only one exception callback for the fd.
        Callback                except_cb;

        Watch(mem::Manager& mem_manager)
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
};

//! \}

} // namespace lowlevel
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_LOWLEVEL_SELECT_DISPATCHER_HEADER

/******************************************************************************/
