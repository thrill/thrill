/*******************************************************************************
 * c7a/net/lowlevel/select_dispatcher.hpp
 *
 * Asynchronous callback wrapper around select()
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_NET_LOWLEVEL_SELECT_DISPATCHER_HEADER
#define C7A_NET_LOWLEVEL_SELECT_DISPATCHER_HEADER

#include <c7a/common/config.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/core/allocator.hpp>
#include <c7a/net/exception.hpp>
#include <c7a/net/lowlevel/select.hpp>
#include <c7a/net/lowlevel/socket.hpp>

#include <cerrno>
#include <chrono>
#include <deque>
#include <functional>
#include <vector>

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
    //! constructor
    SelectDispatcher(core::MemoryManager& memory_manager)
        : memory_manager_(memory_manager) { }

    //! type for file descriptor readiness callbacks
    typedef std::function<bool ()> Callback;

    //! Grow table if needed
    void CheckSize(int fd) {
        assert(fd >= 0);
        assert(fd <= 32000); // this is an arbitrary limit to catch errors.
        if (static_cast<size_t>(fd) >= watch_.size())
            watch_.resize(fd + 1, Watch(memory_manager_));
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
    core::MemoryManager& memory_manager_;

    //! callback vectors per watched file descriptor
    struct Watch
    {
        //! boolean check whether any callbacks are registered
        bool                     active;
        //! queue of callbacks for fd.
        core::mm_deque<Callback> read_cb, write_cb;
        //! only one exception callback for the fd.
        Callback                 except_cb = nullptr;

        Watch(core::MemoryManager& memory_manager)
            : read_cb(core::Allocator<Callback>(memory_manager)),
              write_cb(core::Allocator<Callback>(memory_manager)) { }
    };

    //! handlers for all registered file descriptors. the fd integer range
    //! should be small enough, otherwise a more complicated data structure is
    //! needed.
    core::mm_vector<Watch> watch_ { core::Allocator<Watch>(memory_manager_) };

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
