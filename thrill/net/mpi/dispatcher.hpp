/*******************************************************************************
 * thrill/net/mpi/dispatcher.hpp
 *
 * A Thrill network layer Implementation which uses MPI to transmit messages to
 * peers. See group.hpp for more.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MPI_DISPATCHER_HEADER
#define THRILL_NET_MPI_DISPATCHER_HEADER

#include <thrill/net/dispatcher.hpp>
#include <thrill/net/group.hpp>
#include <thrill/net/mpi/group.hpp>

#include <algorithm>
#include <deque>
#include <string>
#include <vector>

namespace thrill {
namespace net {
namespace mpi {

//! \addtogroup net_mpi MPI Network API
//! \ingroup net
//! \{

class Dispatcher final : public net::Dispatcher
{
    static const bool debug = false;

public:
    //! type for file descriptor readiness callbacks
    using Callback = AsyncCallback;

    //! constructor
    Dispatcher(mem::Manager& mem_manager,
               size_t group_tag, size_t group_size)
        : net::Dispatcher(mem_manager),
          group_tag_(group_tag) {
        watch_.reserve(group_size);
        for (size_t i = 0; i < group_size; ++i)
            watch_.emplace_back(mem_manager_);
    }

    //! Register a buffered read callback and a default exception callback.
    void AddRead(net::Connection& c, const Callback& read_cb) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& mc = static_cast<Connection&>(c);
        size_t p = mc.peer();
        assert(p < watch_.size());
        watch_[p].active = true;
        watch_[p].read_cb.emplace_back(read_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void AddWrite(net::Connection& c, const Callback& write_cb) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& mc = static_cast<Connection&>(c);
        size_t p = mc.peer();
        assert(p < watch_.size());
        watch_[p].active = true;
        watch_[p].write_cb.emplace_back(write_cb);
    }

    //! Register a buffered write callback and a default exception callback.
    void SetExcept(net::Connection& c, const Callback& except_cb) {
        assert(dynamic_cast<Connection*>(&c));
        Connection& mc = static_cast<Connection&>(c);
        size_t p = mc.peer();
        assert(p < watch_.size());
        watch_[p].active = true;
        watch_[p].except_cb = except_cb;
    }

    //! Cancel all callbacks on a given peer.
    void Cancel(net::Connection& c) final {
        assert(dynamic_cast<Connection*>(&c));
        Connection& mc = static_cast<Connection&>(c);
        size_t p = mc.peer();
        assert(p < watch_.size());

        if (watch_[p].read_cb.size() == 0 &&
            watch_[p].write_cb.size() == 0)
            LOG << "SelectDispatcher::Cancel() peer=" << p
                << " called with no callbacks registered.";

        Watch& w = watch_[p];
        w.read_cb.clear();
        w.write_cb.clear();
        w.except_cb = Callback();
        w.active = false;
    }

    //! Run one iteration of dispatching using MPI_Iprobe().
    void DispatchOne(const std::chrono::milliseconds& /* timeout */) final;

    //! Interrupt does nothing.
    void Interrupt() final { }

protected:
    //! group_tag attached to this Dispatcher
    size_t group_tag_;

    //! callback vectors per peer
    struct Watch
    {
        //! boolean check whether any callbacks are registered
        bool                    active = false;
        //! queue of callbacks for peer.
        mem::mm_deque<Callback> read_cb, write_cb;
        //! only one exception callback for the peer.
        Callback                except_cb;

        explicit Watch(mem::Manager& mem_manager)
            : read_cb(mem::Allocator<Callback>(mem_manager)),
              write_cb(mem::Allocator<Callback>(mem_manager)) { }
    };

    //! handlers for each MPI peer.
    mem::mm_vector<Watch> watch_ { mem::Allocator<Watch>(mem_manager_) };

    //! Default exception handler
    static bool DefaultExceptionCallback() {
        throw Exception("SelectDispatcher() exception on socket!", errno);
    }
};

//! \}

} // namespace mpi
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MPI_DISPATCHER_HEADER

/******************************************************************************/
