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
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_NET_MPI_DISPATCHER_HEADER
#define THRILL_NET_MPI_DISPATCHER_HEADER

#include <thrill/net/dispatcher.hpp>
#include <thrill/net/group.hpp>
#include <thrill/net/mpi/group.hpp>

#include <mpi.h>

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
               int group_tag, size_t group_size)
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
    void AddWrite(net::Connection& /* c */, const Callback& /* write_cb */) final {
        // abort: this is not implemented. use AsyncWrites.
        abort();
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

        if (watch_[p].read_cb.size() == 0)
            LOG << "SelectDispatcher::Cancel() peer=" << p
                << " called with no callbacks registered.";

        Watch& w = watch_[p];
        w.read_cb.clear();
        w.except_cb = Callback();
        w.active = false;
    }

    MPI_Request ISend(size_t peer_, const void* data, size_t size) {
        MPI_Request request;
        int r = MPI_Isend(const_cast<void*>(data), static_cast<int>(size), MPI_BYTE,
                          peer_, group_tag_, MPI_COMM_WORLD, &request);

        if (r != MPI_SUCCESS)
            throw Exception("Error during SyncOne", r);

        return request;
    }

    void AsyncWrite(net::Connection& c, Buffer&& buffer,
                    AsyncWriteCallback done_cb = AsyncWriteCallback()) final {
        assert(c.IsValid());

        if (buffer.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        assert(dynamic_cast<Connection*>(&c));
        Connection* mpic = static_cast<Connection*>(&c);

        // perform Isend.
        MPI_Request req = ISend(mpic->peer(), buffer.data(), buffer.size());

        // store request and associated buffer (Isend need memory).
        mpi_async_requests_.emplace_back(req);
        mpi_async_write_.emplace_back(done_cb, std::move(buffer));
        mpi_async_out_.emplace_back();
        mpi_async_status_.emplace_back();
    }

    void AsyncWrite(net::Connection& c, const data::Block& block,
                    AsyncWriteCallback done_cb = AsyncWriteCallback()) final {
        assert(c.IsValid());

        if (block.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        assert(dynamic_cast<Connection*>(&c));
        Connection* mpic = static_cast<Connection*>(&c);

        // perform Isend.
        MPI_Request req = ISend(mpic->peer(), block.data_begin(), block.size());

        // store request and associated data::Block (Isend need memory).
        mpi_async_requests_.emplace_back(req);
        mpi_async_write_.emplace_back(done_cb, block);
        mpi_async_out_.emplace_back();
        mpi_async_status_.emplace_back();
    }

    //! Run one iteration of dispatching using MPI_Iprobe().
    void DispatchOne(const std::chrono::milliseconds& /* timeout */) final;

    //! Interrupt does nothing.
    void Interrupt() final { }

protected:
    //! group_tag attached to this Dispatcher
    int group_tag_;

    //! callback vectors per peer
    struct Watch
    {
        //! boolean check whether any callbacks are registered
        bool                    active = false;
        //! queue of callbacks for peer.
        mem::mm_deque<Callback> read_cb;
        //! only one exception callback for the peer.
        Callback                except_cb;

        explicit Watch(mem::Manager& mem_manager)
            : read_cb(mem::Allocator<Callback>(mem_manager)) { }
    };

    //! callback watch vector
    mem::mm_vector<Watch> watch_ { mem::Allocator<Watch>(mem_manager_) };

    //! Default exception handler
    static bool DefaultExceptionCallback() {
        throw Exception("SelectDispatcher() exception on socket!", errno);
    }

    /**************************************************************************/

    class MpiAsyncBuffer
    {
    public:
        //! default constructor for resize
        MpiAsyncBuffer() = default;

        //! Construct buffered writer with callback
        MpiAsyncBuffer(const AsyncWriteCallback& callback,
                       Buffer&& buffer)
            : callback_(callback),
              buffer_(std::move(buffer)) { }

        //! Construct buffered writer with callback
        MpiAsyncBuffer(const AsyncWriteCallback& callback,
                       const data::Block& block)
            : callback_(callback),
              block_(block) { }

    protected:
        //! functional object to call once data is complete
        AsyncWriteCallback callback_;

        //! Send buffer (owned by this async)
        Buffer buffer_;

        //! Send block (owned by this async)
        data::Block block_;
    };

    //! deque of asynchronous writers
    mem::mm_vector<MpiAsyncBuffer> mpi_async_write_ {
        mem::Allocator<MpiAsyncBuffer>(mem_manager_)
    };

    //! array of current async MPI_Request for MPI_Testsome().
    mem::mm_vector<MPI_Request> mpi_async_requests_ {
        mem::Allocator<MPI_Request>(mem_manager_)
    };

    //! array of output integer of finished requests for MPI_Testsome().
    mem::mm_vector<int> mpi_async_out_ {
        mem::Allocator<int>(mem_manager_)
    };

    //! array of output status of finished requests for MPI_Testsome().
    mem::mm_vector<MPI_Status> mpi_async_status_ {
        mem::Allocator<MPI_Status>(mem_manager_)
    };
};

//! \}

} // namespace mpi
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MPI_DISPATCHER_HEADER

/******************************************************************************/
