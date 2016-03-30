/*******************************************************************************
 * thrill/net/mpi/dispatcher.hpp
 *
 * A Thrill network layer Implementation which uses MPI to transmit messages to
 * peers. See group.hpp for more.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
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
    static constexpr bool debug = false;

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
        watch_active_++;
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
        watch_active_++;
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
        watch_active_--;
    }

    MPI_Request ISend(Connection& c, const void* data, size_t size) {
        MPI_Request request;
        int r = MPI_Isend(const_cast<void*>(data), static_cast<int>(size), MPI_BYTE,
                          c.peer(), group_tag_, MPI_COMM_WORLD, &request);

        if (r != MPI_SUCCESS)
            throw Exception("Error during ISend", r);

        sLOG0 << "Isend size" << size;
        c.tx_bytes_ += size;

        return request;
    }

    void AsyncWrite(
        net::Connection& c, Buffer&& buffer,
        const AsyncWriteCallback& done_cb = AsyncWriteCallback()) final {
        assert(c.IsValid());

        if (buffer.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        assert(dynamic_cast<Connection*>(&c));
        Connection* mpic = static_cast<Connection*>(&c);

        // perform Isend.
        MPI_Request req = ISend(*mpic, buffer.data(), buffer.size());

        // store request and associated buffer (Isend needs memory).
        mpi_async_requests_.emplace_back(req);
        mpi_async_.emplace_back(c, std::move(buffer), done_cb);
        mpi_async_out_.emplace_back();
        mpi_async_status_.emplace_back();
    }

    void AsyncWrite(
        net::Connection& c, const data::PinnedBlock& block,
        const AsyncWriteCallback& done_cb = AsyncWriteCallback()) final {
        assert(c.IsValid());

        if (block.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        assert(dynamic_cast<Connection*>(&c));
        Connection* mpic = static_cast<Connection*>(&c);

        // perform Isend.
        MPI_Request req = ISend(*mpic, block.data_begin(), block.size());

        // store request and associated data::Block (Isend needs memory).
        mpi_async_requests_.emplace_back(req);
        mpi_async_.emplace_back(c, block, done_cb);
        mpi_async_out_.emplace_back();
        mpi_async_status_.emplace_back();
    }

    MPI_Request IRecv(Connection& c, void* data, size_t size) {
        MPI_Request request;
        int r = MPI_Irecv(data, static_cast<int>(size), MPI_BYTE,
                          c.peer(), group_tag_, MPI_COMM_WORLD, &request);

        if (r != MPI_SUCCESS)
            throw Exception("Error during IRecv", r);

        sLOG0 << "Irecv size" << size;
        c.rx_bytes_ += size;

        return request;
    }

    void AsyncRead(net::Connection& c, size_t n,
                   const AsyncReadCallback& done_cb = AsyncReadCallback()) final {
        assert(c.IsValid());

        if (n == 0) {
            if (done_cb) done_cb(c, Buffer());
            return;
        }

        assert(dynamic_cast<Connection*>(&c));
        Connection* mpic = static_cast<Connection*>(&c);

        // allocate associated buffer (Irecv needs memory).
        mpi_async_.emplace_back(c, n, done_cb);
        mpi_async_out_.emplace_back();
        mpi_async_status_.emplace_back();

        Buffer& buffer = mpi_async_.back().read_buffer_.buffer();

        // perform Irecv.
        MPI_Request req = IRecv(*mpic, buffer.data(), buffer.size());
        mpi_async_requests_.emplace_back(req);
    }

    void AsyncRead(net::Connection& c, size_t n,
                   data::PinnedByteBlockPtr&& block,
                   const AsyncReadByteBlockCallback& done_cb) final {
        assert(c.IsValid());
        assert(block.valid());

        if (block->size() == 0) {
            if (done_cb) done_cb(c, std::move(block));
            return;
        }

        assert(dynamic_cast<Connection*>(&c));
        Connection* mpic = static_cast<Connection*>(&c);

        // associated Block's memory (Irecv needs memory).

        // perform Irecv.
        MPI_Request req = IRecv(*mpic, block->data(), n);
        mpi_async_.emplace_back(c, n, std::move(block), done_cb);
        mpi_async_out_.emplace_back();
        mpi_async_requests_.emplace_back(req);
        mpi_async_status_.emplace_back();
    }

    //! Run one iteration of dispatching using MPI_Iprobe().
    void DispatchOne(const std::chrono::milliseconds& /* timeout */) final;

    //! Interrupt does nothing.
    void Interrupt() final { }

private:
    //! group_tag attached to this Dispatcher
    int group_tag_;

    //! callback vectors per peer
    struct Watch
    {
        //! boolean check whether any callbacks are registered
        bool                 active = false;
        //! queue of callbacks for peer.
        mem::deque<Callback> read_cb;
        //! only one exception callback for the peer.
        Callback             except_cb;

        explicit Watch(mem::Manager& mem_manager)
            : read_cb(mem::Allocator<Callback>(mem_manager)) { }
    };

    //! callback watch vector
    mem::vector<Watch> watch_ { mem::Allocator<Watch>(mem_manager_) };

    //! counter of active watches
    size_t watch_active_ { 0 };

    //! Default exception handler
    static bool DefaultExceptionCallback() {
        throw Exception("SelectDispatcher() exception on socket!", errno);
    }

    /**************************************************************************/

    /*!
     * This is the big answer to what happens when an MPI async request is
     * signaled as complete: it unifies all possible async requests, including
     * the reference counts they hold on the appropriate buffers, and dispatches
     * the correct callbacks when done.
     */
    class MpiAsync
    {
    public:
        enum Type {
            NONE,
            WRITE_BUFFER, READ_BUFFER,
            WRITE_BLOCK, READ_BYTE_BLOCK
        };

        //! default constructor for resize
        MpiAsync() : type_(NONE) { }

        //! Construct AsyncWrite with Buffer
        MpiAsync(net::Connection& conn,
                 Buffer&& buffer,
                 const AsyncWriteCallback& callback)
            : type_(WRITE_BUFFER),
              write_buffer_(conn, std::move(buffer), callback) { }

        //! Construct AsyncRead with Buffer
        MpiAsync(net::Connection& conn,
                 size_t buffer_size, const AsyncReadCallback& callback)
            : type_(READ_BUFFER),
              read_buffer_(conn, buffer_size, callback) { }

        //! Construct AsyncWrite with Block
        MpiAsync(net::Connection& conn,
                 const data::PinnedBlock& block,
                 const AsyncWriteCallback& callback)
            : type_(WRITE_BLOCK),
              write_block_(conn, block, callback) { }

        //! Construct AsyncRead with ByteBuffer
        MpiAsync(net::Connection& conn,
                 size_t n,
                 data::PinnedByteBlockPtr&& block,
                 const AsyncReadByteBlockCallback& callback)
            : type_(READ_BYTE_BLOCK),
              read_byte_block_(conn, n, std::move(block), callback) { }

        //! copy-constructor: default (work as long as union members are default
        //! copyable)
        MpiAsync(const MpiAsync& ma) = default;

        //! move-constructor: move item
        MpiAsync(MpiAsync&& ma)
            : type_(ma.type_) {
            Acquire(std::move(ma));
            ma.type_ = NONE;
        }

        //! move-assignment
        MpiAsync& operator = (MpiAsync&& ma) noexcept {
            if (this == &ma) return *this;

            // destroy self (yes, the destructor is just a function)
            this->~MpiAsync();
            // move item.
            type_ = ma.type_;
            Acquire(std::move(ma));
            // release other
            ma.type_ = NONE;

            return *this;
        }

        ~MpiAsync() {
            // call the active content's destructor
            if (type_ == WRITE_BUFFER)
                write_buffer_.~AsyncWriteBuffer();
            else if (type_ == READ_BUFFER)
                read_buffer_.~AsyncReadBuffer();
            else if (type_ == WRITE_BLOCK)
                write_block_.~AsyncWriteBlock();
            else if (type_ == READ_BYTE_BLOCK)
                read_byte_block_.~AsyncReadByteBlock();
        }

        //! Dispatch done message to correct callback.
        void operator () () {
            if (type_ == WRITE_BUFFER)
                write_buffer_.DoCallback();
            else if (type_ == READ_BUFFER)
                read_buffer_.DoCallback();
            else if (type_ == WRITE_BLOCK)
                write_block_.DoCallback();
            else if (type_ == READ_BYTE_BLOCK)
                read_byte_block_.DoCallback();
        }

    private:
        //! type of this async
        Type type_;

        //! the big unification of async receivers. these also hold reference
        //! counts on the Buffer or Block objects.
        union {
            AsyncWriteBuffer   write_buffer_;
            AsyncReadBuffer    read_buffer_;
            AsyncWriteBlock    write_block_;
            AsyncReadByteBlock read_byte_block_;
        };

        //! assign myself the other object's content
        void Acquire(MpiAsync&& ma) noexcept {
            assert(type_ == ma.type_);
            // yes, this placement movement into the correct union component.
            if (type_ == WRITE_BUFFER)
                new (&write_buffer_)AsyncWriteBuffer(std::move(ma.write_buffer_));
            else if (type_ == READ_BUFFER)
                new (&read_buffer_)AsyncReadBuffer(std::move(ma.read_buffer_));
            else if (type_ == WRITE_BLOCK)
                new (&write_block_)AsyncWriteBlock(std::move(ma.write_block_));
            else if (type_ == READ_BYTE_BLOCK)
                new (&read_byte_block_)AsyncReadByteBlock(std::move(ma.read_byte_block_));
        }

        //! for direct access to union
        friend class Dispatcher;
    };

    //! array of asynchronous writers and readers (these have to align with
    //! mpi_async_requests_)
    mem::vector<MpiAsync> mpi_async_ {
        mem::Allocator<MpiAsync>(mem_manager_)
    };

    //! array of current async MPI_Request for MPI_Testsome().
    mem::vector<MPI_Request> mpi_async_requests_ {
        mem::Allocator<MPI_Request>(mem_manager_)
    };

    //! array of output integer of finished requests for MPI_Testsome().
    mem::vector<int> mpi_async_out_ {
        mem::Allocator<int>(mem_manager_)
    };

    //! array of output status of finished requests for MPI_Testsome().
    mem::vector<MPI_Status> mpi_async_status_ {
        mem::Allocator<MPI_Status>(mem_manager_)
    };
};

//! \}

} // namespace mpi
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MPI_DISPATCHER_HEADER

/******************************************************************************/
