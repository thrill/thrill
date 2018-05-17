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

#define THRILL_NET_MPI_QUEUES 1

//! Signature of async MPI request callbacks.
using AsyncRequestCallback = tlx::delegate<
    void(MPI_Status&), mem::GPoolAllocator<char> >;

class AsyncRequest
{
public:
    //! Construct buffered reader with callback
    AsyncRequest(const AsyncRequestCallback& callback)
        : callback_(callback) {
        LOGC(debug_async)
            << "AsyncRequest()";
    }

    //! non-copyable: delete copy-constructor
    AsyncRequest(const AsyncRequest&) = delete;
    //! non-copyable: delete assignment operator
    AsyncRequest& operator = (const AsyncRequest&) = delete;
    //! move-constructor: default
    AsyncRequest(AsyncRequest&&) = default;
    //! move-assignment operator: default
    AsyncRequest& operator = (AsyncRequest&&) = default;

    ~AsyncRequest() {
        LOGC(debug_async)
            << "~AsyncRequest()";
    }

    void DoCallback(MPI_Status& s) {
        if (callback_) {
            callback_(s);
            callback_ = AsyncRequestCallback();
        }
    }

private:
    //! functional object to call once data is complete
    AsyncRequestCallback callback_;
};

class Dispatcher final : public net::Dispatcher
{
    static constexpr bool debug = false;

    class MpiAsync;

public:
    //! type for file descriptor readiness callbacks
    using Callback = AsyncCallback;

    //! constructor
    Dispatcher(size_t group_size);

    //! destructor
    ~Dispatcher();

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

    MPI_Request ISend(
        Connection& c, uint32_t seq, const void* data, size_t size);
    MPI_Request IRecv(
        Connection& c, uint32_t seq, void* data, size_t size);

    void AddAsyncRequest(
        const MPI_Request& req, const AsyncRequestCallback& callback);

    void AsyncWrite(
        net::Connection& c, uint32_t seq, Buffer&& buffer,
        const AsyncWriteCallback& done_cb = AsyncWriteCallback()) final {
        assert(c.IsValid());

        if (buffer.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        QueueAsyncSend(c, MpiAsync(c, seq, std::move(buffer), done_cb));
    }

    void AsyncWrite(
        net::Connection& c, uint32_t seq, data::PinnedBlock&& block,
        const AsyncWriteCallback& done_cb = AsyncWriteCallback()) final {
        assert(c.IsValid());

        if (block.size() == 0) {
            if (done_cb) done_cb(c);
            return;
        }

        QueueAsyncSend(c, MpiAsync(c, seq, std::move(block), done_cb));
    }

    void AsyncRead(net::Connection& c, uint32_t seq, size_t size,
                   const AsyncReadBufferCallback& done_cb
                       = AsyncReadBufferCallback()) final {
        assert(c.IsValid());

        if (size == 0) {
            if (done_cb) done_cb(c, Buffer());
            return;
        }

        QueueAsyncRecv(c, MpiAsync(c, seq, size, done_cb));
    }

    void AsyncRead(net::Connection& c, uint32_t seq, size_t size,
                   data::PinnedByteBlockPtr&& block,
                   const AsyncReadByteBlockCallback& done_cb) final {
        assert(c.IsValid());
        assert(block.valid());

        if (block->size() == 0) {
            if (done_cb) done_cb(c, std::move(block));
            return;
        }

        QueueAsyncRecv(c, MpiAsync(c, seq, size, std::move(block), done_cb));
    }

    //! Enqueue and run the encapsulated result
    void QueueAsyncSend(net::Connection& c, MpiAsync&& a);

    //! Enqueue and run the encapsulated result
    void QueueAsyncRecv(net::Connection& c, MpiAsync&& a);

    //! Issue the encapsulated request to the MPI layer
    void PerformAsync(MpiAsync&& a);

    //! Check send queue and perform waiting requests
    void PumpSendQueue(int peer);

    //! Check recv queue and perform waiting requests
    void PumpRecvQueue(int peer);

    //! Run one iteration of dispatching using MPI_Iprobe().
    void DispatchOne(const std::chrono::milliseconds& timeout) final;

    //! Interrupt does nothing.
    void Interrupt() final { }

private:
    //! callback vectors per peer
    struct Watch {
        //! boolean check whether any callbacks are registered
        bool     active = false;
        //! queue of callbacks for peer.
        std::deque<Callback, mem::GPoolAllocator<Callback> >
                 read_cb;
        //! only one exception callback for the peer.
        Callback except_cb;
    };

    //! callback watch vector
    std::vector<Watch> watch_;

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
            REQUEST,
            WRITE_BUFFER, READ_BUFFER,
            WRITE_BLOCK, READ_BYTE_BLOCK
        };

        //! default constructor for resize
        MpiAsync() : type_(NONE), seq_(0) { }

        //! construct generic MPI async request
        MpiAsync(const AsyncRequestCallback& callback)
            : type_(REQUEST), seq_(0),
              arequest_(callback) { }

        //! Construct AsyncWrite with Buffer
        MpiAsync(net::Connection& conn, uint32_t seq,
                 Buffer&& buffer,
                 const AsyncWriteCallback& callback)
            : type_(WRITE_BUFFER), seq_(seq),
              write_buffer_(conn, std::move(buffer), callback) { }

        //! Construct AsyncRead with Buffer
        MpiAsync(net::Connection& conn, uint32_t seq,
                 size_t buffer_size, const AsyncReadBufferCallback& callback)
            : type_(READ_BUFFER), seq_(seq),
              read_buffer_(conn, buffer_size, callback) { }

        //! Construct AsyncWrite with Block
        MpiAsync(net::Connection& conn, uint32_t seq,
                 data::PinnedBlock&& block,
                 const AsyncWriteCallback& callback)
            : type_(WRITE_BLOCK), seq_(seq),
              write_block_(conn, std::move(block), callback) { }

        //! Construct AsyncRead with ByteBuffer
        MpiAsync(net::Connection& conn, uint32_t seq,
                 size_t size,
                 data::PinnedByteBlockPtr&& block,
                 const AsyncReadByteBlockCallback& callback)
            : type_(READ_BYTE_BLOCK), seq_(seq),
              read_byte_block_(conn, size, std::move(block), callback) { }

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
            if (type_ == REQUEST)
                arequest_.~AsyncRequest();
            else if (type_ == WRITE_BUFFER)
                write_buffer_.~AsyncWriteBuffer();
            else if (type_ == READ_BUFFER)
                read_buffer_.~AsyncReadBuffer();
            else if (type_ == WRITE_BLOCK)
                write_block_.~AsyncWriteBlock();
            else if (type_ == READ_BYTE_BLOCK)
                read_byte_block_.~AsyncReadByteBlock();
        }

        //! Dispatch done message to correct callback.
        void DoCallback(MPI_Status& s) {
            if (type_ == REQUEST)
                arequest_.DoCallback(s);
            else if (type_ == WRITE_BUFFER)
                write_buffer_.DoCallback();
            else if (type_ == READ_BUFFER) {
                int size;
                MPI_Get_count(&s, MPI_BYTE, &size);
                read_buffer_.DoCallback(size);
            }
            else if (type_ == WRITE_BLOCK)
                write_block_.DoCallback();
            else if (type_ == READ_BYTE_BLOCK) {
                int size;
                MPI_Get_count(&s, MPI_BYTE, &size);
                read_byte_block_.DoCallback(size);
            }
        }

        //! Return mpi Connection pointer
        Connection * connection() {
            if (type_ == REQUEST)
                return nullptr;
            else if (type_ == WRITE_BUFFER)
                return static_cast<Connection*>(write_buffer_.connection());
            else if (type_ == READ_BUFFER)
                return static_cast<Connection*>(read_buffer_.connection());
            else if (type_ == WRITE_BLOCK)
                return static_cast<Connection*>(write_block_.connection());
            else if (type_ == READ_BYTE_BLOCK)
                return static_cast<Connection*>(read_byte_block_.connection());
            die("Unknown Buffer type");
        }

    private:
        //! type of this async
        Type type_;

        //! sequence id
        uint32_t seq_;

        //! the big unification of async receivers. these also hold reference
        //! counts on the Buffer or Block objects.
        union {
            AsyncRequest       arequest_;
            AsyncWriteBuffer   write_buffer_;
            AsyncReadBuffer    read_buffer_;
            AsyncWriteBlock    write_block_;
            AsyncReadByteBlock read_byte_block_;
        };

        //! assign myself the other object's content
        void Acquire(MpiAsync&& ma) noexcept {
            assert(type_ == ma.type_);
            seq_ = ma.seq_;
            // yes, this placement movement into the correct union component.
            if (type_ == REQUEST)
                new (&arequest_)AsyncRequest(std::move(ma.arequest_));
            else if (type_ == WRITE_BUFFER)
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
    std::vector<MpiAsync> mpi_async_;

    //! array of current async MPI_Request for MPI_Testsome().
    std::vector<MPI_Request> mpi_async_requests_;

    //! array of output integer of finished requests for MPI_Testsome().
    std::vector<int> mpi_async_out_;

    //! array of output MPI_Status of finished requests for MPI_Testsome().
    std::vector<MPI_Status> mpi_status_out_;

#if THRILL_NET_MPI_QUEUES
    //! queue of delayed requests for each peer
    std::deque<std::deque<MpiAsync> > send_queue_;

    //! queue of delayed requests for each peer
    std::deque<std::deque<MpiAsync> > recv_queue_;

    //! number of active requests
    std::vector<size_t> send_active_;

    //! number of active requests
    std::vector<size_t> recv_active_;
#endif
};

//! \}

} // namespace mpi
} // namespace net
} // namespace thrill

#endif // !THRILL_NET_MPI_DISPATCHER_HEADER

/******************************************************************************/
