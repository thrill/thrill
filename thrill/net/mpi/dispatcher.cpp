/*******************************************************************************
 * thrill/net/mpi/dispatcher.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/net/mpi/dispatcher.hpp>
#include <tlx/die.hpp>

#include <mpi.h>

#include <mutex>
#include <vector>

namespace thrill {
namespace net {
namespace mpi {

//! The Grand MPI Library Invocation Mutex (The GMLIM)
extern std::mutex g_mutex;

//! number of simultaneous transfers
static const size_t g_simultaneous = 32;

/******************************************************************************/
// mpi::Dispatcher

Dispatcher::Dispatcher(size_t group_size)
    : net::Dispatcher() {

    watch_.resize(group_size);
#if THRILL_NET_MPI_QUEUES
    send_queue_.resize(group_size);
    send_active_.resize(group_size);
    recv_queue_.resize(group_size);
    recv_active_.resize(group_size);
#endif
}

Dispatcher::~Dispatcher() {
    LOG << "~mpi::Dispatcher()"
        << " mpi_async_.size()=" << mpi_async_.size();

    // lock the GMLIM
    std::unique_lock<std::mutex> lock(g_mutex);

    for (size_t i = 0; i < mpi_async_requests_.size(); ++i) {
        int r = MPI_Cancel(&mpi_async_requests_[i]);

        if (r != MPI_SUCCESS)
            LOG1 << "Error during MPI_Cancel()";

        MPI_Request_free(&mpi_async_requests_[i]);
    }
}

MPI_Request Dispatcher::ISend(
    Connection& c, uint32_t seq, const void* data, size_t size) {
    // lock the GMLIM
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Request request;
    int r = MPI_Isend(const_cast<void*>(data), static_cast<int>(size), MPI_BYTE,
                      c.peer(), static_cast<int>(seq),
                      MPI_COMM_WORLD, &request);

    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Isend()", r);

    LOG << "MPI_Isend() data=" << data << " size=" << size
        << " peer=" << c.peer() << " seq=" << seq
        << " request=" << request;

    c.tx_bytes_ += size;

    return request;
}

MPI_Request Dispatcher::IRecv(
    Connection& c, uint32_t seq, void* data, size_t size) {
    // lock the GMLIM
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Request request;
    int r = MPI_Irecv(data, static_cast<int>(size), MPI_BYTE,
                      c.peer(), static_cast<int>(seq),
                      MPI_COMM_WORLD, &request);

    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Irecv()", r);

    LOG << "MPI_Irecv() data=" << data << " size=" << size
        << " peer=" << c.peer() << " seq=" << seq
        << " request=" << request;

    c.rx_bytes_ += size;

    return request;
}

void Dispatcher::AddAsyncRequest(
    const MPI_Request& req, const AsyncRequestCallback& callback) {

    LOG << "AddAsyncRequest() req=" << req;

    // store request and associated buffer (Isend needs memory).
    mpi_async_requests_.emplace_back(req);
    mpi_async_.emplace_back(MpiAsync(callback));
    mpi_async_out_.emplace_back();
    mpi_status_out_.emplace_back();
}

void Dispatcher::QueueAsyncSend(net::Connection& c, MpiAsync&& a) {
#if THRILL_NET_MPI_QUEUES
    assert(dynamic_cast<Connection*>(&c));
    Connection* mpic = static_cast<Connection*>(&c);

    int peer = mpic->peer();
    if (send_active_[peer] < g_simultaneous) {
        // perform immediately
        PerformAsync(std::move(a));
    }
    else {
        send_queue_[peer].emplace_back(std::move(a));
    }
#else
    tlx::unused(c);
    // perform immediately
    PerformAsync(std::move(a));
#endif
}

void Dispatcher::QueueAsyncRecv(net::Connection& c, MpiAsync&& a) {
#if THRILL_NET_MPI_QUEUES
    assert(dynamic_cast<Connection*>(&c));
    Connection* mpic = static_cast<Connection*>(&c);

    int peer = mpic->peer();

    if (recv_active_[peer] < g_simultaneous) {
        // perform immediately
        PerformAsync(std::move(a));
    }
    else {
        recv_queue_[peer].emplace_back(std::move(a));
    }
#else
    tlx::unused(c);
    // perform immediately
    PerformAsync(std::move(a));
#endif
}

void Dispatcher::PumpSendQueue(int peer) {
#if THRILL_NET_MPI_QUEUES
    while (send_active_[peer] < g_simultaneous && !send_queue_[peer].empty()) {
        MpiAsync a = std::move(send_queue_[peer].front());
        send_queue_[peer].pop_front();
        PerformAsync(std::move(a));
    }
    if (!send_queue_[peer].empty()) {
        LOG << "Dispatcher::PumpSendQueue() send remaining="
            << send_queue_[peer].size();
    }
#else
    tlx::unused(peer);
#endif
}

void Dispatcher::PumpRecvQueue(int peer) {
#if THRILL_NET_MPI_QUEUES
    while (recv_active_[peer] < g_simultaneous && !recv_queue_[peer].empty()) {
        MpiAsync a = std::move(recv_queue_[peer].front());
        recv_queue_[peer].pop_front();
        PerformAsync(std::move(a));
    }
    if (!recv_queue_[peer].empty()) {
        LOG << "Dispatcher::PumpRecvQueue(). recv remaining="
            << recv_queue_[peer].size();
    }
#else
    tlx::unused(peer);
#endif
}

void Dispatcher::PerformAsync(MpiAsync&& a) {
    if (a.type_ == MpiAsync::WRITE_BUFFER)
    {
        AsyncWriteBuffer& r = a.write_buffer_;
        assert(dynamic_cast<Connection*>(r.connection()));
        Connection* c = static_cast<Connection*>(r.connection());

        MPI_Request req = ISend(*c, a.seq_, r.data(), r.size());

        // store request and associated buffer (Isend needs memory).
        mpi_async_requests_.emplace_back(req);
        mpi_async_.emplace_back(std::move(a));
        mpi_async_out_.emplace_back();
        mpi_status_out_.emplace_back();

#if THRILL_NET_MPI_QUEUES
        send_active_[c->peer()]++;
#endif
    }
    else if (a.type_ == MpiAsync::WRITE_BLOCK)
    {
        AsyncWriteBlock& r = a.write_block_;
        assert(dynamic_cast<Connection*>(r.connection()));
        Connection* c = static_cast<Connection*>(r.connection());

        MPI_Request req = ISend(*c, a.seq_, r.data(), r.size());

        // store request and associated buffer (Isend needs memory).
        mpi_async_requests_.emplace_back(req);
        mpi_async_.emplace_back(std::move(a));
        mpi_async_out_.emplace_back();
        mpi_status_out_.emplace_back();

#if THRILL_NET_MPI_QUEUES
        send_active_[c->peer()]++;
#endif
    }
    else if (a.type_ == MpiAsync::READ_BUFFER)
    {
        AsyncReadBuffer& r = a.read_buffer_;
        assert(dynamic_cast<Connection*>(r.connection()));
        Connection* c = static_cast<Connection*>(r.connection());

        MPI_Request req = IRecv(*c, a.seq_, r.data(), r.size());

        // store request and associated buffer (Irecv needs memory).
        mpi_async_requests_.emplace_back(req);
        mpi_async_.emplace_back(std::move(a));
        mpi_async_out_.emplace_back();
        mpi_status_out_.emplace_back();

#if THRILL_NET_MPI_QUEUES
        recv_active_[c->peer()]++;
#endif
    }
    else if (a.type_ == MpiAsync::READ_BYTE_BLOCK)
    {
        AsyncReadByteBlock& r = a.read_byte_block_;
        assert(dynamic_cast<Connection*>(r.connection()));
        Connection* c = static_cast<Connection*>(r.connection());

        MPI_Request req = IRecv(*c, a.seq_, r.data(), r.size());

        // store request and associated buffer (Irecv needs memory).
        mpi_async_requests_.emplace_back(req);
        mpi_async_.emplace_back(std::move(a));
        mpi_async_out_.emplace_back();
        mpi_status_out_.emplace_back();

#if THRILL_NET_MPI_QUEUES
        recv_active_[c->peer()]++;
#endif
    }
}

void Dispatcher::DispatchOne(const std::chrono::milliseconds& /* timeout */) {

    // use MPI_Testsome() to check for finished writes
    if (mpi_async_requests_.size())
    {
        // lock the GMLIM
        std::unique_lock<std::mutex> lock(g_mutex);

        die_unless(mpi_async_.size() == mpi_async_requests_.size());
        die_unless(mpi_async_.size() == mpi_async_out_.size());
        die_unless(mpi_async_.size() == mpi_status_out_.size());

#if 1
        int out_count;

        sLOG << "DispatchOne(): MPI_Testsome()"
             << " mpi_async_requests_=" << mpi_async_requests_.size();

        int r = MPI_Testsome(
            // in: Length of array_of_requests (integer).
            static_cast<int>(mpi_async_requests_.size()),
            // in: Array of requests (array of handles).
            mpi_async_requests_.data(),
            // out: Number of completed requests (integer).
            &out_count,
            // out: Array of indices of operations that completed (array of
            // integers).
            mpi_async_out_.data(),
            // out: Array of status objects for operations that completed (array
            // of status).
            mpi_status_out_.data());

        lock.unlock();

        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Testsome()", r);

        if (out_count == MPI_UNDEFINED) {
            // nothing returned
        }
        else if (out_count > 0) {
            sLOG << "DispatchOne(): MPI_Testsome() out_count=" << out_count;

            die_unless(std::is_sorted(mpi_async_out_.begin(),
                                      mpi_async_out_.begin() + out_count));

            // rewrite the arrays, process and remove all finished requests.
            {
                size_t i = 0;
                int k = 0;
                std::vector<size_t> pump_send, pump_recv;

                for (size_t j = 0; j < mpi_async_.size(); ++j)
                {
                    if (k < out_count && mpi_async_out_[k] == static_cast<int>(j)) {

                        sLOG << "Working #" << k
                             << "which is $" << mpi_async_out_[k];

                        // perform callback
                        mpi_async_[j].DoCallback(mpi_status_out_[k]);

#if THRILL_NET_MPI_QUEUES
                        MpiAsync& a = mpi_async_[j];
                        int peer = a.connection() ? a.connection()->peer() : 0;
                        MpiAsync::Type a_type = a.type_;

                        if (a_type == MpiAsync::WRITE_BUFFER ||
                            a_type == MpiAsync::WRITE_BLOCK)
                        {
                            die_unless(send_active_[peer] > 0);
                            send_active_[peer]--;
                            LOG << "DispatchOne() send_active_[" << peer << "]="
                                << send_active_[peer];
                            pump_send.emplace_back(peer);
                        }
                        else if (a_type == MpiAsync::READ_BUFFER ||
                                 a_type == MpiAsync::READ_BYTE_BLOCK)
                        {
                            die_unless(recv_active_[peer] > 0);
                            recv_active_[peer]--;
                            LOG << "DispatchOne() recv_active_[" << peer << "]="
                                << recv_active_[peer];
                            PumpRecvQueue(peer);
                            pump_recv.emplace_back(peer);
                        }
#endif
                        // skip over finished request
                        ++k;
                        continue;
                    }
                    if (i != j) {
                        mpi_async_[i] = std::move(mpi_async_[j]);
                        mpi_async_requests_[i] = std::move(mpi_async_requests_[j]);
                    }
                    ++i;
                }

                mpi_async_.resize(i);
                mpi_async_requests_.resize(i);
                mpi_async_out_.resize(i);
                mpi_status_out_.resize(i);

                for (const size_t & peer : pump_send)
                    PumpSendQueue(peer);
                for (const size_t & peer : pump_recv)
                    PumpRecvQueue(peer);
            }
        }
#else
        int out_index = 0, out_flag = 0;
        MPI_Status out_status;

        // (mpi_async_requests_.size() >= 10)
        sLOG0 << "DispatchOne(): MPI_Testany()"
              << " mpi_async_requests_=" << mpi_async_requests_.size();

        int r = MPI_Testany(
            // in: Length of array_of_requests (integer).
            static_cast<int>(mpi_async_requests_.size()),
            // in: Array of requests (array of handles).
            mpi_async_requests_.data(),
            // out: Number of completed request (integer).
            &out_index,
            // out: True if one of the operations is complete (logical).
            &out_flag,
            // out: Status object (status).
            &out_status /* MPI_STATUS_IGNORE */);

        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Testany()", r);

        if (out_flag == 0) {
            // nothing returned
            lock.unlock();
        }
        else {
            die_unless((unsigned)out_index < mpi_async_requests_.size());
            lock.unlock();

            LOG << "DispatchOne(): MPI_Testany() out_flag=" << out_flag
                << " done #" << out_index
                << " out_tag=" << out_status.MPI_TAG;

            // perform callback
            mpi_async_[out_index].DoCallback(out_status);

#if THRILL_NET_MPI_QUEUES
            MpiAsync& a = mpi_async_[out_index];
            int peer = a.connection() ? a.connection()->peer() : 0;
            MpiAsync::Type a_type = a.type_;
#endif

            mpi_async_.erase(mpi_async_.begin() + out_index);
            mpi_async_requests_.erase(mpi_async_requests_.begin() + out_index);
            mpi_async_out_.erase(mpi_async_out_.begin() + out_index);
            mpi_status_out_.erase(mpi_status_out_.begin() + out_index);

#if THRILL_NET_MPI_QUEUES
            if (a_type == MpiAsync::WRITE_BUFFER ||
                a_type == MpiAsync::WRITE_BLOCK)
            {
                die_unless(send_active_[peer] > 0);
                send_active_[peer]--;
                LOG << "DispatchOne() send_active_[" << peer << "]="
                    << send_active_[peer];
                PumpSendQueue(peer);
            }
            else if (a_type == MpiAsync::READ_BUFFER ||
                     a_type == MpiAsync::READ_BYTE_BLOCK)
            {
                die_unless(recv_active_[peer] > 0);
                recv_active_[peer]--;
                LOG << "DispatchOne() recv_active_[" << peer << "]="
                    << recv_active_[peer];
                PumpRecvQueue(peer);
            }
#endif
        }
#endif
    }

    if (watch_active_ && 0)
    {
        // use MPI_Iprobe() to check for a new message on this MPI tag.
        int flag = 0;
        MPI_Status status;

        {
            // lock the GMLIM
            std::unique_lock<std::mutex> lock(g_mutex);

            int r = MPI_Iprobe(MPI_ANY_SOURCE, /* group_tag */ 0,
                               MPI_COMM_WORLD, &flag, &status);

            if (r != MPI_SUCCESS)
                throw Exception("Error during MPI_Iprobe()", r);
        }

        // check whether probe was successful
        if (flag == 0) return;

        // get the right watch
        int p = status.MPI_SOURCE;
        assert(p >= 0 && static_cast<size_t>(p) < watch_.size());

        Watch& w = watch_[p];

        if (!w.active) {
            sLOG << "Got Iprobe() for unwatched peer" << p;
            return;
        }

        sLOG << "Got iprobe for peer" << p;

        if (w.read_cb.size()) {
            // run read callbacks until one returns true (in which case it wants
            // to be called again), or the read_cb list is empty.
            while (w.read_cb.size() && w.read_cb.front()() == false) {
                w.read_cb.pop_front();
            }

            if (w.read_cb.size() == 0) {
                w.active = false;
                watch_active_--;
            }
        }
        else {
            LOG << "Dispatcher: got MPI_Iprobe() for peer "
                << p << " without a read handler.";
        }
    }
}

} // namespace mpi
} // namespace net
} // namespace thrill

/******************************************************************************/
