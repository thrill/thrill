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

#include <mpi.h>

#include <mutex>
#include <vector>

namespace thrill {
namespace net {
namespace mpi {

//! The Grand MPI Library Invocation Mutex (The GMLIM)
extern std::mutex g_mutex;

/******************************************************************************/
// mpi::Dispatcher

MPI_Request Dispatcher::ISend(Connection& c, const void* data, size_t size) {
    // lock the GMLIM
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Request request;
    int r = MPI_Isend(const_cast<void*>(data), static_cast<int>(size), MPI_BYTE,
                      c.peer(), group_tag_, MPI_COMM_WORLD, &request);

    if (r != MPI_SUCCESS)
        throw Exception("Error during ISend", r);

    sLOG0 << "Isend size" << size;
    c.tx_bytes_ += size;

    return request;
}

MPI_Request Dispatcher::IRecv(Connection& c, void* data, size_t size) {
    // lock the GMLIM
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Request request;
    int r = MPI_Irecv(data, static_cast<int>(size), MPI_BYTE,
                      c.peer(), group_tag_, MPI_COMM_WORLD, &request);

    if (r != MPI_SUCCESS)
        throw Exception("Error during IRecv", r);

    sLOG0 << "Irecv size" << size;
    c.rx_bytes_ += size;

    return request;
}

void Dispatcher::DispatchOne(const std::chrono::milliseconds& /* timeout */) {

    // use MPI_Testsome() to check for finished writes
    if (mpi_async_requests_.size())
    {
        // lock the GMLIM
        std::unique_lock<std::mutex> lock(g_mutex);

        assert(mpi_async_.size() == mpi_async_requests_.size());
        assert(mpi_async_.size() == mpi_async_out_.size());

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
            MPI_STATUSES_IGNORE);

        lock.unlock();

        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Testsome()", r);

        if (out_count == MPI_UNDEFINED) {
            // nothing returned
        }
        else if (out_count > 0) {
            sLOG << "DispatchOne(): MPI_Testsome() out_count=" << out_count;

            // run through finished requests back to front, swap last entry into
            // finished ones, such that preceding indexes remain valid.
            for (int k = out_count - 1; k >= 0; --k) {
                size_t p = mpi_async_out_[k];

                // TODO(tb): check for errors?

                // perform callback
                mpi_async_[p]();

                // deleted the entry (no problem is k == len)
                size_t back = mpi_async_.size() - 1;

                mpi_async_[p] = std::move(mpi_async_[back]);
                mpi_async_requests_[p] = std::move(mpi_async_requests_[back]);
                mpi_async_out_[p] = std::move(mpi_async_out_[back]);

                mpi_async_.resize(back);
                mpi_async_requests_.resize(back);
                mpi_async_out_.resize(back);
            }
        }
    }

    if (watch_active_)
    {
        // use MPI_Iprobe() to check for a new message on this MPI tag.
        int flag = 0;
        MPI_Status status;

        {
            // lock the GMLIM
            std::unique_lock<std::mutex> lock(g_mutex);

            int r = MPI_Iprobe(MPI_ANY_SOURCE, group_tag_, MPI_COMM_WORLD,
                               &flag, &status);

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
