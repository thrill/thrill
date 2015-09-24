/*******************************************************************************
 * thrill/net/mpi/dispatcher.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
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

void Dispatcher::DispatchOne(const std::chrono::milliseconds& /* timeout */) {

    // use MPI_Testsome() to check for finished writes
    if (mpi_async_requests_.size())
    {
        // lock the GMLIM
        std::unique_lock<std::mutex> lock(g_mutex);

        assert(mpi_async_write_.size() == mpi_async_requests_.size());
        assert(mpi_async_write_.size() == mpi_async_out_.size());
        assert(mpi_async_write_.size() == mpi_async_status_.size());

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
            mpi_async_status_.data());

        lock.unlock();

        if (r != MPI_SUCCESS)
            throw Exception("Error during MPI_Testsome()", r);

        if (out_count == MPI_UNDEFINED) {
            // nothing returned
        }
        else if (out_count > 0) {
            sLOG << "DispatchOne(): MPI_Testsome() out_count=" << out_count;

            size_t len = mpi_async_write_.size();

            // run through finished requests back to front, swap last entry into
            // finished ones.
            for (int k = out_count - 1; k >= 0; --k) {
                size_t p = mpi_async_out_[k];

                // TODO(tb): check for errors?

                // deleted the entry (no problem is k == len)
                --len;
                mpi_async_write_[p] = std::move(mpi_async_write_[len]);
                mpi_async_requests_[p] = std::move(mpi_async_requests_[len]);
                mpi_async_out_[p] = std::move(mpi_async_out_[len]);
                mpi_async_status_[p] = std::move(mpi_async_status_[len]);
            }

            mpi_async_write_.resize(len);
            mpi_async_requests_.resize(len);
            mpi_async_out_.resize(len);
            mpi_async_status_.resize(len);
        }
    }

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
        }
    }
    else {
        LOG << "Dispatcher: got MPI_Iprobe() for peer "
            << p << " without a read handler.";
    }
}

} // namespace mpi
} // namespace net
} // namespace thrill

/******************************************************************************/
