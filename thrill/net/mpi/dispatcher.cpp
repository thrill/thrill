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

/******************************************************************************/
// mpi::Dispatcher

Dispatcher::~Dispatcher() {
    LOG << "~mpi::Dispatcher()"
        << " mpi_async_.size()=" << mpi_async_.size();

    for (size_t i = 0; i < mpi_async_requests_.size(); ++i) {
        int r = MPI_Cancel(&mpi_async_requests_[i]);

        if (r != MPI_SUCCESS)
            LOG1 << "Error during MPI_Cancel()";

        MPI_Request_free(&mpi_async_requests_[i]);
    }
}

MPI_Request Dispatcher::ISend(Connection& c, const void* data, size_t size) {
    // lock the GMLIM
    std::unique_lock<std::mutex> lock(g_mutex);

    MPI_Request request;
    int r = MPI_Isend(const_cast<void*>(data), static_cast<int>(size), MPI_BYTE,
                      c.peer(), group_tag_, MPI_COMM_WORLD, &request);

    if (r != MPI_SUCCESS)
        throw Exception("Error during MPI_Isend()", r);

    LOG << "MPI_Isend() data=" << data << " size=" << size
        << " request=" << request;

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
        throw Exception("Error during MPI_Irecv()", r);

    LOG << "MPI_Irecv() data=" << data << " size=" << size
        << " request=" << request;

    c.rx_bytes_ += size;

    return request;
}

void Dispatcher::DispatchOne(const std::chrono::milliseconds& /* timeout */) {

    // use MPI_Testsome() to check for finished writes
    if (mpi_async_requests_.size())
    {
        // lock the GMLIM
        std::unique_lock<std::mutex> lock(g_mutex);

        die_unless(mpi_async_.size() == mpi_async_requests_.size());
        die_unless(mpi_async_.size() == mpi_async_out_.size());

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
            MPI_STATUSES_IGNORE);

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

                for (size_t j = 0; j < mpi_async_.size(); ++j)
                {
                    if (k < out_count && mpi_async_out_[k] == static_cast<int>(j)) {

                        sLOG << "Working #" << k
                             << "which is $" << mpi_async_out_[k];

                        // perform callback
                        mpi_async_[j].DoCallback();

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
            }
        }
#else
        int out_index = 0, out_flag = 0;
        MPI_Status out_status;

        sLOG << "DispatchOne(): MPI_Testany()"
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
        }
        else {
            int out_size;
            MPI_Get_count(&out_status, MPI_BYTE, &out_size);

            LOG1 << "DispatchOne(): MPI_Testany() out_flag=" << out_flag
                 << " done #" << out_index
                 << " out_size=" << out_size
                 << " out_tag=" << out_status.MPI_TAG;

            // perform callback
            mpi_async_[out_index].DoCallback(out_size);

            mpi_async_.erase(mpi_async_.begin() + out_index);
            mpi_async_requests_.erase(mpi_async_requests_.begin() + out_index);
            mpi_async_out_.erase(mpi_async_out_.begin() + out_index);
        }

        lock.unlock();
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
