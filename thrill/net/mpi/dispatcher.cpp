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

    // since MPI can always write to the network: call all write callbacks
    for (size_t i = 0; i < watch_.size(); ++i) {
        if (!watch_[i].active) continue;
        Watch& w = watch_[i];

        while (w.write_cb.size()) {
            if (w.write_cb.front()()) break;
            w.write_cb.pop_front();
        }

        if (w.read_cb.size() == 0 && w.write_cb.size() == 0) {
            w.active = false;
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

        if (w.read_cb.size() == 0 && w.write_cb.size() == 0) {
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
