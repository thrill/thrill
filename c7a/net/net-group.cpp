/*******************************************************************************
 * c7a/communication/net-group.cpp
 *
 * NetGroup is a collection of NetConnections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/net-group.hpp>

#include <thread>

namespace c7a {

void NetGroup::ExecuteLocalMock(size_t num_clients,
                                const std::function<void(NetGroup*)>& thread_function)
{
    // construct a group of num_clients
    std::vector<std::unique_ptr<NetGroup> > group(num_clients);

    for (size_t i = 0; i != num_clients; ++i) {
        group[i] = std::unique_ptr<NetGroup>(new NetGroup(i, num_clients));
    }

    // construct a stream socket pair for (i,j) with i <= j
    for (size_t i = 0; i != num_clients; ++i) {
        for (size_t j = i + 1; j < num_clients; ++j) {
            LOG << "doing Socket::CreatePair() for i=" << i << " j=" << j;

            std::pair<Socket, Socket> sp = Socket::CreatePair();

            group[i]->connections_[j] = NetConnection(sp.first);
            group[j]->connections_[i] = NetConnection(sp.second);
        }
    }

    // create a thread for each NetGroup object and run user program.
    std::vector<std::thread*> threads(num_clients);

    for (size_t i = 0; i != num_clients; ++i) {
        threads[i] = new std::thread(
            std::bind(thread_function, group[i].get()));
    }

    for (size_t i = 0; i != num_clients; ++i) {
        threads[i]->join();
        delete threads[i];
    }

    // close sockets allocated before
    for (size_t i = 0; i != num_clients; ++i) {
        group[i]->Close();
    }
}

} // namespace c7a

/******************************************************************************/
