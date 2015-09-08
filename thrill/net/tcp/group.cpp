/*******************************************************************************
 * thrill/net/tcp/group.cpp
 *
 * net::Group is a collection of Connections providing simple MPI-like
 * collectives and point-to-point communication.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/net/tcp/group.hpp>

#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace thrill {
namespace net {
namespace tcp {

std::vector<Group> Group::ConstructLocalMesh(size_t num_clients) {

    // construct a group of num_clients
    std::vector<Group> group(num_clients);

    for (size_t i = 0; i != num_clients; ++i) {
        group[i].Initialize(i, num_clients);
    }

    // construct a stream socket pair for (i,j) with i < j
    for (size_t i = 0; i != num_clients; ++i) {
        for (size_t j = i + 1; j < num_clients; ++j) {
            LOG << "doing Socket::CreatePair() for i=" << i << " j=" << j;

            std::pair<Socket, Socket> sp = Socket::CreatePair();

            sp.first.SetNonBlocking(true);
            sp.second.SetNonBlocking(true);

            group[i].connections_[j] = Connection(std::move(sp.first));
            group[j].connections_[i] = Connection(std::move(sp.second));
        }
    }

    return group;
}

void Group::ExecuteLocalMock(
    size_t num_clients,
    const std::function<void(Group*)>& thread_function) {

    // construct a group of num_clients
    std::vector<Group> group = ConstructLocalMesh(num_clients);

    // create a thread for each Group object and run user program.
    std::vector<std::thread> threads(num_clients);

    for (size_t i = 0; i != num_clients; ++i) {
        threads[i] = std::thread(
            std::bind(thread_function, &group[i]));
    }

    for (size_t i = 0; i != num_clients; ++i) {
        threads[i].join();
    }

    // tear down mesh by closing all group objects
    for (size_t i = 0; i != num_clients; ++i) {
        group[i].Close();
    }
}

} // namespace tcp
} // namespace net
} // namespace thrill

/******************************************************************************/
