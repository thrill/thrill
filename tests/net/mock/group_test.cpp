/*******************************************************************************
 * tests/net/mock/group_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/net/collective_communication.hpp>
#include <thrill/net/mock/group.hpp>

#include <tests/net/group_test_base.hpp>

#include <thread>

using namespace thrill;      // NOLINT

void MockTest(const std::function<void(net::mock::Group*)>& thread_function) {

    size_t group_size = 8;

    std::vector<net::mock::Group> groups(group_size);

    for (size_t i = 0; i < groups.size(); ++i) {
        groups[i].Initialize(i, group_size);
        for (size_t j = 0; j < groups.size(); ++j) {
            groups[i].peers_[j] = &groups[j];
        }
    }

    // create a thread for each Group object and run user program.
    std::vector<std::thread> threads(group_size);

    for (size_t i = 0; i != group_size; ++i) {
        threads[i] = std::thread(
            std::bind(thread_function, &groups[i]));
    }

    for (size_t i = 0; i != group_size; ++i) {
        threads[i].join();
    }
}

TEST(MockGroup, NoOperation) {
    MockTest(TestNoOperation);
}

TEST(MockGroup, PrefixSumForPowersOfTwo) {
    MockTest(TestPrefixSumForPowersOfTwo);
}

TEST(MockGroup, TestSendRecvCyclic) {
    MockTest(TestSendRecvCyclic);
}

TEST(MockGroup, TestBroadcastIntegral) {
    MockTest(TestBroadcastIntegral);
}

TEST(MockGroup, TestSendReceiveAll2All) {
    MockTest(TestSendReceiveAll2All);
}

static void ThreadInitializeAsyncRead(net::mock::Group* net) {
    // send a message to all other clients except ourselves.
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;
        net->connection(i).SyncSend(&i, sizeof(size_t));
    }

    size_t received = 0;
    mem::Manager mem_manager(nullptr, "Dispatcher");
    net::mock::Dispatcher dispatcher(mem_manager);

    net::AsyncReadCallback callback =
        [net, &received](net::Connection& /* s */, const net::Buffer& buffer) {
            ASSERT_EQ(*(reinterpret_cast<const size_t*>(buffer.data())),
                      net->my_host_rank());
            received++;
        };

    // add async reads to net dispatcher
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;
        dispatcher.AsyncRead(net->connection(i), sizeof(size_t), callback);
    }

    while (received < net->num_hosts() - 1) {
        dispatcher.Dispatch();
    }
}

TEST(MockGroup, ThreadInitializeAsyncRead) {
    MockTest([](net::mock::Group* net) {
                 ThreadInitializeAsyncRead(net);
             });
}

/******************************************************************************/
