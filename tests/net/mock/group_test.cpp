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

/*[[[cog
import tests.net.group_test_gen as m
m.generate_group_tests('MockGroup', 'MockTest')
m.generate_dispatcher_tests('MockGroup', 'MockTest', 'net::mock::Dispatcher')
  ]]]*/
TEST(MockGroup, NoOperation) {
    MockTest(TestNoOperation);
}
TEST(MockGroup, SendRecvCyclic) {
    MockTest(TestSendRecvCyclic);
}
TEST(MockGroup, BroadcastIntegral) {
    MockTest(TestBroadcastIntegral);
}
TEST(MockGroup, SendReceiveAll2All) {
    MockTest(TestSendReceiveAll2All);
}
TEST(MockGroup, PrefixSumForPowersOfTwo) {
    MockTest(TestPrefixSumForPowersOfTwo);
}
TEST(MockGroup, ReduceToRoot) {
    MockTest(TestReduceToRoot);
}
TEST(MockGroup, DispatcherSyncSendAsyncRead) {
    MockTest(
        DispatcherTestSyncSendAsyncRead<net::mock::Dispatcher>);
}
// [[[end]]]

/******************************************************************************/
