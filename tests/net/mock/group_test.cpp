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

void MockTestOne(size_t num_hosts,
                 const std::function<void(net::mock::Group*)>& thread_function) {

    net::ExecuteLocalMock(
        net::mock::Group::ConstructLocalMesh(num_hosts),
        thread_function);
}

void MockTest(const std::function<void(net::Group*)>& thread_function) {
    MockTestOne(1, thread_function);
    MockTestOne(2, thread_function);
    MockTestOne(3, thread_function);
    MockTestOne(4, thread_function);
    MockTestOne(5, thread_function);
    MockTestOne(6, thread_function);
    MockTestOne(7, thread_function);
    MockTestOne(8, thread_function);
    MockTestOne(16, thread_function);
    MockTestOne(20, thread_function);
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
TEST(MockGroup, PrefixSumForPowersOfTwoString) {
    MockTest(TestPrefixSumForPowersOfTwoString);
}
TEST(MockGroup, ReduceToRoot) {
    MockTest(TestReduceToRoot);
}
TEST(MockGroup, ReduceToRootString) {
    MockTest(TestReduceToRootString);
}
TEST(MockGroup, DispatcherSyncSendAsyncRead) {
    MockTest(
        DispatcherTestSyncSendAsyncRead<net::mock::Dispatcher>);
}
// [[[end]]]

/******************************************************************************/
