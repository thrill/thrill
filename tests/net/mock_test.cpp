/*******************************************************************************
 * tests/net/mock_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/mock/group.hpp>

#include <thread>

#include "flow_control_test_base.hpp"
#include "group_test_base.hpp"

using namespace thrill;      // NOLINT

void MockTestOne(size_t num_hosts,
                 const std::function<void(net::mock::Group*)>& thread_function) {
    sLOG0 << "MockTestOne num_hosts" << num_hosts;
    // construct mock network mesh and run threads
    net::ExecuteGroupThreads(
        net::mock::Group::ConstructLoopbackMesh(num_hosts),
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

void MockTestLess(const std::function<void(net::Group*)>& thread_function) {
    MockTestOne(1, thread_function);
    MockTestOne(2, thread_function);
    MockTestOne(3, thread_function);
    MockTestOne(5, thread_function);
    MockTestOne(8, thread_function);
}

/*[[[perl
  require("tests/net/test_gen.pm");
  generate_group_tests("MockGroup", "MockTest");
  generate_flow_control_tests("MockGroup", "MockTestLess");
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
TEST(MockGroup, PrefixSumHypercube) {
    MockTest(TestPrefixSumHypercube);
}
TEST(MockGroup, PrefixSumHypercubeString) {
    MockTest(TestPrefixSumHypercubeString);
}
TEST(MockGroup, PrefixSum) {
    MockTest(TestPrefixSum);
}
TEST(MockGroup, Broadcast) {
    MockTest(TestBroadcast);
}
TEST(MockGroup, Reduce) {
    MockTest(TestReduce);
}
TEST(MockGroup, ReduceString) {
    MockTest(TestReduceString);
}
TEST(MockGroup, AllReduceString) {
    MockTest(TestAllReduceString);
}
TEST(MockGroup, AllReduceHypercubeString) {
    MockTest(TestAllReduceHypercubeString);
}
TEST(MockGroup, AllReduceEliminationString) {
    MockTest(TestAllReduceEliminationString);
}
TEST(MockGroup, DispatcherSyncSendAsyncRead) {
    MockTest(TestDispatcherSyncSendAsyncRead);
}
TEST(MockGroup, DispatcherLaunchAndTerminate) {
    MockTest(TestDispatcherLaunchAndTerminate);
}
TEST(MockGroup, SingleThreadPrefixSum) {
    MockTestLess(TestSingleThreadPrefixSum);
}
TEST(MockGroup, SingleThreadVectorPrefixSum) {
    MockTestLess(TestSingleThreadVectorPrefixSum);
}
TEST(MockGroup, SingleThreadBroadcast) {
    MockTestLess(TestSingleThreadBroadcast);
}
TEST(MockGroup, MultiThreadBroadcast) {
    MockTestLess(TestMultiThreadBroadcast);
}
TEST(MockGroup, MultiThreadReduce) {
    MockTestLess(TestMultiThreadReduce);
}
TEST(MockGroup, SingleThreadAllReduce) {
    MockTestLess(TestSingleThreadAllReduce);
}
TEST(MockGroup, MultiThreadAllReduce) {
    MockTestLess(TestMultiThreadAllReduce);
}
TEST(MockGroup, MultiThreadPrefixSum) {
    MockTestLess(TestMultiThreadPrefixSum);
}
TEST(MockGroup, PredecessorManyItems) {
    MockTestLess(TestPredecessorManyItems);
}
TEST(MockGroup, PredecessorFewItems) {
    MockTestLess(TestPredecessorFewItems);
}
TEST(MockGroup, PredecessorOneItem) {
    MockTestLess(TestPredecessorOneItem);
}
TEST(MockGroup, HardcoreRaceConditionTest) {
    MockTestLess(TestHardcoreRaceConditionTest);
}
// [[[end]]]

/******************************************************************************/
