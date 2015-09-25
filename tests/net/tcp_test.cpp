/*******************************************************************************
 * tests/net/tcp_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/mem/manager.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/tcp/group.hpp>
#include <thrill/net/tcp/select_dispatcher.hpp>

#include <random>
#include <string>
#include <thread>
#include <vector>

#include "flow_control_test_base.hpp"
#include "group_test_base.hpp"

using namespace thrill;      // NOLINT

static void RealGroupTest(
    const std::function<void(net::Group*)>& thread_function) {
    // execute locally connected TCP stream socket tests
    net::ExecuteGroupThreads(
        net::tcp::Group::ConstructLocalRealTCPMesh(6),
        thread_function);
}

static void LocalGroupTest(
    const std::function<void(net::Group*)>& thread_function) {
    // execute local stream socket tests
    net::ExecuteGroupThreads(
        net::tcp::Group::ConstructLoopbackMesh(6),
        thread_function);
}

/*[[[perl
  require("tests/net/test_gen.pm");
  generate_group_tests("RealTcpGroup", "RealGroupTest");

  generate_group_tests("LocalTcpGroup", "LocalGroupTest");
  generate_flow_control_tests("LocalTcpGroup", "LocalGroupTest");
  ]]]*/
TEST(RealTcpGroup, NoOperation) {
    RealGroupTest(TestNoOperation);
}
TEST(RealTcpGroup, SendRecvCyclic) {
    RealGroupTest(TestSendRecvCyclic);
}
TEST(RealTcpGroup, BroadcastIntegral) {
    RealGroupTest(TestBroadcastIntegral);
}
TEST(RealTcpGroup, SendReceiveAll2All) {
    RealGroupTest(TestSendReceiveAll2All);
}
TEST(RealTcpGroup, PrefixSumHypercube) {
    RealGroupTest(TestPrefixSumHypercube);
}
TEST(RealTcpGroup, PrefixSumHypercubeString) {
    RealGroupTest(TestPrefixSumHypercubeString);
}
TEST(RealTcpGroup, PrefixSum) {
    RealGroupTest(TestPrefixSum);
}
TEST(RealTcpGroup, Broadcast) {
    RealGroupTest(TestBroadcast);
}
TEST(RealTcpGroup, ReduceToRoot) {
    RealGroupTest(TestReduceToRoot);
}
TEST(RealTcpGroup, ReduceToRootString) {
    RealGroupTest(TestReduceToRootString);
}
TEST(RealTcpGroup, AllReduceString) {
    RealGroupTest(TestAllReduceString);
}
TEST(RealTcpGroup, AllReduceHypercubeString) {
    RealGroupTest(TestAllReduceHypercubeString);
}
TEST(RealTcpGroup, DispatcherSyncSendAsyncRead) {
    RealGroupTest(TestDispatcherSyncSendAsyncRead);
}
TEST(RealTcpGroup, DispatcherLaunchAndTerminate) {
    RealGroupTest(TestDispatcherLaunchAndTerminate);
}
TEST(RealTcpGroup, DispatcherAsyncWriteAndReadIntoFuture) {
    RealGroupTest(TestDispatcherAsyncWriteAndReadIntoFuture);
}
TEST(RealTcpGroup, DispatcherAsyncWriteAndReadIntoFutureX) {
    RealGroupTest(TestDispatcherAsyncWriteAndReadIntoFutureX);
}
TEST(LocalTcpGroup, NoOperation) {
    LocalGroupTest(TestNoOperation);
}
TEST(LocalTcpGroup, SendRecvCyclic) {
    LocalGroupTest(TestSendRecvCyclic);
}
TEST(LocalTcpGroup, BroadcastIntegral) {
    LocalGroupTest(TestBroadcastIntegral);
}
TEST(LocalTcpGroup, SendReceiveAll2All) {
    LocalGroupTest(TestSendReceiveAll2All);
}
TEST(LocalTcpGroup, PrefixSumHypercube) {
    LocalGroupTest(TestPrefixSumHypercube);
}
TEST(LocalTcpGroup, PrefixSumHypercubeString) {
    LocalGroupTest(TestPrefixSumHypercubeString);
}
TEST(LocalTcpGroup, PrefixSum) {
    LocalGroupTest(TestPrefixSum);
}
TEST(LocalTcpGroup, Broadcast) {
    LocalGroupTest(TestBroadcast);
}
TEST(LocalTcpGroup, ReduceToRoot) {
    LocalGroupTest(TestReduceToRoot);
}
TEST(LocalTcpGroup, ReduceToRootString) {
    LocalGroupTest(TestReduceToRootString);
}
TEST(LocalTcpGroup, AllReduceString) {
    LocalGroupTest(TestAllReduceString);
}
TEST(LocalTcpGroup, AllReduceHypercubeString) {
    LocalGroupTest(TestAllReduceHypercubeString);
}
TEST(LocalTcpGroup, DispatcherSyncSendAsyncRead) {
    LocalGroupTest(TestDispatcherSyncSendAsyncRead);
}
TEST(LocalTcpGroup, DispatcherLaunchAndTerminate) {
    LocalGroupTest(TestDispatcherLaunchAndTerminate);
}
TEST(LocalTcpGroup, DispatcherAsyncWriteAndReadIntoFuture) {
    LocalGroupTest(TestDispatcherAsyncWriteAndReadIntoFuture);
}
TEST(LocalTcpGroup, DispatcherAsyncWriteAndReadIntoFutureX) {
    LocalGroupTest(TestDispatcherAsyncWriteAndReadIntoFutureX);
}
TEST(LocalTcpGroup, SingleThreadPrefixSum) {
    LocalGroupTest(TestSingleThreadPrefixSum);
}
TEST(LocalTcpGroup, SingleThreadVectorPrefixSum) {
    LocalGroupTest(TestSingleThreadVectorPrefixSum);
}
TEST(LocalTcpGroup, SingleThreadBroadcast) {
    LocalGroupTest(TestSingleThreadBroadcast);
}
TEST(LocalTcpGroup, MultiThreadBroadcast) {
    LocalGroupTest(TestMultiThreadBroadcast);
}
TEST(LocalTcpGroup, SingleThreadAllReduce) {
    LocalGroupTest(TestSingleThreadAllReduce);
}
TEST(LocalTcpGroup, MultiThreadAllReduce) {
    LocalGroupTest(TestMultiThreadAllReduce);
}
TEST(LocalTcpGroup, MultiThreadPrefixSum) {
    LocalGroupTest(TestMultiThreadPrefixSum);
}
TEST(LocalTcpGroup, HardcoreRaceConditionTest) {
    LocalGroupTest(TestHardcoreRaceConditionTest);
}
// [[[end]]]

/******************************************************************************/
