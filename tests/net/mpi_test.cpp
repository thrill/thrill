/*******************************************************************************
 * tests/net/mpi_test.cpp
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
#include <thrill/net/mpi/dispatcher.hpp>
#include <thrill/net/mpi/group.hpp>

#include "flow_control_test_base.hpp"
#include "group_test_base.hpp"

using namespace thrill;      // NOLINT

void MpiTest(const std::function<void(net::Group*)>& thread_function) {

    size_t num_hosts = net::mpi::NumMpiProcesses();
    sLOG0 << "MpiTest num_hosts" << num_hosts;

    // construct MPI network group and run program
    net::DispatcherThread dispatcher(
        std::make_unique<net::mpi::Dispatcher>(num_hosts), num_hosts);
    std::unique_ptr<net::mpi::Group> group;

    if (net::mpi::Construct(num_hosts, dispatcher, &group, 1)) {
        // only run if construction included this host in the group.

        // we cannot run a truly threaded test anyway.
        thread_function(group.get());
    }

    // needed for sync, otherwise independent tests run in parallel
    group->Barrier();
}

/*[[[perl
  require("tests/net/test_gen.pm");
  generate_group_tests("MpiGroup", "MpiTest");
  generate_flow_control_tests("MpiGroup", "MpiTest");
  ]]]*/
TEST(MpiGroup, NoOperation) {
    MpiTest(TestNoOperation);
}
TEST(MpiGroup, SendRecvCyclic) {
    MpiTest(TestSendRecvCyclic);
}
TEST(MpiGroup, BroadcastIntegral) {
    MpiTest(TestBroadcastIntegral);
}
TEST(MpiGroup, SendReceiveAll2All) {
    MpiTest(TestSendReceiveAll2All);
}
TEST(MpiGroup, PrefixSumHypercube) {
    MpiTest(TestPrefixSumHypercube);
}
TEST(MpiGroup, PrefixSumHypercubeString) {
    MpiTest(TestPrefixSumHypercubeString);
}
TEST(MpiGroup, PrefixSum) {
    MpiTest(TestPrefixSum);
}
TEST(MpiGroup, Broadcast) {
    MpiTest(TestBroadcast);
}
TEST(MpiGroup, Reduce) {
    MpiTest(TestReduce);
}
TEST(MpiGroup, ReduceString) {
    MpiTest(TestReduceString);
}
TEST(MpiGroup, AllReduceString) {
    MpiTest(TestAllReduceString);
}
TEST(MpiGroup, AllReduceHypercubeString) {
    MpiTest(TestAllReduceHypercubeString);
}
TEST(MpiGroup, AllReduceEliminationString) {
    MpiTest(TestAllReduceEliminationString);
}
TEST(MpiGroup, DispatcherSyncSendAsyncRead) {
    MpiTest(TestDispatcherSyncSendAsyncRead);
}
TEST(MpiGroup, DispatcherLaunchAndTerminate) {
    MpiTest(TestDispatcherLaunchAndTerminate);
}
TEST(MpiGroup, SingleThreadPrefixSum) {
    MpiTest(TestSingleThreadPrefixSum);
}
TEST(MpiGroup, SingleThreadVectorPrefixSum) {
    MpiTest(TestSingleThreadVectorPrefixSum);
}
TEST(MpiGroup, SingleThreadBroadcast) {
    MpiTest(TestSingleThreadBroadcast);
}
TEST(MpiGroup, MultiThreadBroadcast) {
    MpiTest(TestMultiThreadBroadcast);
}
TEST(MpiGroup, MultiThreadReduce) {
    MpiTest(TestMultiThreadReduce);
}
TEST(MpiGroup, SingleThreadAllReduce) {
    MpiTest(TestSingleThreadAllReduce);
}
TEST(MpiGroup, MultiThreadAllReduce) {
    MpiTest(TestMultiThreadAllReduce);
}
TEST(MpiGroup, MultiThreadPrefixSum) {
    MpiTest(TestMultiThreadPrefixSum);
}
TEST(MpiGroup, PredecessorManyItems) {
    MpiTest(TestPredecessorManyItems);
}
TEST(MpiGroup, PredecessorFewItems) {
    MpiTest(TestPredecessorFewItems);
}
TEST(MpiGroup, PredecessorOneItem) {
    MpiTest(TestPredecessorOneItem);
}
TEST(MpiGroup, HardcoreRaceConditionTest) {
    MpiTest(TestHardcoreRaceConditionTest);
}
TEST(MpiGroup, AllGather) {
    MpiTest(TestAllGather);
}
TEST(MpiGroup, AllGatherMultiThreaded) {
    MpiTest(TestAllGatherMultiThreaded);
}
TEST(MpiGroup, AllGatherString) {
    MpiTest(TestAllGatherString);
}
// [[[end]]]

/******************************************************************************/
