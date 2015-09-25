/*******************************************************************************
 * tests/net/mpi_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/mpi/group.hpp>

#include "flow_control_test_base.hpp"
#include "group_test_base.hpp"

using namespace thrill;      // NOLINT

void MpiTestOne(size_t num_hosts,
                const std::function<void(net::mpi::Group*)>& thread_function) {
    sLOG0 << "MpiTestOne num_hosts" << num_hosts;

    if (net::mpi::NumMpiProcesses() < num_hosts) {
        sLOG1 << "Not running MpiTest with" << num_hosts << "."
              << "Increase the number of MPI processes.";
        return;
    }

    // construct MPI network group and run program
    std::unique_ptr<net::mpi::Group> group;

    if (net::mpi::Construct(num_hosts, &group, 1)) {
        // only run if construction included this host in the group.

        // we cannot run a truly threaded test anyway.
        thread_function(group.get());
    }

    // needed for sync, otherwise independent tests run in parallel
    group->Barrier();
}

void MpiTest(const std::function<void(net::Group*)>& thread_function) {
    MpiTestOne(1, thread_function);
    MpiTestOne(2, thread_function);
    MpiTestOne(3, thread_function);
    MpiTestOne(4, thread_function);
    MpiTestOne(5, thread_function);
    MpiTestOne(7, thread_function);
    MpiTestOne(8, thread_function);
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
TEST(MpiGroup, ReduceToRoot) {
    MpiTest(TestReduceToRoot);
}
TEST(MpiGroup, ReduceToRootString) {
    MpiTest(TestReduceToRootString);
}
TEST(MpiGroup, AllReduceString) {
    MpiTest(TestAllReduceString);
}
TEST(MpiGroup, AllReduceHypercubeString) {
    MpiTest(TestAllReduceHypercubeString);
}
TEST(MpiGroup, DispatcherSyncSendAsyncRead) {
    MpiTest(TestDispatcherSyncSendAsyncRead);
}
TEST(MpiGroup, DispatcherLaunchAndTerminate) {
    MpiTest(TestDispatcherLaunchAndTerminate);
}
TEST(MpiGroup, DispatcherAsyncWriteAndReadIntoFuture) {
    MpiTest(TestDispatcherAsyncWriteAndReadIntoFuture);
}
TEST(MpiGroup, DispatcherAsyncWriteAndReadIntoFutureX) {
    MpiTest(TestDispatcherAsyncWriteAndReadIntoFutureX);
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
TEST(MpiGroup, SingleThreadAllReduce) {
    MpiTest(TestSingleThreadAllReduce);
}
TEST(MpiGroup, MultiThreadAllReduce) {
    MpiTest(TestMultiThreadAllReduce);
}
TEST(MpiGroup, MultiThreadPrefixSum) {
    MpiTest(TestMultiThreadPrefixSum);
}
TEST(MpiGroup, HardcoreRaceConditionTest) {
    MpiTest(TestHardcoreRaceConditionTest);
}
// [[[end]]]

/******************************************************************************/
