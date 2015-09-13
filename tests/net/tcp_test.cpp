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
        net::tcp::Group::ConstructLocalMesh(6),
        thread_function);
}

/*[[[cog
import tests.net.test_gen as m

m.generate_group_tests('RealTcpGroup', 'RealGroupTest')
m.generate_dispatcher_tests('RealTcpGroup', 'RealGroupTest',
                            'net::tcp::SelectDispatcher')

m.generate_group_tests('LocalTcpGroup', 'LocalGroupTest')
m.generate_dispatcher_tests('LocalTcpGroup', 'LocalGroupTest',
                            'net::tcp::SelectDispatcher')
m.generate_flow_control_tests('LocalTcpGroup', 'LocalGroupTest')

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
TEST(RealTcpGroup, PrefixSumForPowersOfTwo) {
    RealGroupTest(TestPrefixSumForPowersOfTwo);
}
TEST(RealTcpGroup, PrefixSumForPowersOfTwoString) {
    RealGroupTest(TestPrefixSumForPowersOfTwoString);
}
TEST(RealTcpGroup, ReduceToRoot) {
    RealGroupTest(TestReduceToRoot);
}
TEST(RealTcpGroup, ReduceToRootString) {
    RealGroupTest(TestReduceToRootString);
}
TEST(RealTcpGroup, Broadcast) {
    RealGroupTest(TestBroadcast);
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
TEST(RealTcpGroup, DispatcherSyncSendAsyncRead) {
    RealGroupTest(
        DispatcherTestSyncSendAsyncRead<net::tcp::SelectDispatcher>);
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
TEST(LocalTcpGroup, PrefixSumForPowersOfTwo) {
    LocalGroupTest(TestPrefixSumForPowersOfTwo);
}
TEST(LocalTcpGroup, PrefixSumForPowersOfTwoString) {
    LocalGroupTest(TestPrefixSumForPowersOfTwoString);
}
TEST(LocalTcpGroup, ReduceToRoot) {
    LocalGroupTest(TestReduceToRoot);
}
TEST(LocalTcpGroup, ReduceToRootString) {
    LocalGroupTest(TestReduceToRootString);
}
TEST(LocalTcpGroup, Broadcast) {
    LocalGroupTest(TestBroadcast);
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
TEST(LocalTcpGroup, DispatcherSyncSendAsyncRead) {
    LocalGroupTest(
        DispatcherTestSyncSendAsyncRead<net::tcp::SelectDispatcher>);
}
TEST(FlowControlLocalTcpGroup, SingleThreadPrefixSum) {
    LocalGroupTest(TestSingleThreadPrefixSum);
}
TEST(FlowControlLocalTcpGroup, SingleThreadVectorPrefixSum) {
    LocalGroupTest(TestSingleThreadVectorPrefixSum);
}
TEST(FlowControlLocalTcpGroup, SingleThreadBroadcast) {
    LocalGroupTest(TestSingleThreadBroadcast);
}
TEST(FlowControlLocalTcpGroup, MultiThreadBroadcast) {
    LocalGroupTest(TestMultiThreadBroadcast);
}
TEST(FlowControlLocalTcpGroup, SingleThreadAllReduce) {
    LocalGroupTest(TestSingleThreadAllReduce);
}
TEST(FlowControlLocalTcpGroup, MultiThreadAllReduce) {
    LocalGroupTest(TestMultiThreadAllReduce);
}
TEST(FlowControlLocalTcpGroup, MultiThreadPrefixSum) {
    LocalGroupTest(TestMultiThreadPrefixSum);
}
TEST(FlowControlLocalTcpGroup, HardcoreRaceConditionTest) {
    LocalGroupTest(TestHardcoreRaceConditionTest);
}
// [[[end]]]

#if COLLECTIVES_ARE_DISABLED_MAYBE_REMOVE

TEST(Group, TestPrefixSum) {
    for (size_t p = 1; p <= 8; ++p) {
        // Construct Group of p workers which perform a PrefixSum collective
        tcp::Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value = 1;
                PrefixSum(*net, local_value);
                ASSERT_EQ(local_value, net->my_host_rank() + 1);
            });
    }
}

TEST(Group, TestAllReduce) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct Group of p workers which perform an AllReduce collective
        tcp::Group::ExecuteLocalMock(
            p, [](tcp::Group* net) {
                size_t local_value = net->my_host_rank();
                AllReduce(*net, local_value);
                ASSERT_EQ(local_value, net->num_hosts() * (net->num_hosts() - 1) / 2);
            });
    }
}

TEST(Group, TestAllReduceInHypercube) {
    // Construct a NetGroup of 8 workers which perform an AllReduce collective
    for (size_t p = 1; p <= 8; p <<= 1) {
        Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value = net->my_host_rank();
                AllReduceForPowersOfTwo(*net, local_value);
                ASSERT_EQ(local_value, net->num_hosts() * (net->num_hosts() - 1) / 2);
            });
    }
}

TEST(Group, TestBarrier) {
    static const bool debug = false;
    std::mutex sync_mtx;            // Synchronisation mutex for the barrier
    std::mutex local_mtx;           // Mutex for writing to the results array
    std::condition_variable cv;     // Condition variable for the barrier

    for (int p = 0; p <= 8; ++p) {
        int workers = p;
        int workers_copy = workers; // Will be decremented by the barrier function

        std::vector<char> result(2 * workers);
        int k = 0;                  // The counter for the result array
        sLOG << "I'm in test" << workers;

        Group::ExecuteLocalMock(
            workers, [&](Group* net) {
                local_mtx.lock();
                result[k++] = 'B'; // B stands for 'Before barrier'
                local_mtx.unlock();

                sLOG << "Before Barrier, worker" << net->my_host_rank();

                ThreadBarrier(sync_mtx, cv, workers_copy);

                local_mtx.lock();
                result[k++] = 'A'; // A stands for 'After barrier'
                local_mtx.unlock();

                sLOG << "After Barrier, worker" << net->my_host_rank();
            });
        for (int i = 0; i < workers; ++i) {
            sLOG << "Checking position" << i;
            ASSERT_EQ(result[i], 'B');
        }
        for (int i = workers; i < 2 * workers; ++i) {
            sLOG << "Checking position" << i;
            ASSERT_EQ(result[i], 'A');
        }
    }
}

#endif // COLLECTIVES_ARE_DISABLED_MAYBE_REMOVE

/******************************************************************************/
