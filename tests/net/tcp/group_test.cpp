/*******************************************************************************
 * tests/net/tcp/group_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/mem/manager.hpp>
#include <thrill/net/collective_communication.hpp>
#include <thrill/net/flow_control_channel.hpp>
#include <thrill/net/manager.hpp>
#include <thrill/net/tcp/group.hpp>
#include <thrill/net/tcp/select_dispatcher.hpp>

#include <tests/net/group_test_base.hpp>

#include <random>
#include <string>
#include <thread>
#include <vector>

using namespace thrill;      // NOLINT
using namespace thrill::net; // NOLINT

static void ThreadInitializeAsyncRead(tcp::Group* net) {
    // send a message to all other clients except ourselves.
    for (size_t i = 0; i != net->num_hosts(); ++i)
    {
        if (i == net->my_host_rank()) continue;
        net->tcp_connection(i).GetSocket().send(&i, sizeof(size_t));
    }

    size_t received = 0;
    mem::Manager mem_manager(nullptr, "Dispatcher");
    tcp::SelectDispatcher dispatcher(mem_manager);

    AsyncReadCallback callback =
        [net, &received](Connection& /* s */, const Buffer& buffer) {
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

static void RealGroupConstructAndCall(
    std::function<void(tcp::Group*)> thread_function) {
    // randomize base port number for test
    std::default_random_engine generator(std::random_device { } ());
    std::uniform_int_distribution<int> distribution(10000, 30000);
    const size_t port_base = distribution(generator);

    std::vector<std::string> endpoints = {
        "127.0.0.1:" + std::to_string(port_base + 0),
        "127.0.0.1:" + std::to_string(port_base + 1),
        "127.0.0.1:" + std::to_string(port_base + 2),
        "127.0.0.1:" + std::to_string(port_base + 3),
        "127.0.0.1:" + std::to_string(port_base + 4),
        "127.0.0.1:" + std::to_string(port_base + 5)
    };

    sLOG1 << "Group test uses ports" << port_base << "-" << port_base + 5;

    const size_t count = endpoints.size();

    std::vector<std::thread> threads(count);

    // lambda to construct Group and call user thread function.

    std::vector<std::unique_ptr<Manager> > groups(count);

    for (size_t i = 0; i < count; i++) {
        threads[i] = std::thread(
            [i, &endpoints, thread_function, &groups]() {
                // construct Group i with endpoints
                groups[i] = std::make_unique<Manager>(i, endpoints);
                // run thread function
                thread_function(&groups[i]->GetFlowGroup());
            });
    }

    for (size_t i = 0; i < count; i++) {
        threads[i].join();
    }
    for (size_t i = 0; i < count; i++) {
        groups[i]->Close();
    }
}

TEST(Group, RealNoOperation) {
    // Construct a real Group of 6 workers which do nothing but terminate.
    RealGroupConstructAndCall(TestNoOperation);
}

TEST(Group, RealInitializeSendReceive) {
    // Construct a real Group of 6 workers which execute the thread function
    // above which sends and receives a message from all neighbors.
    RealGroupConstructAndCall(TestSendReceiveAll2All);
}

TEST(Group, RealInitializeSendReceiveAsync) { //TODO(ej) test hangs from time to time
    // Construct a real Group of 6 workers which execute the thread function
    // which sends and receives asynchronous messages between all workers.
    RealGroupConstructAndCall(ThreadInitializeAsyncRead);
}

TEST(Group, RealInitializeBroadcast) {
    // Construct a real Group of 6 workers which execute the thread function
    // above which sends and receives a message from all workers.
    RealGroupConstructAndCall(TestBroadcastIntegral);
}
TEST(Group, RealSendCyclic) {
    RealGroupConstructAndCall(TestSendRecvCyclic);
}

TEST(Group, InitializeAndClose) {
    // Construct a Group of 6 workers which do nothing but terminate.
    tcp::Group::ExecuteLocalMock(6, [](tcp::Group*) { });
}

TEST(Group, TestSendReceiveAll2All) {
    tcp::Group::ExecuteLocalMock(6, TestSendReceiveAll2All);
}

TEST(Group, TestBroadcastIntegral) {
    tcp::Group::ExecuteLocalMock(6, TestBroadcastIntegral);
}

TEST(Group, SendCyclic) {
    tcp::Group::ExecuteLocalMock(6, TestSendRecvCyclic);
}

TEST(Group, TestPrefixSumInHypercube) {
    for (size_t p = 1; p <= 8; p <<= 1) {
        tcp::Group::ExecuteLocalMock(p, TestPrefixSumForPowersOfTwo);
    }
}

TEST(Group, TestReduceToRoot) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct Group of p workers which perform an Broadcast collective
        tcp::Group::ExecuteLocalMock(
            p, [](tcp::Group* net) {
                size_t local_value = net->my_host_rank();
                ReduceToRoot(*net, local_value);
                if (net->my_host_rank() == 0)
                    ASSERT_EQ(local_value, net->num_hosts() * (net->num_hosts() - 1) / 2);
            });
    }
}

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

TEST(Group, TestBroadcast) {
    for (size_t p = 0; p <= 8; ++p) {
        // Construct Group of p workers which perform an Broadcast collective
        Group::ExecuteLocalMock(
            p, [](Group* net) {
                size_t local_value;
                if (net->my_host_rank() == 0) local_value = 42;
                Broadcast(*net, local_value);
                ASSERT_EQ(local_value, 42u);
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
