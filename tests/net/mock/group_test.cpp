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

#include <thread>

using namespace thrill;      // NOLINT

void MockTest(const std::function<void(net::mock::Group*)>& thread_function) {

    size_t group_size = 8;

    std::vector<net::mock::Group> groups(group_size);

    for (size_t i = 0; i < groups.size(); ++i) {
        groups[i].my_rank_ = i;
        groups[i].inbound_.resize(group_size);
        groups[i].peers_.resize(group_size);
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

TEST(MockGroup, RealInitializeAndClose) {
    MockTest([](net::mock::Group* net) {
                 size_t local_value = 1;
                 net::PrefixSumForPowersOfTwo(*net, local_value);
                 ASSERT_EQ(local_value, net->my_host_rank() + 1);
             });
}

/******************************************************************************/
