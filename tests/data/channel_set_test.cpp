/*******************************************************************************
 * tests/data/channel_set.cpp
 *
 * Part of Project thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/data/multiplexer.hpp>
#include <thrill/data/channel.hpp>
#include <thrill/net/mock/group.hpp>
#include <thrill/common/thread_pool.hpp>
#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace thrill;

static const bool debug = true;
static const size_t test_block_size = 1024;


TEST(ChannelSet, TestLoopbacks) {
    size_t workers_per_host = 3;
    size_t hosts = 1;
    auto groups = net::mock::Group::ConstructLocalMesh(hosts);
    net::Group* group = groups[0].get();
    mem::Manager mem_manager(nullptr, "Benchmark");
    data::BlockPool block_pool(&mem_manager);
    data::Multiplexer multiplexer(mem_manager, block_pool, workers_per_host, *group);

    auto producer = [workers_per_host](std::shared_ptr<data::Channel> channel, size_t my_id){
        common::NameThisThread("worker " + mem::to_string(my_id));
        //send data between workers
        auto writers = channel->OpenWriters(test_block_size);
        for (size_t j = 0; j < workers_per_host; j++) {
            sLOG << "sending from" << my_id << "to" << j;
            writers[j](std::to_string(my_id) + "->" + std::to_string(j));
            writers[j].Close();
        }
    };
    auto consumer = [workers_per_host](std::shared_ptr<data::Channel> channel, size_t my_id){
        common::NameThisThread("worker " + mem::to_string(my_id));
        //check data on each worker
        auto readers = channel->OpenReaders();
        for (size_t j = 0; j < workers_per_host; j++) {
            /*std::string expected = std::to_string(j) + "->" + std::to_string(my_id);
            std::string actual = readers[j].Next<std::string>();
            ASSERT_EQ(expected, actual);*/
            auto vec = readers[j].ReadComplete<std::string>();
            for (auto& x : vec)
                std::cout << x << ",";
            std::cout << std::endl;
        }
    };

    //no we cannot use ExecuteLocalMock, because we need the same
    //ChannelSet instance for all the threads.
    auto channel0 = multiplexer.GetOrCreateChannel(0, 0);
    auto channel1 = multiplexer.GetOrCreateChannel(0, 1);
    auto channel2 = multiplexer.GetOrCreateChannel(0, 2);
    producer(channel0, 0);
    producer(channel1, 1);
    producer(channel2, 2);
    consumer(channel0, 0);
    consumer(channel1, 1);
    consumer(channel2, 2);
}

/******************************************************************************/
