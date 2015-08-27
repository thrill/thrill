/*******************************************************************************
 * tests/data/channel_set.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/multiplexer.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/net/group.hpp>
#include <c7a/common/thread_pool.hpp>
#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace c7a;

static const bool debug = true;
static const size_t test_block_size = 1024;


TEST(ChannelSet, TestLoopbacks) {
    size_t workers_per_host = 3;
    size_t hosts = 1;
    auto groups = net::Group::ConstructLocalMesh(hosts);
    data::Multiplexer multiplexer(workers_per_host, groups[0]);

    auto producer = [workers_per_host](std::shared_ptr<data::Channel> channel, size_t my_id){
        common::NameThisThread("worker " + std::to_string(my_id));
        //send data between workers
        auto writers = channel->OpenWriters(test_block_size);
        for (size_t j = 0; j < workers_per_host; j++) {
            sLOG << "sending from" << my_id << "to" << j;
            writers[j](std::to_string(my_id) + "->" + std::to_string(j));
            writers[j].Close();
        }
    };
    auto consumer = [workers_per_host](std::shared_ptr<data::Channel> channel, size_t my_id){
        common::NameThisThread("worker " + std::to_string(my_id));
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
