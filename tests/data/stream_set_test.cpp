/*******************************************************************************
 * tests/data/stream_set_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/cat_stream.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/net/mock/group.hpp>

#include <string>
#include <vector>

using namespace thrill;

static const bool debug = true;
static const size_t test_block_size = 1024;

TEST(StreamSet, TestLoopbacks) {
    size_t workers_per_host = 3;
    size_t hosts = 1;
    auto groups = net::mock::Group::ConstructLoopbackMesh(hosts);
    net::Group* group = groups[0].get();
    mem::Manager mem_manager(nullptr, "Benchmark");
    data::BlockPool block_pool;
    data::Multiplexer multiplexer(mem_manager, block_pool, workers_per_host, *group);

    auto producer = [workers_per_host](std::shared_ptr<data::CatStream> stream, size_t my_id) {
                        common::NameThisThread("worker " + mem::to_string(my_id));
                        // send data between workers
                        auto writers = stream->OpenWriters(test_block_size);
                        for (size_t j = 0; j < workers_per_host; j++) {
                            sLOG << "sending from" << my_id << "to" << j;
                            writers[j].Put(std::to_string(my_id) + "->" + std::to_string(j));
                            writers[j].Close();
                        }
                    };
    auto consumer = [workers_per_host](std::shared_ptr<data::CatStream> stream, size_t my_id) {
                        common::NameThisThread("worker " + mem::to_string(my_id));
                        // check data on each worker
                        auto readers = stream->OpenReaders();
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

    // no we cannot use ExecuteLocalMock, because we need the same
    // CatStreamSet instance for all the threads.
    auto stream0 = multiplexer.GetOrCreateCatStream(0, 0);
    auto stream1 = multiplexer.GetOrCreateCatStream(0, 1);
    auto stream2 = multiplexer.GetOrCreateCatStream(0, 2);
    producer(stream0, 0);
    producer(stream1, 1);
    producer(stream2, 2);
    consumer(stream0, 0);
    consumer(stream1, 1);
    consumer(stream2, 2);
}

/******************************************************************************/
