/*******************************************************************************
 * tests/data/multiplexer_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/data/cat_stream.hpp>
#include <thrill/data/mix_stream.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/data/multiplexer_header.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/group.hpp>
#include <thrill/net/mock/group.hpp>

#include <algorithm>
#include <string>
#include <vector>

using namespace thrill;

static constexpr bool debug = false;
static constexpr size_t test_block_size = 1024;

/******************************************************************************/
// MultiplexerHeader tests

struct MultiplexerHeaderTest : public ::testing::Test {
    MultiplexerHeaderTest() {
        candidate.stream_id = 2;
        candidate.size = 4;
        candidate.num_items = 5;
        candidate.sender_worker = 6;
    }

    data::StreamMultiplexerHeader candidate;
};

TEST_F(MultiplexerHeaderTest, ParsesAndSerializesHeader) {
    net::BufferBuilder bb;
    candidate.Serialize(bb);
    net::Buffer b = bb.ToBuffer();

    net::BufferReader br(b);
    data::StreamMultiplexerHeader result =
        data::StreamMultiplexerHeader::Parse(br);

    ASSERT_EQ(candidate.stream_id, result.stream_id);
    ASSERT_EQ(candidate.size, result.size);
    ASSERT_EQ(candidate.num_items, result.num_items);
    ASSERT_EQ(candidate.sender_worker, result.sender_worker);
}

TEST_F(MultiplexerHeaderTest, HeaderIsEnd) {
    ASSERT_FALSE(candidate.IsEnd());
    candidate.size = 0;
    ASSERT_TRUE(candidate.IsEnd());
}

/******************************************************************************/
// Multiplexer StreamSet tests

TEST(StreamSet, TestLoopbacks) {
    size_t workers_per_host = 3;
    size_t hosts = 1;
    data::default_block_size = test_block_size;

    auto groups = net::mock::Group::ConstructLoopbackMesh(hosts);
    net::Group* group = groups[0].get();
    mem::Manager mem_manager(nullptr, "Benchmark");
    net::DispatcherThread disp(group->ConstructDispatcher(), 0);
    data::BlockPool block_pool(workers_per_host);
    data::Multiplexer multiplexer(mem_manager, block_pool, disp, *group, workers_per_host);

    auto producer =
        [workers_per_host](data::CatStreamDataPtr stream, size_t my_id) {
            common::NameThisThread("worker " + mem::to_string(my_id));
            // send data between workers
            auto writers = stream->GetWriters();
            for (size_t j = 0; j < workers_per_host; j++) {
                sLOG << "sending from" << my_id << "to" << j;
                writers[j].Put(std::to_string(my_id) + "->" + std::to_string(j));
                writers[j].Close();
            }
        };
    auto consumer =
        [workers_per_host](data::CatStreamDataPtr stream, size_t my_id) {
            common::NameThisThread("worker " + mem::to_string(my_id));
            // check data on each worker
            auto readers = stream->GetReaders();
            for (size_t j = 0; j < workers_per_host; j++) {
                std::string expected = std::to_string(j) + "->" + std::to_string(my_id);
                std::string actual = readers[j].Next<std::string>();
                ASSERT_EQ(expected, actual);
                ASSERT_FALSE(readers[j].HasNext());
            }
        };

    // no we cannot use ExecuteLocalMock, because we need the same
    // CatStreamSet instance for all the threads.
    auto stream0 = multiplexer.GetOrCreateCatStreamData(0, 0, /* dia_id */ 0);
    auto stream1 = multiplexer.GetOrCreateCatStreamData(0, 1, /* dia_id */ 0);
    auto stream2 = multiplexer.GetOrCreateCatStreamData(0, 2, /* dia_id */ 0);
    producer(stream0, 0);
    producer(stream1, 1);
    producer(stream2, 2);
    consumer(stream0, 0);
    consumer(stream1, 1);
    consumer(stream2, 2);
    stream0->Close();
    stream1->Close();
    stream2->Close();
}

/******************************************************************************/
// Multiplexer tests

struct Multiplexer : public ::testing::Test {

    using WorkerThread = std::function<void(data::Multiplexer&)>;

    static void FunctionSelect(
        net::Group* group, WorkerThread f1, WorkerThread f2, WorkerThread f3) {
        mem::Manager mem_manager(nullptr, "MultiplexerTest");
        net::DispatcherThread disp(group->ConstructDispatcher(), 0);
        data::BlockPool block_pool;
        data::Multiplexer multiplexer(mem_manager, block_pool, disp, *group, 1);
        switch (group->my_host_rank()) {
        case 0:
            common::NameThisThread("t0");
            if (f1) f1(multiplexer);
            break;
        case 1:
            common::NameThisThread("t1");
            if (f2) f2(multiplexer);
            break;
        case 2:
            common::NameThisThread("t2");
            if (f3) f3(multiplexer);
            break;
        }
        multiplexer.Close();
        disp.Terminate();
    }

    static void Execute(WorkerThread f1 = nullptr,
                        WorkerThread f2 = nullptr,
                        WorkerThread f3 = nullptr) {
        net::RunLoopbackGroupTest(
            // calculate number of threads
            (f1 ? 1 : 0) + (f2 ? 1 : 0) + (f3 ? 1 : 0),
            [=](net::Group* g) {
                try {
                    FunctionSelect(g, f1, f2, f3);
                }
                catch (std::exception& e) {
                    LOG1 << "Caught exception " << typeid(e).name();
                    LOG1 << "  what(): " << e.what();
                    throw;
                }
            });
    }
};

// open a Stream via data::Multiplexer, and send a short message to all workers,
// receive and check the message.
void TalkAllToAllViaCatStream(net::Group* net) {
    common::NameThisThread("chmp" + mem::to_string(net->my_host_rank()));

    unsigned char send_buffer[123];
    for (size_t i = 0; i != sizeof(send_buffer); ++i)
        send_buffer[i] = static_cast<unsigned char>(i);

    static constexpr size_t iterations = 1000;
    size_t num_hosts = net->num_hosts();
    size_t num_workers_per_host = 2;
    size_t total_workers = num_hosts * num_workers_per_host;

    data::default_block_size = test_block_size;

    mem::Manager mem_manager(nullptr, "Benchmark");
    data::BlockPool block_pool(num_workers_per_host);
    net::DispatcherThread disp(net->ConstructDispatcher(), 0);
    data::Multiplexer multiplexer(
        mem_manager, block_pool, disp, *net, num_workers_per_host);

    auto thread_func =
        [&](size_t my_local_worker_id) {

            auto stream = multiplexer.GetNewCatStream(
                my_local_worker_id, /* dia_id */ 0);

            size_t my_worker_rank =
                net->my_host_rank() * num_workers_per_host + my_local_worker_id;

            // open Writers and send a message to all workers

            auto writers = stream->GetWriters();

            for (size_t tgt = 0; tgt != writers.size(); ++tgt) {
                std::string msg = "hello I am ";
                msg += ('0' + my_worker_rank);
                msg += " calling ";
                msg += ('0' + tgt);
                writers[tgt].Put(msg);
                writers[tgt].Flush();

                // write a few MiBs of oddly sized raw data
                for (size_t r = 0; r != iterations; ++r) {
                    writers[tgt].Append(send_buffer, sizeof(send_buffer));
                }

                // add one more important integer
                writers[tgt].Put<uint32_t>(42u);

                writers[tgt].Flush();
                writers[tgt].Close();
            }

            // open Readers and receive message from all workers

            auto readers = stream->GetReaders();

            for (size_t src = 0; src != readers.size(); ++src) {
                std::string msg = readers[src].Next<std::string>();

                std::string expect = "hello I am ";
                expect += ('0' + src);
                expect += " calling ";
                expect += ('0' + my_worker_rank);
                ASSERT_EQ(msg, expect);

                sLOG << net->my_host_rank() << "got msg from" << src;

                ASSERT_TRUE(readers[src].HasNext());

                // read a few MiBs of oddly sized raw data
                unsigned char recv_buffer[sizeof(send_buffer)];

                for (size_t r = 0; r != iterations; ++r) {
                    readers[src].Read(recv_buffer, sizeof(recv_buffer));

                    ASSERT_TRUE(std::equal(send_buffer,
                                           send_buffer + sizeof(send_buffer),
                                           recv_buffer));
                }

                ASSERT_TRUE(readers[src].HasNext());

                // read important integer
                uint32_t x = readers[src].Next<uint32_t>();
                ASSERT_EQ(x, 42u);

                ASSERT_FALSE(readers[src].HasNext());
            }

            stream->Close();

            /*----------------------------------------------------------------*/
            // check Stream statistics

            std::this_thread::sleep_for(std::chrono::milliseconds(500));

            ASSERT_EQ(2 * total_workers, stream->tx_items());
            ASSERT_EQ(2 * total_workers, stream->rx_items());

            const size_t self_verify =
                data::CatStream::Writer::self_verify ? 8 : 0;

            ASSERT_EQ(total_workers *
                      (self_verify + 1 + 22
                       + iterations * sizeof(send_buffer)
                       + self_verify + sizeof(uint32_t))
                      // add headers of blocks
                      + stream->tx_net_blocks() * data::MultiplexerHeader::total_size,
                      stream->tx_bytes());

            ASSERT_EQ(stream->tx_bytes(), stream->rx_bytes());
        };

    std::thread t0 = std::thread(thread_func, 0);
    std::thread t1 = std::thread(thread_func, 1);
    t0.join(), t1.join();
}

TEST_F(Multiplexer, TalkAllToAllViaCatStreamForManyNetSizes) {
    // test for all network mesh sizes 1, 2, 5, 9:
    net::RunLoopbackGroupTest(1, TalkAllToAllViaCatStream);
    net::RunLoopbackGroupTest(2, TalkAllToAllViaCatStream);
    net::RunLoopbackGroupTest(5, TalkAllToAllViaCatStream);
    net::RunLoopbackGroupTest(9, TalkAllToAllViaCatStream);
}

TEST_F(Multiplexer, ReadCompleteCatStream) {
    data::default_block_size = test_block_size;
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            std::string msg1 = "I came from worker 0";
            std::string msg2 = "I am another message from worker 0";
            writers[2].Put(msg1);
            // writers[2].Flush();
            writers[2].Put(msg2);
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }
        };
    auto w1 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            std::string msg1 = "I came from worker 1";
            writers[2].Put(msg1);
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }
        };
    auto w2 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }

            auto reader = c->GetCatReader(true);
            ASSERT_EQ("I came from worker 0", reader.Next<std::string>());
            ASSERT_EQ("I am another message from worker 0", reader.Next<std::string>());
            ASSERT_EQ("I came from worker 1", reader.Next<std::string>());
        };
    Execute(w0, w1, w2);
}

TEST_F(Multiplexer, ReadCompleteCatStreamManyTimes) {
    data::default_block_size = test_block_size;
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            std::string msg1 = "I came from worker 0";
            std::string msg2 = "I am another message from worker 0";
            writers[2].Put(msg1);
            // writers[2].Flush();
            writers[2].Put(msg2);
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }
        };
    auto w1 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            std::string msg1 = "I came from worker 1";
            writers[2].Put(msg1);
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }
        };
    auto w2 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }
            {
                auto reader = c->GetCatReader(false);
                ASSERT_EQ("I came from worker 0",
                          reader.Next<std::string>());
                ASSERT_EQ("I am another message from worker 0",
                          reader.Next<std::string>());
                ASSERT_EQ("I came from worker 1",
                          reader.Next<std::string>());
                ASSERT_TRUE(!reader.HasNext());
            }
            {
                auto reader = c->GetCatReader(false);
                ASSERT_EQ("I came from worker 0",
                          reader.Next<std::string>());
                ASSERT_EQ("I am another message from worker 0",
                          reader.Next<std::string>());
                ASSERT_EQ("I came from worker 1",
                          reader.Next<std::string>());
                ASSERT_TRUE(!reader.HasNext());
            }
            {
                auto reader = c->GetCatReader(true);
                ASSERT_EQ("I came from worker 0",
                          reader.Next<std::string>());
                ASSERT_EQ("I am another message from worker 0",
                          reader.Next<std::string>());
                ASSERT_EQ("I came from worker 1",
                          reader.Next<std::string>());
                ASSERT_TRUE(!reader.HasNext());
            }
        };
    Execute(w0, w1, w2);
}

/******************************************************************************/
// MixStream Tests

TEST_F(Multiplexer, ReadCompleteMixStreamManyTimes) {
    data::default_block_size = test_block_size;
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewMixStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            std::string msg1 = "I came from worker 0";
            std::string msg2 = "I am another message from worker 0";
            writers[2].Put(msg1);
            // writers[2].Flush();
            writers[2].Put(msg2);
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }
            auto reader = c->GetMixReader(false);
            ASSERT_TRUE(!reader.HasNext());
        };
    auto w1 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewMixStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            std::string msg1 = "I came from worker 1";
            writers[2].Put(msg1);
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }
            auto reader = c->GetMixReader(false);
            ASSERT_TRUE(!reader.HasNext());
        };
    auto w2 =
        [](data::Multiplexer& multiplexer) {
            auto c = multiplexer.GetNewMixStream(0, /* dia_id */ 0);
            auto writers = c->GetWriters();
            for (auto& w : writers) {
                sLOG << "close worker";
                w.Close();
            }
            {
                auto reader = c->GetMixReader(/* consume */ false);
                // receive three std::string items
                std::vector<std::string> recv;

                for (size_t i = 0; i < 3; ++i) {
                    recv.emplace_back(reader.Next<std::string>());
                }
                ASSERT_TRUE(!reader.HasNext());

                // check sorted strings
                std::sort(recv.begin(), recv.end());

                ASSERT_EQ(3u, recv.size());
                ASSERT_EQ("I am another message from worker 0", recv[0]);
                ASSERT_EQ("I came from worker 0", recv[1]);
                ASSERT_EQ("I came from worker 1", recv[2]);
            }
            {
                auto reader = c->GetMixReader(/* consume */ false);
                // receive three std::string items
                std::vector<std::string> recv;

                for (size_t i = 0; i < 3; ++i) {
                    recv.emplace_back(reader.Next<std::string>());
                }
                ASSERT_TRUE(!reader.HasNext());

                // check sorted strings
                std::sort(recv.begin(), recv.end());

                ASSERT_EQ(3u, recv.size());
                ASSERT_EQ("I am another message from worker 0", recv[0]);
                ASSERT_EQ("I came from worker 0", recv[1]);
                ASSERT_EQ("I came from worker 1", recv[2]);
            }
            {
                auto reader = c->GetMixReader(/* consume */ true);
                // receive three std::string items
                std::vector<std::string> recv;

                for (size_t i = 0; i < 3; ++i) {
                    recv.emplace_back(reader.Next<std::string>());
                }
                ASSERT_TRUE(!reader.HasNext());

                // check sorted strings
                std::sort(recv.begin(), recv.end());

                ASSERT_EQ(3u, recv.size());
                ASSERT_EQ("I am another message from worker 0", recv[0]);
                ASSERT_EQ("I came from worker 0", recv[1]);
                ASSERT_EQ("I came from worker 1", recv[2]);
            }
        };
    Execute(w0, w1, w2);
}

// open a Stream via data::Multiplexer, and send a short message to all workers,
// receive and check the message.
void TalkAllToAllViaMixStream(net::Group* net) {
    common::NameThisThread("chmp" + mem::to_string(net->my_host_rank()));

    char send_buffer[123];
    for (size_t i = 0; i != sizeof(send_buffer); ++i)
        send_buffer[i] = static_cast<char>(i);

    std::string send_string(send_buffer, sizeof(send_buffer));

    static constexpr size_t iterations = 1000;
    size_t num_hosts = net->num_hosts();
    size_t num_workers_per_host = 2;
    size_t total_workers = num_hosts * num_workers_per_host;

    data::default_block_size = test_block_size;

    mem::Manager mem_manager(nullptr, "Benchmark");
    data::BlockPool block_pool(num_workers_per_host);
    net::DispatcherThread disp(net->ConstructDispatcher(), 0);
    data::Multiplexer multiplexer(
        mem_manager, block_pool, disp, *net, num_workers_per_host);

    auto thread_func =
        [&](size_t my_local_worker_id) {

            auto stream = multiplexer.GetNewMixStream(
                my_local_worker_id, /* dia_id */ 0);

            size_t my_worker_rank =
                net->my_host_rank() * num_workers_per_host + my_local_worker_id;

            // open Writers and send a message to all workers

            auto writers = stream->GetWriters();

            for (size_t tgt = 0; tgt != writers.size(); ++tgt) {
                std::string msg = "hello I am ";
                msg += ('0' + my_worker_rank);
                msg += " calling ";
                msg += ('0' + tgt);
                msg += send_string;

                writers[tgt].Put(msg);
                // try a Flush.
                writers[tgt].Flush();

                // write a few MiBs of oddly sized data
                for (size_t r = 1; r < iterations; ++r) {
                    writers[tgt].Put(msg);
                }

                writers[tgt].Flush();
                writers[tgt].Close();
            }

            // open mix Reader and receive messages from all workers

            auto reader = stream->GetMixReader(true);

            std::vector<std::string> recv;

            while (reader.HasNext())
                recv.emplace_back(reader.Next<std::string>());

            // sort messages and check them

            std::sort(recv.begin(), recv.end());

            ASSERT_EQ(iterations * total_workers, recv.size());

            size_t recvi = 0;

            for (size_t src = 0; src < total_workers; ++src) {
                std::string expect = "hello I am ";
                expect += ('0' + src);
                expect += " calling ";
                expect += ('0' + my_worker_rank);
                expect += send_string;

                for (size_t i = 0; i < iterations; ++i) {
                    ASSERT_EQ(expect, recv[recvi]);
                    ++recvi;
                }
            }

            stream->Close();

            /*----------------------------------------------------------------*/
            // check Stream statistics

            ASSERT_EQ(iterations * total_workers, stream->tx_items());
            ASSERT_EQ(iterations * total_workers, stream->rx_items());

            const size_t self_verify =
                data::CatStream::Writer::self_verify ? 8 : 0;

            ASSERT_EQ(total_workers *
                      ((self_verify + 2 + 22 + sizeof(send_buffer)) * iterations)
                      // add headers of blocks
                      + stream->tx_net_blocks() * data::MultiplexerHeader::total_size,
                      stream->tx_bytes());

            ASSERT_EQ(stream->tx_bytes(), stream->rx_bytes());
        };

    std::thread t0 = std::thread(thread_func, 0);
    std::thread t1 = std::thread(thread_func, 1);
    t0.join(), t1.join();
}

TEST_F(Multiplexer, TalkAllToAllViaMixStreamForManyNetSizes) {
    // test for all network mesh sizes 1, 2, 5, 9:
    net::RunLoopbackGroupTest(1, TalkAllToAllViaMixStream);
    net::RunLoopbackGroupTest(2, TalkAllToAllViaMixStream);
    net::RunLoopbackGroupTest(5, TalkAllToAllViaMixStream);
    net::RunLoopbackGroupTest(9, TalkAllToAllViaMixStream);
}

/******************************************************************************/
// Scatter Tests

TEST_F(Multiplexer, Scatter_OneWorker) {
    data::default_block_size = test_block_size;
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool(), 0, /* dia_id */ 0);
            {
                auto writer = file.GetWriter();
                writer.Put<std::string>("foo");
                writer.Put<std::string>("bar");
                writer.Flush();
                writer.Put<std::string>(
                    "breakfast is the most important meal of the day.");
            }

            // scatter File contents via stream: only items [0,3) are sent
            auto ch = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            ch->Scatter<std::string>(file, { 0, 2 });

            // check that got items
            auto reader = ch->GetCatReader(true);
            ASSERT_TRUE(reader.HasNext());
            ASSERT_EQ(reader.Next<std::string>(), "foo");
            ASSERT_TRUE(reader.HasNext());
            ASSERT_EQ(reader.Next<std::string>(), "bar");
            ASSERT_FALSE(reader.HasNext());
        };
    Execute(w0);
}

TEST_F(Multiplexer, Scatter_TwoWorkers_OnlyLocalCopy) {
    data::default_block_size = test_block_size;
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool(), 0, /* dia_id */ 0);
            {
                auto writer = file.GetWriter();
                writer.Put<std::string>("foo");
                writer.Put<std::string>("bar");
            }

            // scatter File contents via stream: only items [0,2) are to local worker
            auto ch = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            ch->Scatter<std::string>(file, { 0, 2, 2 });

            // check that got items
            auto res = ch->GetCatReader(true).ReadComplete<std::string>();
            ASSERT_EQ(res, (std::vector<std::string>{ "foo", "bar" }));
        };
    auto w1 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool(), 0, /* dia_id */ 0);
            {
                auto writer = file.GetWriter();
                writer.Put<std::string>("hello");
                writer.Put<std::string>("world");
                writer.Put<std::string>(".");
            }

            // scatter File contents via stream: only items [0,3) are to local worker
            auto ch = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            ch->Scatter<std::string>(file, { 0, 0, 3 });

            // check that got items
            auto res = ch->GetCatReader(true).ReadComplete<std::string>();
            ASSERT_EQ(res, (std::vector<std::string>{ "hello", "world", "." }));
        };
    Execute(w0, w1);
}

TEST_F(Multiplexer, Scatter_TwoWorkers_CompleteExchange) {
    data::default_block_size = test_block_size;
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool(), 0, /* dia_id */ 0);
            {
                auto writer = file.GetWriter();
                writer.Put<std::string>("foo");
                writer.Put<std::string>("bar");
            }

            // scatter File contents via stream.
            auto ch = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            ch->Scatter<std::string>(file, { 0, 1, 2 });

            // check that got items
            auto res = ch->GetCatReader(true).ReadComplete<std::string>();
            ASSERT_EQ(res, (std::vector<std::string>{ "foo", "hello" }));
        };
    auto w1 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool(), 0, /* dia_id */ 0);
            {
                auto writer = file.GetWriter();
                writer.Put<std::string>("hello");
                writer.Put<std::string>("world");
                writer.Put<std::string>(".");
            }

            // scatter File contents via stream.
            auto ch = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            ch->Scatter<std::string>(file, { 0, 1, 2 });

            // check that got items
            auto res = ch->GetCatReader(true).ReadComplete<std::string>();
            ASSERT_EQ(res, (std::vector<std::string>{ "bar", "world" }));
        };
    Execute(w0, w1);
}

TEST_F(Multiplexer, Scatter_ThreeWorkers_PartialExchange) {
    data::default_block_size = test_block_size;
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool(), 0, /* dia_id */ 0);
            {
                auto writer = file.GetWriter();
                writer.Put<int>(1);
                writer.Put<int>(2);
            }

            // scatter File contents via stream.
            auto ch = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            ch->Scatter<int>(file, { 0, 2, 2, 2 });

            // check that got items
            auto res = ch->GetCatReader(true).ReadComplete<int>();
            ASSERT_EQ(res, (std::vector<int>{ 1, 2 }));
        };
    auto w1 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool(), 0, /* dia_id */ 0);
            {
                auto writer = file.GetWriter();
                writer.Put<int>(3);
                writer.Put<int>(4);
                writer.Put<int>(5);
                writer.Put<int>(6);
            }

            // scatter File contents via stream.
            auto ch = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            ch->Scatter<int>(file, { 0, 0, 2, 4 });

            // check that got items
            auto res = ch->GetCatReader(true).ReadComplete<int>();
            ASSERT_EQ(res, (std::vector<int>{ 3, 4 }));
        };
    auto w2 =
        [](data::Multiplexer& multiplexer) {
            // empty File :...(
            data::File file(multiplexer.block_pool(), 0, /* dia_id */ 0);

            // scatter File contents via stream.
            auto ch = multiplexer.GetNewCatStream(0, /* dia_id */ 0);
            ch->Scatter<int>(file, { 0, 0, 0, 0 });

            // check that got items
            auto res = ch->GetCatReader(true).ReadComplete<int>();
            ASSERT_EQ(res, (std::vector<int>{ 5, 6 }));
        };
    Execute(w0, w1, w2);
}

/******************************************************************************/
