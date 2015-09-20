/*******************************************************************************
 * tests/data/multiplexer_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/data/cat_stream.hpp>
#include <thrill/data/mix_stream.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/group.hpp>

#include <algorithm>
#include <string>
#include <vector>

using namespace thrill;

static const bool debug = false;
static const size_t test_block_size = 1024;

struct Multiplexer : public::testing::Test {

    using WorkerThread = std::function<void(data::Multiplexer&)>;

    static void FunctionSelect(
        net::Group* group, WorkerThread f1, WorkerThread f2, WorkerThread f3) {
        mem::Manager mem_manager(nullptr, "MultiplexerTest");
        data::BlockPool block_pool(&mem_manager);
        data::Multiplexer multiplexer(mem_manager, block_pool, 1, *group);
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
    }

    static void Execute(WorkerThread f1 = nullptr,
                        WorkerThread f2 = nullptr,
                        WorkerThread f3 = nullptr) {
        net::RunGroupTest(
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

    static const size_t iterations = 1000;
    size_t my_local_worker_id = 0;
    size_t num_workers_per_host = 1;

    mem::Manager mem_manager(nullptr, "Benchmark");
    data::BlockPool block_pool(&mem_manager);
    data::Multiplexer multiplexer(mem_manager, block_pool, num_workers_per_host, *net);
    {
        data::StreamId id = multiplexer.AllocateCatStreamId(my_local_worker_id);

        // open Writers and send a message to all workers

        auto writers = multiplexer.GetOrCreateCatStream(
            id, my_local_worker_id)->OpenWriters(test_block_size);

        for (size_t tgt = 0; tgt != writers.size(); ++tgt) {
            writers[tgt]("hello I am " + std::to_string(net->my_host_rank())
                         + " calling " + std::to_string(tgt));

            writers[tgt].Flush();

            // write a few MiBs of oddly sized data
            for (size_t r = 0; r != iterations; ++r) {
                writers[tgt].Append(send_buffer, sizeof(send_buffer));
            }

            writers[tgt].Flush();
            writers[tgt].Close();
        }

        // open Readers and receive message from all workers

        auto readers = multiplexer.GetOrCreateCatStream(
            id, my_local_worker_id)->OpenReaders();

        for (size_t src = 0; src != readers.size(); ++src) {
            std::string msg = readers[src].Next<std::string>();

            ASSERT_EQ(msg, "hello I am " + std::to_string(src)
                      + " calling " + std::to_string(net->my_host_rank()));

            sLOG << net->my_host_rank() << "got msg from" << src;

            // read a few MiBs of oddly sized data
            unsigned char recv_buffer[sizeof(send_buffer)];

            for (size_t r = 0; r != iterations; ++r) {
                readers[src].Read(recv_buffer, sizeof(recv_buffer));

                ASSERT_TRUE(std::equal(send_buffer,
                                       send_buffer + sizeof(send_buffer),
                                       recv_buffer));
            }
        }
    }
}

TEST_F(Multiplexer, TalkAllToAllViaCatStreamForManyNetSizes) {
    // test for all network mesh sizes 1, 2, 5, 9:
    net::RunGroupTest(1, TalkAllToAllViaCatStream);
    net::RunGroupTest(2, TalkAllToAllViaCatStream);
    net::RunGroupTest(5, TalkAllToAllViaCatStream);
    net::RunGroupTest(9, TalkAllToAllViaCatStream);
}

TEST_F(Multiplexer, ReadCompleteCatStream) {
    auto w0 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateCatStreamId(0);
                  auto c = multiplexer.GetOrCreateCatStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  std::string msg1 = "I came from worker 0";
                  std::string msg2 = "I am another message from worker 0";
                  writers[2](msg1);
                  // writers[2].Flush();
                  writers[2](msg2);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w1 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateCatStreamId(0);
                  auto c = multiplexer.GetOrCreateCatStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  std::string msg1 = "I came from worker 1";
                  writers[2](msg1);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w2 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateCatStreamId(0);
                  auto c = multiplexer.GetOrCreateCatStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }

                  auto reader = c->OpenCatReader(true);
                  ASSERT_EQ("I came from worker 0", reader.Next<std::string>());
                  ASSERT_EQ("I am another message from worker 0", reader.Next<std::string>());
                  ASSERT_EQ("I came from worker 1", reader.Next<std::string>());
              };
    Execute(w0, w1, w2);
}

TEST_F(Multiplexer, ReadCompleteCatStreamManyTimes) {
    auto w0 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateCatStreamId(0);
                  auto c = multiplexer.GetOrCreateCatStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  std::string msg1 = "I came from worker 0";
                  std::string msg2 = "I am another message from worker 0";
                  writers[2](msg1);
                  // writers[2].Flush();
                  writers[2](msg2);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w1 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateCatStreamId(0);
                  auto c = multiplexer.GetOrCreateCatStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  std::string msg1 = "I came from worker 1";
                  writers[2](msg1);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w2 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateCatStreamId(0);
                  auto c = multiplexer.GetOrCreateCatStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
                  {
                      auto reader = c->OpenCatReader(false);
                      ASSERT_EQ("I came from worker 0",
                                reader.Next<std::string>());
                      ASSERT_EQ("I am another message from worker 0",
                                reader.Next<std::string>());
                      ASSERT_EQ("I came from worker 1",
                                reader.Next<std::string>());
                      ASSERT_TRUE(!reader.HasNext());
                  }
                  {
                      auto reader = c->OpenCatReader(false);
                      ASSERT_EQ("I came from worker 0",
                                reader.Next<std::string>());
                      ASSERT_EQ("I am another message from worker 0",
                                reader.Next<std::string>());
                      ASSERT_EQ("I came from worker 1",
                                reader.Next<std::string>());
                      ASSERT_TRUE(!reader.HasNext());
                  }
                  {
                      auto reader = c->OpenCatReader(true);
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
    auto w0 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateMixStreamId(0);
                  auto c = multiplexer.GetOrCreateMixStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  std::string msg1 = "I came from worker 0";
                  std::string msg2 = "I am another message from worker 0";
                  writers[2](msg1);
                  // writers[2].Flush();
                  writers[2](msg2);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w1 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateMixStreamId(0);
                  auto c = multiplexer.GetOrCreateMixStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  std::string msg1 = "I came from worker 1";
                  writers[2](msg1);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w2 = [](data::Multiplexer& multiplexer) {
                  auto id = multiplexer.AllocateMixStreamId(0);
                  auto c = multiplexer.GetOrCreateMixStream(id, 0);
                  auto writers = c->OpenWriters(test_block_size);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
                  {
                      auto reader = c->OpenMixReader(false);
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
                      auto reader = c->OpenMixReader(false);
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
                      auto reader = c->OpenMixReader(true);
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

    static const size_t iterations = 1000;
    size_t my_local_worker_id = 0;
    size_t num_workers_per_host = 1;

    mem::Manager mem_manager(nullptr, "Benchmark");
    data::BlockPool block_pool(&mem_manager);
    data::Multiplexer multiplexer(mem_manager, block_pool, num_workers_per_host, *net);
    {
        data::StreamId id = multiplexer.AllocateMixStreamId(my_local_worker_id);

        // open Writers and send a message to all workers

        auto writers = multiplexer.GetOrCreateMixStream(
            id, my_local_worker_id)->OpenWriters(test_block_size);

        for (size_t tgt = 0; tgt != writers.size(); ++tgt) {
            std::string txt =
                "hello I am " + std::to_string(net->my_host_rank())
                + " calling " + std::to_string(tgt)
                + send_string;

            writers[tgt].PutItem(txt);
            // try a Flush.
            writers[tgt].Flush();

            // write a few MiBs of oddly sized data
            for (size_t r = 1; r < iterations; ++r) {
                writers[tgt].PutItem(txt);
            }

            writers[tgt].Flush();
            writers[tgt].Close();
        }

        // open mix Reader and receive messages from all workers

        auto reader = multiplexer.GetOrCreateMixStream(
            id, my_local_worker_id)->OpenMixReader(true);

        std::vector<std::string> recv;

        while (reader.HasNext())
            recv.emplace_back(reader.Next<std::string>());

        // sort messages and check them

        std::sort(recv.begin(), recv.end());

        ASSERT_EQ(iterations * net->num_hosts(), recv.size());

        size_t i = 0;

        for (size_t src = 0; src < net->num_hosts(); ++src) {
            std::string txt =
                "hello I am " + std::to_string(src)
                + " calling " + std::to_string(net->my_host_rank())
                + send_string;

            for (size_t iter = 0; iter < iterations; ++iter) {
                ASSERT_EQ(txt, recv[i]);
                ++i;
            }
        }
    }
}

TEST_F(Multiplexer, DISABLED_TalkAllToAllViaMixStreamForManyNetSizes) {
    // test for all network mesh sizes 1, 2, 5, 9:
    net::RunGroupTest(1, TalkAllToAllViaMixStream);
    net::RunGroupTest(2, TalkAllToAllViaMixStream);
    net::RunGroupTest(5, TalkAllToAllViaMixStream);
    net::RunGroupTest(9, TalkAllToAllViaMixStream);
    // the test does not work for two digit #workers (due to sorting digits)
}

/******************************************************************************/
// Scatter Tests

TEST_F(Multiplexer, Scatter_OneWorker) {
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool());
            {
                auto writer = file.GetWriter(test_block_size);
                writer(std::string("foo"));
                writer(std::string("bar"));
                writer.Flush();
                writer(std::string("breakfast is the most important meal of the day."));
            }

            // scatter File contents via stream: only items [0,3) are sent
            data::StreamId id = multiplexer.AllocateCatStreamId(0);
            auto ch = multiplexer.GetOrCreateCatStream(id, 0);
            ch->Scatter<std::string>(file, { 2 });

            // check that got items
            auto reader = ch->OpenCatReader(true);
            ASSERT_TRUE(reader.HasNext());
            ASSERT_EQ(reader.Next<std::string>(), "foo");
            ASSERT_TRUE(reader.HasNext());
            ASSERT_EQ(reader.Next<std::string>(), "bar");
            ASSERT_FALSE(reader.HasNext());
        };
    Execute(w0);
}

TEST_F(Multiplexer, Scatter_TwoWorkers_OnlyLocalCopy) {
    auto w0 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool());
            {
                auto writer = file.GetWriter(test_block_size);
                writer(std::string("foo"));
                writer(std::string("bar"));
            }

            // scatter File contents via stream: only items [0,2) are to local worker
            data::StreamId id = multiplexer.AllocateCatStreamId(0);
            auto ch = multiplexer.GetOrCreateCatStream(id, 0);
            ch->Scatter<std::string>(file, { 2, 2 });

            // check that got items
            auto res = ch->OpenCatReader(true).ReadComplete<std::string>();
            ASSERT_EQ(res, (std::vector<std::string>{ "foo", "bar" }));
        };
    auto w1 =
        [](data::Multiplexer& multiplexer) {
            // produce a File containing some items
            data::File file(multiplexer.block_pool());
            {
                auto writer = file.GetWriter(test_block_size);
                writer(std::string("hello"));
                writer(std::string("world"));
                writer(std::string("."));
            }

            // scatter File contents via stream: only items [0,3) are to local worker
            data::StreamId id = multiplexer.AllocateCatStreamId(0);
            auto ch = multiplexer.GetOrCreateCatStream(id, 0);
            ch->Scatter<std::string>(file, { 0, 3 });

            // check that got items
            auto res = ch->OpenCatReader(true).ReadComplete<std::string>();
            ASSERT_EQ(res, (std::vector<std::string>{ "hello", "world", "." }));
        };
    Execute(w0, w1);
}

TEST_F(Multiplexer, Scatter_TwoWorkers_CompleteExchange) {
    auto w0 = [](data::Multiplexer& multiplexer) {
                  // produce a File containing some items
                  data::File file(multiplexer.block_pool());
                  {
                      auto writer = file.GetWriter(test_block_size);
                      writer(std::string("foo"));
                      writer(std::string("bar"));
                  }

                  // scatter File contents via stream.
                  data::StreamId id = multiplexer.AllocateCatStreamId(0);
                  auto ch = multiplexer.GetOrCreateCatStream(id, 0);
                  ch->Scatter<std::string>(file, { 1, 2 });

                  // check that got items
                  auto res = ch->OpenCatReader(true).ReadComplete<std::string>();
                  ASSERT_EQ(res, (std::vector<std::string>{ "foo", "hello" }));
              };
    auto w1 = [](data::Multiplexer& multiplexer) {
                  // produce a File containing some items
                  data::File file(multiplexer.block_pool());
                  {
                      auto writer = file.GetWriter(test_block_size);
                      writer(std::string("hello"));
                      writer(std::string("world"));
                      writer(std::string("."));
                  }

                  // scatter File contents via stream.
                  data::StreamId id = multiplexer.AllocateCatStreamId(0);
                  auto ch = multiplexer.GetOrCreateCatStream(id, 0);
                  ch->Scatter<std::string>(file, { 1, 2 });

                  // check that got items
                  auto res = ch->OpenCatReader(true).ReadComplete<std::string>();
                  ASSERT_EQ(res, (std::vector<std::string>{ "bar", "world" }));
              };
    Execute(w0, w1);
}

TEST_F(Multiplexer, Scatter_ThreeWorkers_PartialExchange) {
    auto w0 = [](data::Multiplexer& multiplexer) {
                  // produce a File containing some items
                  data::File file(multiplexer.block_pool());
                  {
                      auto writer = file.GetWriter(test_block_size);
                      writer(1);
                      writer(2);
                  }

                  // scatter File contents via stream.
                  data::StreamId id = multiplexer.AllocateCatStreamId(0);
                  auto ch = multiplexer.GetOrCreateCatStream(id, 0);
                  ch->Scatter<int>(file, { 2, 2, 2 });

                  // check that got items
                  auto res = ch->OpenCatReader(true).ReadComplete<int>();
                  ASSERT_EQ(res, (std::vector<int>{ 1, 2 }));
              };
    auto w1 = [](data::Multiplexer& multiplexer) {
                  // produce a File containing some items
                  data::File file(multiplexer.block_pool());
                  {
                      auto writer = file.GetWriter(test_block_size);
                      writer(3);
                      writer(4);
                      writer(5);
                      writer(6);
                  }

                  // scatter File contents via stream.
                  data::StreamId id = multiplexer.AllocateCatStreamId(0);
                  auto ch = multiplexer.GetOrCreateCatStream(id, 0);
                  ch->Scatter<int>(file, { 0, 2, 4 });

                  // check that got items
                  auto res = ch->OpenCatReader(true).ReadComplete<int>();
                  ASSERT_EQ(res, (std::vector<int>{ 3, 4 }));
              };
    auto w2 = [](data::Multiplexer& multiplexer) {
                  // empty File :...(
                  data::File file(multiplexer.block_pool());

                  // scatter File contents via stream.
                  data::StreamId id = multiplexer.AllocateCatStreamId(0);
                  auto ch = multiplexer.GetOrCreateCatStream(id, 0);
                  ch->Scatter<int>(file, { 0, 0, 0 });

                  // check that got items
                  auto res = ch->OpenCatReader(true).ReadComplete<int>();
                  ASSERT_EQ(res, (std::vector<int>{ 5, 6 }));
              };
    Execute(w0, w1, w2);
}

/******************************************************************************/
