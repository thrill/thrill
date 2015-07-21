/*******************************************************************************
 * tests/data/channel_multiplexer_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm  <tobias.sturm@student.kit.edu>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/cyclic_barrier.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/channel_multiplexer.hpp>
#include <c7a/data/manager.hpp>
#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/net/group.hpp>
#include <gtest/gtest.h>

#include <string>

using namespace c7a;

static const bool debug = false;

struct ChannelMultiplexer : public::testing::Test {

    using WorkerThread = std::function<void(data::Manager&)>;

    static void FunctionSelect(
        net::Group* group, WorkerThread f1, WorkerThread f2, WorkerThread f3) {
        net::DispatcherThread dispatcher("dp");
        data::Manager manager(dispatcher);
        manager.Connect(group);
        switch (group->MyRank()) {
        case 0:
            common::GetThreadDirectory().NameThisThread("t0");
            if (f1) f1(manager);
            break;
        case 1:
            common::GetThreadDirectory().NameThisThread("t1");
            if (f2) f2(manager);
            break;
        case 2:
            common::GetThreadDirectory().NameThisThread("t2");
            if (f3) f3(manager);
            break;
        }
    }

    static void Execute(WorkerThread f1 = nullptr,
                        WorkerThread f2 = nullptr,
                        WorkerThread f3 = nullptr) {
        net::Group::ExecuteLocalMock(
            // calculate number of threads
            (f1 ? 1 : 0) + (f2 ? 1 : 0) + (f3 ? 1 : 0),
            [=](net::Group* g) {
                FunctionSelect(g, f1, f2, f3);
            });
    }
};

// open a Channel via data::Manager, and send a short message to all workers,
// receive and check the message.
void TalkAllToAllViaChannel(net::Group* net) {
    common::GetThreadDirectory().NameThisThread(
        "chmp" + std::to_string(net->MyRank()));

    net::DispatcherThread dispatcher(
        "chmp" + std::to_string(net->MyRank()) + "-dp");

    unsigned char send_buffer[123];
    for (size_t i = 0; i != sizeof(send_buffer); ++i)
        send_buffer[i] = i;

    static const size_t iterations = 1000;

    data::ChannelMultiplexer<1024> cmp(dispatcher);
    cmp.Connect(net);
    {
        data::ChannelId id = cmp.AllocateNext();

        // open Writers and send a message to all workers

        auto writer = cmp.GetOrCreateChannel(id)->OpenWriters();

        for (size_t tgt = 0; tgt != net->Size(); ++tgt) {
            writer[tgt]("hello I am " + std::to_string(net->MyRank())
                        + " calling " + std::to_string(tgt));

            writer[tgt].Flush();

            // write a few MiBs of oddly sized data
            for (size_t r = 0; r != iterations; ++r) {
                writer[tgt].Append(send_buffer, sizeof(send_buffer));
            }

            writer[tgt].Flush();
        }

        // open Readers and receive message from all workers

        auto reader = cmp.GetOrCreateChannel(id)->OpenReaders();

        for (size_t src = 0; src != net->Size(); ++src) {
            std::string msg = reader[src].Next<std::string>();

            ASSERT_EQ(msg, "hello I am " + std::to_string(src)
                      + " calling " + std::to_string(net->MyRank()));

            sLOG << net->MyRank() << "got msg from" << src;

            // read a few MiBs of oddly sized data
            unsigned char recv_buffer[sizeof(send_buffer)];

            for (size_t r = 0; r != iterations; ++r) {
                reader[src].Read(recv_buffer, sizeof(recv_buffer));

                ASSERT_TRUE(std::equal(send_buffer,
                                       send_buffer + sizeof(send_buffer),
                                       recv_buffer));
            }
        }
    }
}

TEST_F(ChannelMultiplexer, TalkAllToAllViaChannelForManyNetSizes) {
    // test for all network mesh sizes 1, 2, 5, 16:
    net::Group::ExecuteLocalMock(1, TalkAllToAllViaChannel);
    net::Group::ExecuteLocalMock(2, TalkAllToAllViaChannel);
    net::Group::ExecuteLocalMock(5, TalkAllToAllViaChannel);
    net::Group::ExecuteLocalMock(16, TalkAllToAllViaChannel);
}

TEST_F(ChannelMultiplexer, ReadCompleteChannel) {
    auto w0 = [](data::Manager& manager) {
                  auto c = manager.GetNewChannel();
                  auto writers = c->OpenWriters();
                  std::string msg1 = "I came from worker 0";
                  std::string msg2 = "I am another message from worker 0";
                  writers[2](msg1);
                  //writers[2].Flush();
                  writers[2](msg2);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w1 = [](data::Manager& manager) {
                  auto c = manager.GetNewChannel();
                  auto writers = c->OpenWriters();
                  std::string msg1 = "I came from worker 1";
                  writers[2](msg1);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w2 = [](data::Manager& manager) {
                  auto c = manager.GetNewChannel();
                  auto writers = c->OpenWriters();
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }

                  auto reader = c->OpenReader();
                  ASSERT_EQ("I came from worker 0", reader.Next<std::string>());
                  ASSERT_EQ("I am another message from worker 0", reader.Next<std::string>());
                  ASSERT_EQ("I came from worker 1", reader.Next<std::string>());
              };
    Execute(w0, w1, w2);
}

TEST_F(ChannelMultiplexer, Scatter_OneWorker) {
    auto w0 =
        [](data::Manager& manager) {
            // produce a File containing some items
            data::File file;
            {
                auto writer = file.GetWriter();
                writer(std::string("foo"));
                writer(std::string("bar"));
                writer.Flush();
                writer(std::string("breakfast is the most important meal of the day."));
            }

            // scatter File contents via channel: only items [0,3) are sent
            auto ch = manager.GetNewChannel();
            ch->Scatter<std::string>(file, { 2 });

            // check that got items
            auto reader = ch->OpenReader();
            ASSERT_TRUE(reader.HasNext());
            ASSERT_EQ(reader.Next<std::string>(), "foo");
            ASSERT_TRUE(reader.HasNext());
            ASSERT_EQ(reader.Next<std::string>(), "bar");
            ASSERT_FALSE(reader.HasNext());
        };
    Execute(w0);
}

TEST_F(ChannelMultiplexer, Scatter_TwoWorkers_OnlyLocalCopy) {
    auto w0 =
        [](data::Manager& manager) {
            // produce a File containing some items
            data::File file;
            {
                auto writer = file.GetWriter();
                writer(std::string("foo"));
                writer(std::string("bar"));
            }

            // scatter File contents via channel: only items [0,2) are to local worker
            auto ch = manager.GetNewChannel();
            ch->Scatter<std::string>(file, { 2, 2 });

            // check that got items
            auto res = ch->OpenReader().ReadComplete<std::string>();
            ASSERT_EQ(res, (std::vector<std::string>{ "foo", "bar" }));
        };
    auto w1 =
        [](data::Manager& manager) {
            // produce a File containing some items
            data::File file;
            {
                auto writer = file.GetWriter();
                writer(std::string("hello"));
                writer(std::string("world"));
                writer(std::string("."));
            }

            // scatter File contents via channel: only items [0,3) are to local worker
            auto ch = manager.GetNewChannel();
            ch->Scatter<std::string>(file, { 0, 3 });

            // check that got items
            auto res = ch->OpenReader().ReadComplete<std::string>();
            ASSERT_EQ(res, (std::vector<std::string>{ "hello", "world", "." }));
        };
    Execute(w0, w1);
}

TEST_F(ChannelMultiplexer, Scatter_TwoWorkers_CompleteExchange) {
    auto w0 = [](data::Manager& manager) {
                  // produce a File containing some items
                  data::File file;
                  {
                      auto writer = file.GetWriter();
                      writer(std::string("foo"));
                      writer(std::string("bar"));
                  }

                  // scatter File contents via channel.
                  auto ch = manager.GetNewChannel();
                  ch->Scatter<std::string>(file, { 1, 2 });

                  // check that got items
                  auto res = ch->OpenReader().ReadComplete<std::string>();
                  ASSERT_EQ(res, (std::vector<std::string>{ "foo", "hello" }));
              };
    auto w1 = [](data::Manager& manager) {
                  // produce a File containing some items
                  data::File file;
                  {
                      auto writer = file.GetWriter();
                      writer(std::string("hello"));
                      writer(std::string("world"));
                      writer(std::string("."));
                  }

                  // scatter File contents via channel.
                  auto ch = manager.GetNewChannel();
                  ch->Scatter<std::string>(file, { 1, 2 });

                  // check that got items
                  auto res = ch->OpenReader().ReadComplete<std::string>();
                  ASSERT_EQ(res, (std::vector<std::string>{ "bar", "world" }));
              };
    Execute(w0, w1);
}

TEST_F(ChannelMultiplexer, Scatter_ThreeWorkers_PartialExchange) {
    auto w0 = [](data::Manager& manager) {
                  // produce a File containing some items
                  data::File file;
                  {
                      auto writer = file.GetWriter();
                      writer(1);
                      writer(2);
                  }

                  // scatter File contents via channel.
                  auto ch = manager.GetNewChannel();
                  ch->Scatter<int>(file, { 2, 2, 2 });

                  // check that got items
                  auto res = ch->OpenReader().ReadComplete<int>();
                  ASSERT_EQ(res, (std::vector<int>{ 1, 2 }));
              };
    auto w1 = [](data::Manager& manager) {
                  // produce a File containing some items
                  data::File file;
                  {
                      auto writer = file.GetWriter();
                      writer(3);
                      writer(4);
                      writer(5);
                      writer(6);
                  }

                  // scatter File contents via channel.
                  auto ch = manager.GetNewChannel();
                  ch->Scatter<int>(file, { 0, 2, 4 });

                  // check that got items
                  auto res = ch->OpenReader().ReadComplete<int>();
                  ASSERT_EQ(res, (std::vector<int>{ 3, 4 }));
              };
    auto w2 = [](data::Manager& manager) {
                  // empty File :...(
                  data::File file;

                  // scatter File contents via channel.
                  auto ch = manager.GetNewChannel();
                  ch->Scatter<int>(file, { 0, 0, 0 });

                  // check that got items
                  auto res = ch->OpenReader().ReadComplete<int>();
                  ASSERT_EQ(res, (std::vector<int>{ 5, 6 }));
              };
    Execute(w0, w1, w2);
}

/******************************************************************************/
