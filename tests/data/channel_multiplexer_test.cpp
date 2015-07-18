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

#include <c7a/data/channel_multiplexer.hpp>
#include <c7a/data/channel.hpp>
#include <c7a/data/manager.hpp>
#include <c7a/net/group.hpp>
#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/common/cyclic_barrier.hpp>

#include <gtest/gtest.h>
#include <string>

using namespace c7a;
using namespace c7a::common;
using namespace c7a::net;
using c7a::net::Group;

static const bool debug = false;

struct ChannelMultiplexerTest : public::testing::Test {

    using WorkerThread = std::function<void(data::Manager&)>;

    void FunctionSelect(
        Group* group, WorkerThread f1, WorkerThread f2, WorkerThread f3) {
        net::DispatcherThread dispatcher("dp");
        data::Manager manager(dispatcher);
        manager.Connect(group);
        switch (group->MyRank()) {
        case 0:
            GetThreadDirectory().NameThisThread("t0");
            f1(manager);
            break;
        case 1:
            GetThreadDirectory().NameThisThread("t1");
            f2(manager);
            break;
        case 2:
            GetThreadDirectory().NameThisThread("t2");
            f3(manager);
            break;
        }
    }

    void Execute(WorkerThread f1, WorkerThread f2, WorkerThread f3) {
        Group::ExecuteLocalMock(3,
                                [=](Group* g) {
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

TEST(ChannelMultiplexer, TalkAllToAllViaChannelForManyNetSizes) {
    // test for all network mesh sizes 1, 2, 5, 16:
    net::Group::ExecuteLocalMock(1, TalkAllToAllViaChannel);
    net::Group::ExecuteLocalMock(2, TalkAllToAllViaChannel);
    net::Group::ExecuteLocalMock(5, TalkAllToAllViaChannel);
    net::Group::ExecuteLocalMock(16, TalkAllToAllViaChannel);
}

TEST_F(ChannelMultiplexerTest, ReadCompleteChannel) {
    Barrier sync(3);
    auto w0 = [&sync](data::Manager& manager) {
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
    auto w1 = [&sync](data::Manager& manager) {
                  auto c = manager.GetNewChannel();
                  auto writers = c->OpenWriters();
                  std::string msg1 = "I came from worker 1";
                  writers[2](msg1);
                  for (auto& w : writers) {
                      sLOG << "close worker";
                      w.Close();
                  }
              };
    auto w2 = [&sync](data::Manager& manager) {
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

/******************************************************************************/
