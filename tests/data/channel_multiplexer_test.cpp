/*******************************************************************************
 * tests/data/channel_multiplexer_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/channel_multiplexer.hpp>
#include <c7a/data/channel.hpp>

#include <gtest/gtest.h>
#include <string>

using namespace c7a;

static const bool debug = false;

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

            // write a few MiBs of oddly sized data
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
    // test for all network mesh sizes 1-8:
    for (size_t i = 1; i <= 8; ++i) {
        net::Group::ExecuteLocalMock(i, TalkAllToAllViaChannel);
    }
}

/******************************************************************************/
