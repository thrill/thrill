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

using namespace c7a;

TEST(ChannelMultiplexerTest, Test) {

    net::Group::ExecuteLocalMock(
        2, [](net::Group* net) {
            net::DispatcherThread dispatcher("cmp" + std::to_string(net->MyRank()));

            data::ChannelMultiplexer cmp(dispatcher);
            cmp.Connect(net);

            data::ChannelId id = cmp.AllocateNext();

            auto emit = cmp.OpenWriters(id);

            emit[0]("hello I am " + std::to_string(net->MyRank()) + " calling 0");
            emit[1]("hello I am " + std::to_string(net->MyRank()) + " calling 1");

            cmp.Close();
        });
}

#if MAYBE_FIXUP_LATER

using::testing::_;
using::testing::InvokeArgument;
using::testing::Return;
using::testing::ByRef;

using namespace c7a::net;

struct NetDispatcherMock : public NetDispatcher {
    MOCK_METHOD3(AsyncRead, void(NetConnection &, size_t, NetDispatcher::AsyncReadCallback));
};

struct ChannelMultiplexerTest : public::testing::Test {
    ChannelMultiplexerTest()
        : dispatch_mock(),
          candidate(dispatch_mock, 1),
          element1("foo"),
          element2("bar22"),
          element3("."),
          boundaries{element1.length(), element2.length(), element3.length()},
          boundaries2{element2.length(), element1.length(), element3.length()},
          boundaries3{} {
        header.num_elements = 3;
        header.channel_id = 3;
        header.boundaries = boundaries;

        header2.num_elements = 3;
        header2.channel_id = 3;
        header2.boundaries = boundaries2;

        header3.num_elements = 0;
        header3.channel_id = 3;
        header3.boundaries = boundaries3;
        serializeHeaders();
    }

    //tests can call this if they need to change the headers and re-genenerate
    void serializeHeaders() {
        std::string data = header.Serialize();
        header_part1 = data.substr(0, sizeof(size_t) * 2);
        header_part2 = data.substr(header_part1.length(), data.length() - header_part1.length());

        data = header2.Serialize();
        header2_part1 = data.substr(0, sizeof(size_t) * 2);
        header2_part2 = data.substr(header2_part1.length(), data.length() - header2_part1.length());
        data = header3.Serialize();

        header3_part1 = data.substr(0, sizeof(size_t) * 2);
        header3_part2 = data.substr(header3_part1.length(), data.length() - header3_part1.length());
    }

    struct StreamBlockHeader header;
    struct StreamBlockHeader header2;
    struct StreamBlockHeader header3;
    NetConnection            socket;
    NetDispatcherMock        dispatch_mock;
    ChannelMultiplexer       candidate;
    std::string              header_part1;
    std::string              header_part2;
    std::string              header2_part1;
    std::string              header2_part2;
    std::string              header3_part1;
    std::string              header3_part2;
    std::string              element1;
    std::string              element2;
    std::string              element3;
    size_t                   boundaries[3];
    size_t                   boundaries2[3];
    size_t                   boundaries3[0];
};

ACTION_P(SetBufferTo, value) {
    *static_cast<int*>(arg0) = value;
}

TEST_F(ChannelMultiplexerTest, AddSocketIssuesReadForTwoNumberFields) {
    size_t exp_size = sizeof(size_t) * 2;
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, exp_size, _));
    candidate.AddSocket(socket);
}

TEST_F(ChannelMultiplexerTest, ReadsElementBoundariesAfterStreamHead) {
    size_t exp_size1 = sizeof(size_t) * 2;
    size_t exp_size2 = sizeof(size_t) * 3;
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, exp_size1, _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part1));
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, exp_size2, _));
    candidate.AddSocket(socket);
}

TEST_F(ChannelMultiplexerTest, ReadsNoElementBoundariesIfStreamIsEmpty) {
    struct StreamBlockHeader header; //channel_id, num_elements
    header.num_elements = 0;
    header.channel_id = 2;
    std::string data = header.Serialize();
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, _, _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), data));
    candidate.AddSocket(socket);
}

TEST_F(ChannelMultiplexerTest, HasChannel) {
    ASSERT_FALSE(candidate.HasChannel(3));

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, _, _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part1))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part2))
    .WillRepeatedly(Return());

    candidate.AddSocket(socket);

    ASSERT_TRUE(candidate.HasChannel(3));
    ASSERT_FALSE(candidate.HasChannel(2));
}

TEST_F(ChannelMultiplexerTest, ReadsElementsByBoundaries) {
    size_t exp_len1 = header_part1.length();
    size_t exp_len2 = header_part2.length();
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, exp_len1, _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part1))
    .WillOnce(Return());

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, exp_len2, _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part2)); //read boundaries

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, element1.length(), _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), element1));     //read foo

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, element2.length(), _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), element2));     //read bar22

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, element3.length(), _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), element3));     //read .

    candidate.AddSocket(socket);

    auto received_data = candidate.PickupChannel(3)->Data();
    ASSERT_EQ(element1, received_data[0]);
    ASSERT_EQ(element2, received_data[1]);
    ASSERT_EQ(element3, received_data[2]);
}

TEST_F(ChannelMultiplexerTest, ReadsThreeBlocksSingleChannel) {
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, header_part1.length(), _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part1))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header2_part1))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header3_part1));

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, header_part2.length(), _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part2))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header2_part2));
    //no call for header 3 because it has 0 elements

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, element1.length(), _))
    .WillRepeatedly(InvokeArgument<2>(ByRef(socket), element1));     //read foo

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, element2.length(), _))
    .WillRepeatedly(InvokeArgument<2>(ByRef(socket), element2));     //read bar22

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, element3.length(), _))
    .WillRepeatedly(InvokeArgument<2>(ByRef(socket), element3));     //read .

    candidate.AddSocket(socket);

    auto received_data = candidate.PickupChannel(3)->Data();
    ASSERT_EQ(element1, received_data[0]);
    ASSERT_EQ(element2, received_data[1]);
    ASSERT_EQ(element3, received_data[2]);
    ASSERT_EQ(element2, received_data[3]);
    ASSERT_EQ(element1, received_data[4]);
    ASSERT_EQ(element3, received_data[5]);
}

TEST_F(ChannelMultiplexerTest, ReadsThreeBlocksThreeChannel) {
    header.channel_id = 0;
    header2.channel_id = 1;
    header3.channel_id = 2;
    serializeHeaders();

    EXPECT_CALL(dispatch_mock, AsyncRead(socket, _, _))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part1))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header_part2))
    .WillOnce(InvokeArgument<2>(ByRef(socket), element1))     //read foo
    .WillOnce(InvokeArgument<2>(ByRef(socket), element2))     //read bar22
    .WillOnce(InvokeArgument<2>(ByRef(socket), element3))     //read .
    .WillOnce(InvokeArgument<2>(ByRef(socket), header2_part1))
    .WillOnce(InvokeArgument<2>(ByRef(socket), header2_part2))
    .WillOnce(InvokeArgument<2>(ByRef(socket), element2))     //read bar22
    .WillOnce(InvokeArgument<2>(ByRef(socket), element1))     //read foo
    .WillOnce(InvokeArgument<2>(ByRef(socket), element3))     //read .
    .WillOnce(InvokeArgument<2>(ByRef(socket), header3_part1));

    candidate.AddSocket(socket);

    auto received_data0 = candidate.PickupChannel(0)->Data();
    ASSERT_EQ(element1, received_data0[0]);
    ASSERT_EQ(element2, received_data0[1]);
    ASSERT_EQ(element3, received_data0[2]);
    auto received_data1 = candidate.PickupChannel(1)->Data();
    ASSERT_EQ(element2, received_data1[0]);
    ASSERT_EQ(element1, received_data1[1]);
    ASSERT_EQ(element3, received_data1[2]);
    auto received_data2 = candidate.PickupChannel(2)->Data();
    ASSERT_EQ(0, received_data2.size());
}

#endif // MAYBE_FIXUP_LATER

/******************************************************************************/
