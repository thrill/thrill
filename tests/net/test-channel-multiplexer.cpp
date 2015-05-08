/*******************************************************************************
 * tests/communication/test_net_dispatcher.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include <c7a/net/channel-multiplexer.hpp>

using::testing::_;
using::testing::InvokeArgument;
using::testing::Return;
using::testing::ByRef;

using namespace c7a::net;
using namespace c7a;

struct NetDispatcherMock : public NetDispatcher {
    MOCK_METHOD3(AsyncRead, void(Socket &, size_t, NetDispatcher::AsyncReadCallback));
};

struct ChannelMultiplexerTest : public::testing::Test {
    ChannelMultiplexerTest() :
        dispatch_mock(),
        candidate(dispatch_mock, 1),
        element1("foo"),
        element2("bar22"),
        element3("."),
        boundaries{element1.length(), element2.length(), element3.length()}
    {
        header.num_elements = 3;
        header.channel_id = 3;
        header.boundaries = boundaries;
        std::string data = header.Serialize();
        header_part1 = data.substr(0, sizeof(size_t) * 2);
        header_part2 = data.substr(header_part1.length(), data.length() - header_part1.length());
    }

    struct StreamBlockHeader header; //channel_id, num_elements
    Socket                   socket;
    NetDispatcherMock        dispatch_mock;
    ChannelMultiplexer       candidate;
    std::string              header_part1;
    std::string              header_part2;
    std::string              element1;
    std::string              element2;
    std::string              element3;
    size_t                   boundaries[3];
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

    size_t exp_len1 = header_part1.length();
    size_t exp_len2 = header_part2.length();
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
