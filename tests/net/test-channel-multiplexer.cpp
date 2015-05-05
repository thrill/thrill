/*******************************************************************************
 * tests/communication/test_net_dispatcher.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
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
    MOCK_METHOD3(AsyncRead, void(Socket&, size_t, NetDispatcher::AsyncReadCallback));
};

struct ChannelMultiplexerTest : public::testing::Test {
    ChannelMultiplexerTest() :
        dispatch_mock(),
        candidate(dispatch_mock, 1) {
    }

    Socket socket;
    NetDispatcherMock dispatch_mock;
    ChannelMultiplexer candidate;
};

ACTION_P(SetBufferTo, value) {
    *static_cast<int*>(arg0) = value;
}

TEST_F(ChannelMultiplexerTest, AddSocketIssuesReadForTwoNumberFields) {
    size_t exp_size = sizeof(size_t) * 2;
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, exp_size, _));
    candidate.AddSocket(socket);
}

TEST_F(ChannelMultiplexerTest, DISABLED_ReadsElementBoundariesAfterStreamHead) {
    size_t exp_size = sizeof(size_t) * 4;
    struct StreamBlockHeader header; //channel_id, num_elements
    header.num_elements = 4;
    header.channel_id = 2;
    std::string data = header.Serialize();
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, _, _)).WillOnce(InvokeArgument<2>(ByRef(socket), data));
    EXPECT_CALL(dispatch_mock, AsyncRead(socket, exp_size, _));
    candidate.AddSocket(socket);
}
