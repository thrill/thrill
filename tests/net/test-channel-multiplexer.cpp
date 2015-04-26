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

using ::testing::_;
using ::testing::Return;

using namespace c7a::net;
using namespace c7a;

struct MockSocket : public Socket {
    MOCK_METHOD0(GetFileDescriptor, void());
    MOCK_METHOD3(recv_one, ssize_t(void*, size_t, int));
    MOCK_METHOD3(recv, ssize_t(void*, size_t, int));
};

struct ChannelMultiplexerTest : public ::testing::Test {
    MockSocket socket;
    ChannelMultiplexer candidate;
};

TEST_F(ChannelMultiplexerTest, ReadsHeaderIfSocketIsFresh) {
    EXPECT_CALL(socket, recv_one(_, sizeof(BlockHeader), _)).WillOnce(Return(1));
    candidate.Consume(socket);
}

