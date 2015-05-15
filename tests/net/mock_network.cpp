/*******************************************************************************
 * tests/net/mock-network.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include "gtest/gtest.h"
#include "c7a/net/mock_network.hpp"

//using namespace c7a;

TEST(TestMockNetwork, TestMockNetwork)
{
    c7a::net::MockNetwork net;
    c7a::net::MockSelect client1(net, 1), client10(net, 10);

    client1.sendToWorkerString(10, "hello this is client 1 -> 10");
    client10.sendToWorkerString(1, "hello this is client 10 -> 1");

    size_t out_sender;
    std::string out_data;

    client10.receiveFromAnyString(&out_sender, &out_data);
    ASSERT_EQ(out_sender, 1);
    ASSERT_EQ(out_data, "hello this is client 1 -> 10");

    client1.receiveFromAnyString(&out_sender, &out_data);
    ASSERT_EQ(out_sender, 10);
    ASSERT_EQ(out_data, "hello this is client 10 -> 1");
}

/******************************************************************************/
