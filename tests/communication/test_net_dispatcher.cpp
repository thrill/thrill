#include "gtest/gtest.h"
#include "c7a/communication/net_dispatcher.hpp"

using namespace c7a::communication;

TEST(NetDispatcher, InitializeAndClose) {
    auto endpoints = {ExecutionEndpoint::ParseEndpoint("127.0.0.1:1234", 0)};
    auto candidate = NetDispatcher(0, endpoints);
    candidate.Initialize();
    candidate.Close();
}
