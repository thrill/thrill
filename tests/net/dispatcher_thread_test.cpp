/*******************************************************************************
 * tests/net/dispatcher_thread_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/future.hpp>
#include <thrill/common/thread_pool.hpp>
#include <thrill/net/dispatcher_thread.hpp>
#include <thrill/net/tcp/connection.hpp>

#include <future>
#include <tuple>
#include <utility>

using namespace std::literals;
using namespace thrill::net;
using thrill::common::ThreadPool;
using thrill::common::Future;
using thrill::common::FutureX;

struct DispatcherThreadTest : public::testing::Test {
    DispatcherThreadTest() {
        thrill::common::NameThisThread("test-driver");
    }
};

TEST_F(DispatcherThreadTest, LaunchAndTerminate) {
    DispatcherThread disp("dispatcher");
    // sleep for a new ticks until the dispatcher thread reaches select().
    std::this_thread::sleep_for(100ns);
}

TEST_F(DispatcherThreadTest, AsyncWriteAndReadIntoFuture) {
    static const bool debug = true;

    ThreadPool pool(2);

    using tcp::Socket;

    std::pair<Socket, Socket> sp = Socket::CreatePair();
    tcp::Connection connA(sp.first), connB(sp.second);

    DispatcherThread disp("dispatcher");

    pool.Enqueue([&]() {
                     std::this_thread::sleep_for(10ms);
                     disp.AsyncWriteCopy(connA, "Hello");
                     sLOG << "I just sent Hello.";
                 });

    pool.Enqueue([&]() {
                     Future<Buffer> f;
                     disp.AsyncRead(connB, 5,
                                    [&f](Connection&, Buffer&& b) -> void {
                                        sLOG << "Got Hello in callback";
                                        f.Callback(std::move(b));
                                    });
                     Buffer b = f.Wait();
                     sLOG << "Waiter got packet:" << b.ToString();
                 });

    pool.LoopUntilEmpty();
}

TEST_F(DispatcherThreadTest, AsyncWriteAndReadIntoFutureX) {
    static const bool debug = true;

    ThreadPool pool(2);

    using tcp::Socket;

    std::pair<Socket, Socket> sp = Socket::CreatePair();
    tcp::Connection connA(sp.first), connB(sp.second);

    DispatcherThread disp("dispatcher");

    pool.Enqueue(
        [&]() {
            std::this_thread::sleep_for(10ms);
            disp.AsyncWriteCopy(connA, "Hello");
            sLOG << "I just sent Hello.";
        });

    pool.Enqueue(
        [&]() {
            FutureX<int, Buffer> f;
            disp.AsyncRead(connB, 5,
                           [&f](Connection& c, Buffer&& b) -> void {
                               sLOG << "Got Hello in callback";
                               f.Callback(42, std::move(b));
                           });
            std::tuple<int, Buffer> t = f.Wait();
            Buffer& b = std::get<1>(t);
            sLOG << "Waiter got packet:" << b.ToString();
        });

    pool.LoopUntilEmpty();
}

// this test produces a data race condition, which is probably a problem of
// std::future
TEST_F(DispatcherThreadTest, DISABLED_AsyncWriteAndReadIntoStdFuture) {
    static const bool debug = true;

    ThreadPool pool(2);

    using tcp::Socket;

    std::pair<Socket, Socket> sp = Socket::CreatePair();
    tcp::Connection connA(sp.first), connB(sp.second);

    DispatcherThread disp("dispatcher");

    pool.Enqueue(
        [&]() {
            std::this_thread::sleep_for(10ms);
            disp.AsyncWriteCopy(connA, "Hello");
            sLOG << "I just sent Hello.";
        });

    pool.Enqueue(
        [&]() {
            std::promise<Buffer> promise;
            disp.AsyncRead(connB, 5,
                           [&promise](Connection&, Buffer&& b) -> void {
                               sLOG << "Got Hello in callback";
                               promise.set_value(std::move(b));
                           });
            std::future<Buffer> f = promise.get_future();
            f.wait();
            Buffer b = f.get();
            sLOG << "Waiter got packet:" << b.ToString();
        });

    pool.LoopUntilEmpty();
}

/******************************************************************************/
