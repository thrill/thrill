/*******************************************************************************
 * tests/net/dispatcher_thread_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/dispatcher_thread.hpp>
#include <c7a/common/future.hpp>
#include <c7a/common/thread_pool.hpp>
#include <gtest/gtest.h>

using namespace std::literals;
using namespace c7a::net;
using c7a::common::ThreadPool;
using c7a::common::Future;

TEST(DispatcherThread, Test1) {

    DispatcherThread disp;
    disp.Start();
    std::this_thread::sleep_for(100ns);
    disp.Stop();
}

TEST(DispatcherThread, FutureTest) {
    static const bool debug = true;

    ThreadPool pool(2);

    using lowlevel::Socket;

    std::pair<Socket, Socket> sp = Socket::CreatePair();
    Connection connA(sp.first), connB(sp.second);

    DispatcherThread disp;

    pool.Enqueue([&]() {
                     std::this_thread::sleep_for(10ms);
                     disp.AsyncWriteCopy(connA, "Hello");
                     sLOG << std::this_thread::get_id()
                          << "I just sent Hello.";
                 });

    pool.Enqueue([&]() {
                     Future<Buffer> f;
                     disp.AsyncRead(connB, 5,
                                    [&f](Connection&, Buffer&& b) -> void {
                                        sLOG << std::this_thread::get_id()
                                             << "Got Hello in callback";
                                        f.GetCallback()(std::move(b));
                                    });
                     Buffer b = f.Get();
                     sLOG << std::this_thread::get_id()
                          << "Waiter got packet:" << b.ToString();
                 });

    disp.Start();
    pool.LoopUntilEmpty();
    disp.Stop();
}

/******************************************************************************/
