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
#include <future>

using namespace std::literals;
using namespace c7a::net;
using c7a::common::ThreadPool;
using c7a::common::Future;
using c7a::common::FutureX;

TEST(DispatcherThread, LaunchAndTerminate) {

    DispatcherThread disp;
    // sleep for a new ticks until the dispatcher thread reaches select().
    std::this_thread::sleep_for(100ns);
}

TEST(DispatcherThread, AsyncWriteAndReadIntoFuture) {
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
                                        f.Callback(std::move(b));
                                    });
                     Buffer b = f.Wait();
                     sLOG << std::this_thread::get_id()
                          << "Waiter got packet:" << b.ToString();
                 });

    pool.LoopUntilEmpty();
}

TEST(DispatcherThread, AsyncWriteAndReadIntoFutureX) {
    static const bool debug = true;

    ThreadPool pool(2);

    using lowlevel::Socket;

    std::pair<Socket, Socket> sp = Socket::CreatePair();
    Connection connA(sp.first), connB(sp.second);

    DispatcherThread disp;

    pool.Enqueue(
        [&]() {
            std::this_thread::sleep_for(10ms);
            disp.AsyncWriteCopy(connA, "Hello");
            sLOG << std::this_thread::get_id()
                 << "I just sent Hello.";
        });

    pool.Enqueue(
        [&]() {
            FutureX<Connection, Buffer> f;
            disp.AsyncRead(connB, 5,
                           [&f](Connection& c, Buffer&& b) -> void {
                               sLOG << std::this_thread::get_id()
                                    << "Got Hello in callback";
                               f.Callback(std::move(c), std::move(b));
                           });
            std::tuple<Connection, Buffer> t = f.Wait();
            Buffer& b = std::get<1>(t);
            sLOG << std::this_thread::get_id()
                 << "Waiter got packet:" << b.ToString();
        });

    pool.LoopUntilEmpty();
}

TEST(DispatcherThread, AsyncWriteAndReadIntoStdFuture) {
    static const bool debug = true;

    ThreadPool pool(2);

    using lowlevel::Socket;

    std::pair<Socket, Socket> sp = Socket::CreatePair();
    Connection connA(sp.first), connB(sp.second);

    DispatcherThread disp;

    pool.Enqueue(
        [&]() {
            std::this_thread::sleep_for(10ms);
            disp.AsyncWriteCopy(connA, "Hello");
            sLOG << std::this_thread::get_id()
                 << "I just sent Hello.";
        });

    pool.Enqueue(
        [&]() {
            std::promise<Buffer> promise;
            std::future<Buffer> f = promise.get_future();
            disp.AsyncRead(connB, 5,
                           [&f, &promise](Connection&, Buffer&& b) -> void {
                               sLOG << std::this_thread::get_id()
                                    << "Got Hello in callback";
                               promise.set_value(std::move(b));
                           });
            f.wait();
            Buffer b = f.get();
            sLOG << std::this_thread::get_id()
                 << "Waiter got packet:" << b.ToString();
        });

    pool.LoopUntilEmpty();
}

/******************************************************************************/
