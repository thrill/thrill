/*******************************************************************************
 * tests/data/block_emitter_iterator_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/block_iterator.hpp>
#include <c7a/data/block_emitter.hpp>
#include <c7a/data/manager.hpp>

#include <vector>
#include <string>

#include "gtest/gtest.h"

using namespace c7a::data;
using namespace c7a::net;

struct EmitterIteratorIntegration : public::testing::Test {
    EmitterIteratorIntegration()
        : dispatcher(),
          manager(dispatcher),
          id(manager.AllocateDIA()) { }

    //not required, just for the ctor
    DispatcherThread dispatcher;

    Manager    manager;
    ChainId    id;
};

TEST_F(EmitterIteratorIntegration, EmptyHasNotNext) {
    auto it = manager.GetIterator<int>(id);
    ASSERT_FALSE(it.HasNext());
}

TEST_F(EmitterIteratorIntegration, EmptyIsNotClosed) {
    auto it = manager.GetIterator<int>(id);
    ASSERT_FALSE(it.IsClosed());
}

TEST_F(EmitterIteratorIntegration, ClosedIsClosed) {
    auto it = manager.GetIterator<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    emitt.Close();
    ASSERT_TRUE(it.IsClosed());
}

TEST_F(EmitterIteratorIntegration, OneElementEmitted) {
    auto it = manager.GetIterator<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    emitt(123);
    emitt.Flush();
    ASSERT_FALSE(it.IsClosed());
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(123, it.Next());

    emitt.Close();
    ASSERT_TRUE(it.IsClosed());
}

TEST_F(EmitterIteratorIntegration, CloseFlushesEmitter) {
    auto it = manager.GetIterator<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    emitt(123);
    emitt.Close();
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(123, it.Next());
}

TEST_F(EmitterIteratorIntegration, HasNext_ReturnsFalseIfNoDataAvailable) {
    auto it = manager.GetIterator<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    emitt(1);
    emitt.Flush();
    emitt(2);
    emitt(3);
    emitt.Flush();
    ASSERT_TRUE(it.HasNext());
    it.Next();
    it.Next();
    it.Next();
    ASSERT_FALSE(it.HasNext());
    emitt(4);
    emitt.Flush();
    ASSERT_TRUE(it.HasNext());
}

TEST_F(EmitterIteratorIntegration, DISABLED_HasNext_ReturnsFalseIfIteratorIsClosed) {
    auto it = manager.GetIterator<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    emitt(1);
    emitt.Flush(); //force second buffer in buffer_chain
    emitt(2);
    emitt(3);
    emitt.Flush(); //finishes the buffer_chain
    ASSERT_TRUE(it.HasNext());
    it.Next();
    it.Next();
    it.Next();
    emitt.Close();
    ASSERT_FALSE(it.HasNext());
}

TEST_F(EmitterIteratorIntegration, EmitAndReadEightKB) {
    auto it = manager.GetIterator<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    for (int i = 0; i < (int)(8 * 1024 / sizeof(int)); i++)
        emitt(i);
    emitt.Flush();

    for (int i = 0; i < (int)(8 * 1024 / sizeof(int)); i++) {
        ASSERT_TRUE(it.HasNext());
        ASSERT_EQ(i, it.Next());
    }
    ASSERT_FALSE(it.HasNext());
}

TEST_F(EmitterIteratorIntegration, WaitForMore_PausesThread) {
    using namespace std::literals;
    auto it = manager.GetIterator<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    int received_elelements = 0;
    int wait_calls = 0;
    std::thread receiver([&it, &wait_calls, &received_elelements]() {
                             while (!it.IsClosed()) {
                                 if (!it.HasNext()) {
                                     wait_calls++;
                                     it.WaitForMore();
                                 }
                                 else {
                                     it.Next();
                                     received_elelements++;
                                 }
                             }
                         });
    std::this_thread::sleep_for(10ms); //let other thread run

    emitt(123);
    emitt.Flush();

    std::this_thread::sleep_for(10ms);
    ASSERT_EQ(1, received_elelements);
    ASSERT_EQ(2, wait_calls);

    emitt.Close();
    receiver.join();
    ASSERT_EQ(2, wait_calls); //should be unchanged
}

TEST_F(EmitterIteratorIntegration, WaitForAll_PausesThread) {
    using namespace std::literals;
    auto it = manager.GetIterator<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    int received_elelements = 0;
    int wait_calls = 0;
    std::thread receiver([&it, &wait_calls, &received_elelements]() {
                             while (!it.IsClosed()) {
                                 wait_calls++;
                                 it.WaitForAll();
                                 while (it.HasNext()) {
                                     it.Next();
                                     received_elelements++;
                                 }
                             }
                         });
    std::this_thread::sleep_for(10ms); //let other thread run

    emitt(123);
    emitt.Flush();

    //should wait once, read nothing
    std::this_thread::sleep_for(10ms);
    ASSERT_EQ(0, received_elelements);
    ASSERT_EQ(1, wait_calls);

    emitt(444);
    emitt(222);
    emitt.Flush();

    //we expect no changes
    std::this_thread::sleep_for(10ms);
    ASSERT_EQ(0, received_elelements);
    ASSERT_EQ(1, wait_calls);

    emitt.Close();
    receiver.join();

    //all elements are accessible / no further wait
    std::this_thread::sleep_for(10ms);
    ASSERT_EQ(3, received_elelements);
    ASSERT_EQ(1, wait_calls);
}

/******************************************************************************/
