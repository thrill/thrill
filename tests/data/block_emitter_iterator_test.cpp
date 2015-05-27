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
#include <c7a/data/data_manager.hpp>

#include <vector>
#include <string>

#include "gtest/gtest.h"

using namespace c7a::data;
using namespace c7a::net;

struct EmitterIteratorIntegration : public::testing::Test {
    EmitterIteratorIntegration()
        : dispatcher(),
          multiplexer(dispatcher),
          manager(multiplexer),
          id(manager.AllocateDIA()) { }

    //not required, just for the ctor
    NetDispatcher      dispatcher;
    ChannelMultiplexer multiplexer;

    DataManager        manager;
    size_t             id;
};

TEST_F(EmitterIteratorIntegration, EmptyHasNotNext) {
    auto it = manager.GetLocalBlocks<int>(id);
    ASSERT_FALSE(it.HasNext());
}

TEST_F(EmitterIteratorIntegration, EmptyIsNotClosed) {
    auto it = manager.GetLocalBlocks<int>(id);
    ASSERT_FALSE(it.IsClosed());
}

TEST_F(EmitterIteratorIntegration, ClosedIsClosed) {
    auto it = manager.GetLocalBlocks<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    emitt.Close();
    ASSERT_TRUE(it.IsClosed());
}

TEST_F(EmitterIteratorIntegration, OneElementEmitted) {
    auto it = manager.GetLocalBlocks<int>(id);
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
    auto it = manager.GetLocalBlocks<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    emitt(123);
    emitt.Close();
    ASSERT_TRUE(it.HasNext());
    ASSERT_EQ(123, it.Next());
}

TEST_F(EmitterIteratorIntegration, HasNext_ReturnsFalseIfNoDataAvailable) {
    auto it = manager.GetLocalBlocks<int>(id);
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

TEST_F(EmitterIteratorIntegration, HasNext_ReturnsFalseIfIteratorIsClosed) {
    auto it = manager.GetLocalBlocks<int>(id);
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
    auto it = manager.GetLocalBlocks<int>(id);
    auto emitt = manager.GetLocalEmitter<int>(id);
    for (size_t i = 0; i < 8 * 1024 / sizeof(int); i++)
        emitt(i);
    emitt.Flush();

    for (size_t i = 0; i < 8 * 1024 / sizeof(int); i++) {
        ASSERT_TRUE(it.HasNext());
        ASSERT_EQ(i, it.Next());
    }
    ASSERT_FALSE(it.HasNext());
}

/******************************************************************************/
