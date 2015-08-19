/*******************************************************************************
 * tests/data/block_sink_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/block_queue.hpp>
#include <c7a/data/block_sink.hpp>
#include <gtest/gtest.h>

#include <string>

using namespace c7a;


struct BlockQueue : public::testing::Test {
    BlockQueue() : sink(destination) { }

    data::BlockQueue destination;
    data::ForwardingBlockSink sink;
};

TEST_F(BlockQueue, DefaultConstructedIsClosedAfterOneClose) {
    ASSERT_FALSE(destination.write_closed());
    sink.Close();
    ASSERT_TRUE(destination.write_closed());
}

TEST_F(BlockQueue, ClosedAfterExpectedNumberCloseOps) {
    data::ForwardingBlockSink sink(destination, 3);
    ASSERT_FALSE(destination.write_closed());
    sink.Close();
    ASSERT_FALSE(destination.write_closed());
    sink.Close();
    ASSERT_FALSE(destination.write_closed());
    sink.Close();
    ASSERT_TRUE(destination.write_closed());
}

/******************************************************************************/
