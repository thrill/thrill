/*******************************************************************************
 * tests/data/stream_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/data/stream.hpp>

#include "gtest/gtest.h"

using namespace c7a::data;
using BlockPtr = std::shared_ptr<Block<1> >;
struct TestStream : public::testing::Test { };

TEST_F(TestStream, CloseWorksWithoutObservers) {
    Stream<1> candidate;
    ASSERT_NO_THROW(candidate.Close());
}

TEST_F(TestStream, AppendThrowsWithoutObservers) {
    Stream<1> candidate;
    ASSERT_ANY_THROW(candidate.Append(BlockPtr(), 0, 0, 0));
}

TEST_F(TestStream, AppendCallsObserver) {
    Stream<1> candidate;
    BlockPtr exp_block = std::make_shared<Block<1> >();
    size_t block_used = 1;
    size_t nitems = 2;
    size_t first = 3;
    bool ran = false;

    auto fn = [&](const Stream<1>& stream, const BlockPtr& block, size_t block_used, size_t nitems, size_t first) {
                  ASSERT_EQ(exp_block, block);
                  ASSERT_EQ(1u, block_used);
                  ASSERT_EQ(2u, nitems);
                  ASSERT_EQ(3u, first);
                  ASSERT_EQ(&candidate, &stream);
                  ran = true;
              };

    candidate.OnAppend(fn);
    candidate.Append(exp_block, block_used, nitems, first);
    ASSERT_TRUE(ran);
}

TEST_F(TestStream, CloseCallsObserver) {
    Stream<1> candidate;
    bool ran = false;
    auto fn = [&](const Stream<1>& stream) {
                  ran = true;
                  ASSERT_EQ(&candidate, &stream);
              };
    candidate.OnClose(fn);
    candidate.Close();
    ASSERT_TRUE(ran);
}

/******************************************************************************/
