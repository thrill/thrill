/*******************************************************************************
 * tests/api/context_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/api/dia.hpp>
#include <c7a/api/context.hpp>
#include <c7a/api/function_stack.hpp>
#include <c7a/core/stage_builder.hpp>
#include <tests/c7a_tests.hpp>

#include <string>
#include <vector>

#include <gtest/gtest.h>

using namespace c7a::core;

TEST(API, ContextGetCurrentDirTest) {
    using c7a::Context;

    Context ctx;

    ASSERT_TRUE(ctx.get_current_dir() != "");

    std::cout << ctx.get_current_dir() << std::endl;

    return;
}

/******************************************************************************/
