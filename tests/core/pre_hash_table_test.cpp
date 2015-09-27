/*******************************************************************************
 * tests/core/pre_hash_table_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/core/reduce_pre_table.hpp>
#include <thrill/data/file.hpp>

#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;

using IntPair = std::pair<int, int>;
using StringPairPair = std::pair<std::string, std::pair<std::string, int> >;
using StringPair = std::pair<std::string, int>;

struct PreTable : public::testing::Test { };

struct MyStruct
{
    size_t key;
    int    count;
};

using MyPair = std::pair<int, MyStruct>;

/******************************************************************************/
