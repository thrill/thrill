/*******************************************************************************
 * tests/api/read_write_test_aws.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/common/logger.hpp>

using namespace thrill;

TEST(IO, WriteToS3) {
    auto start_func =
        [](Context& ctx) {
            auto integers = Generate(ctx, 240,
                                     [](const size_t& ele) {
                                         return std::to_string(ele);
                                     });

            integers.WriteLines("s3://thrill-test/some_integers");
        };

    api::RunLocalTests(start_func);
}

TEST(IO, ReadFilesFromS3) {
    auto start_func =
        [](Context& ctx) {
            size_t size = ReadLines(ctx, "s3://thrill-data/tbl/customer").Size();
            ASSERT_EQ(size, (size_t)150000);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
