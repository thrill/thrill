/*******************************************************************************
 * tests/api/io_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/thrill.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace thrill;
using thrill::api::Context;
using thrill::api::DIARef;

TEST(IO, GenerateFromFileCorrectAmountOfCorrectIntegers) {
    api::RunSameThread(
        [](api::Context& ctx) {
            std::default_random_engine generator({ std::random_device()() });
            std::uniform_int_distribution<int> distribution(1000, 10000);

            size_t generate_size = distribution(generator);

            auto input = GenerateFromFile(
                ctx,
                "test1",
                [](const std::string& line) {
                    return std::stoi(line);
                },
                generate_size);

            size_t writer_size = 0;

            input.Map(
                [&writer_size](const int& item) {
                    // file contains ints between 1  and 16
                    // fails if wrong integer is generated
                    EXPECT_GE(item, 1);
                    EXPECT_GE(16, item);
                    writer_size++;
                    return std::to_string(item) + "\n";
                })
            .WriteBinary("test1.out");

            ASSERT_EQ(generate_size, writer_size);
        });
}

TEST(IO, ReadFolder) {
    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            ASSERT_EQ(ReadLines(ctx, "read_folder/*").Size(), 20);
        };

    api::RunLocalTests(start_func);
}

TEST(IO, WriteToSingleFile) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::string path = "testsf.out";

            auto integers = ReadLines(ctx, "test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            integers.WriteBinary("binary/output_");

            ctx.Barrier();

            int bla = 5;

            auto integers2 = ReadBinary(ctx, "binary/*", bla);

            integers2.Map(
                [](const int& item) {
                    return std::to_string(item);
                })
            .WriteLines(path);

            // Race condition as one worker might be finished while others are
            // still writing to output file.
            ctx.Barrier();

            std::ifstream file(path);
            size_t begin = file.tellg();
            file.seekg(0, std::ios::end);
            size_t end = file.tellg();
            ASSERT_EQ(end - begin, 39);
            file.seekg(0);
            for (int i = 1; i <= 16; i++) {
                std::string line;
                std::getline(file, line);
                ASSERT_EQ(std::stoi(line), i);
            }            
            system("exec rm -r /binary/*");
    };

    api::RunLocalTests(start_func);
}

TEST(IO, ReadAndAllGatherElementsCorrect) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = ReadLines(ctx, "test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            std::vector<int> out_vec = integers.AllGather();

            int i = 1;
            for (int element : out_vec) {
                ASSERT_EQ(element, i++);
            }

            ASSERT_EQ((size_t)16, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(IO, ReadPartOfFolderAndAllGatherElementsCorrect) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = ReadLines(ctx, "read_ints/read*")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            std::vector<int> out_vec = integers.AllGather();

            int i = 25;
            for (int element : out_vec) {
                ASSERT_EQ(element, i--);
            }

            ASSERT_EQ((size_t)25, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
