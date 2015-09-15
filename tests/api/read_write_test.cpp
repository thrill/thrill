/*******************************************************************************
 * tests/api/read_write_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/allgather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/core/file_io.hpp>

#include <sys/stat.h>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <functional>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;
using thrill::api::Context;
using thrill::api::DIARef;

TEST(IO, ReadSingleFile) {
    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            auto integers = ReadLines(ctx, "inputs/test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            std::vector<int> out_vec = integers.AllGather();

            int i = 1;
            for (int element : out_vec) {
                ASSERT_EQ(element, i++);
            }

            ASSERT_EQ(16u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(IO, ReadFolder) {
    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            ASSERT_EQ(ReadLines(ctx, "inputs/read_folder/*").Size(), 20);
        };

    api::RunLocalTests(start_func);
}

TEST(IO, ReadPartOfFolderCompressed) {
#if defined(_MSC_VER)
    return;
#endif
    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            // folder read_ints contains compressed and non-compressed files
            // with integers from 25 to 1 and a file 'donotread', which contains
            // non int-castable strings
            auto integers = ReadLines(ctx, "inputs/read_ints/read*")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            std::vector<int> out_vec = integers.AllGather();

            int i = 25;
            for (int element : out_vec) {
                ASSERT_EQ(element, i--);
            }

            ASSERT_EQ(25u, out_vec.size());
        };

    api::RunLocalTests(start_func);
}

TEST(IO, GenerateFromFileRandomIntegers) {
    api::RunSameThread(
        [](api::Context& ctx) {
            std::default_random_engine generator(std::random_device { } ());
            std::uniform_int_distribution<int> distribution(1000, 10000);

            size_t generate_size = distribution(generator);

            auto input = GenerateFromFile(
                ctx,
                "inputs/test1",
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
            .WriteLinesMany("outputs/generated", 8 * 1024);

            // DIA contains as many elements as we wanted to generate
            ASSERT_EQ(generate_size, writer_size);
        });
}

TEST(IO, WriteBinaryPatternFormatter) {

    std::string str1 = core::FillFilePattern("test-$$$$-########", 42, 10);
    ASSERT_EQ("test-0042-00000010", str1);

    std::string str2 = core::FillFilePattern("test", 42, 10);
    ASSERT_EQ("test00420000000010", str2);
}

TEST(IO, GenerateIntegerWriteReadBinary) {
    core::TemporaryDirectory tmpdir;

    api::RunLocalTests(
        [&tmpdir](api::Context& ctx) {

            // wipe directory from last test
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.Barrier();

            // generate a dia of integers and write them to disk
            size_t generate_size = 32000;
            {
                auto dia = Generate(
                    ctx,
                    [](const size_t index) { return index + 42; },
                    generate_size);

                dia.WriteBinary(tmpdir.get() + "/IO.IntegerBinary",
                                16 * 1024);
            }
            ctx.Barrier();

            // read the integers from disk (collectively) and compare
            {
                auto dia = api::ReadBinary<size_t>(
                    ctx,
                    tmpdir.get() + "/IO.IntegerBinary*");

                std::vector<size_t> vec = dia.AllGather();

                ASSERT_EQ(generate_size, vec.size());
                // this is another action
                ASSERT_EQ(generate_size, dia.Size());

                for (size_t i = 0; i < vec.size(); ++i) {
                    ASSERT_EQ(42 + i, vec[i]);
                }
            }
        });
}

TEST(IO, GenerateIntegerWriteReadBinaryCompressed) {
#if defined(_MSC_VER)
    return;
#endif

    core::TemporaryDirectory tmpdir;

    api::RunLocalTests(
        [&tmpdir](api::Context& ctx) {

            // wipe directory from last test
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.Barrier();

            // generate a dia of integers and write them to disk
            size_t generate_size = 32000;
            {
                auto dia = Generate(
                    ctx,
                    [](const size_t index) { return index + 42; },
                    generate_size);

                dia.WriteBinary(tmpdir.get() + "/IO.IntegerBinary-$$$$-####.gz",
                                16 * 1024);
            }
            ctx.Barrier();

            // read the integers from disk (collectively) and compare
            {
                auto dia = api::ReadBinary<size_t>(
                    ctx,
                    tmpdir.get() + "/IO.IntegerBinary*");

                std::vector<size_t> vec = dia.AllGather();

                ASSERT_EQ(generate_size, vec.size());
                // this is another action
                ASSERT_EQ(generate_size, dia.Size());

                for (size_t i = 0; i < vec.size(); ++i) {
                    ASSERT_EQ(42 + i, vec[i]);
                }
            }
        });
}

// make weird test strings of different lengths
std::string test_string(size_t index) {
    return std::string((index * index) % 20,
                       '0' + static_cast<char>(index) % 100);
}

TEST(IO, GenerateStringWriteBinary) {
    core::TemporaryDirectory tmpdir;

    // use pairs for easier checking and stranger string sizes.
    using Item = std::pair<size_t, std::string>;

    api::RunLocalTests(
        [&tmpdir](api::Context& ctx) {

            // wipe directory from last test
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.Barrier();

            // generate a dia of string Items and write them to disk
            size_t generate_size = 32000;
            {
                auto dia = Generate(
                    ctx,
                    [](const size_t index) {
                        return Item(index, test_string(index));
                    },
                    generate_size);

                dia.WriteBinary(tmpdir.get() + "/IO.StringBinary",
                                16 * 1024);
            }
            ctx.Barrier();

            // read the Items from disk (collectively) and compare
            {
                auto dia = api::ReadBinary<Item>(
                    ctx,
                    tmpdir.get() + "/IO.StringBinary*");

                std::vector<Item> vec = dia.AllGather();

                ASSERT_EQ(generate_size, vec.size());
                // this is another action
                ASSERT_EQ(generate_size, dia.Size());

                for (size_t i = 0; i < vec.size(); ++i) {
                    ASSERT_EQ(Item(i, test_string(i)), vec[i]);
                }
            }
        });
}

TEST(IO, WriteAndReadBinaryEqualDIAS) {
    core::TemporaryDirectory tmpdir;

    std::function<void(Context&)> start_func =
        [&tmpdir](Context& ctx) {
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.Barrier();

            auto integers = ReadLines(ctx, "inputs/test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            ASSERT_EQ(16u, integers.Size());

            integers.WriteBinary(tmpdir.get() + "/output_");

            std::string path = "outputs/testsf.out";

            ctx.Barrier();

            auto integers2 = api::ReadBinary<int>(
                ctx, tmpdir.get() + "/*");

            integers2.Map(
                [](const int& item) {
                    return std::to_string(item);
                })
            .WriteLines(path);

            // Race condition as one worker might be finished while others are
            // still writing to output file.
            ctx.Barrier();

            std::ifstream file(path, std::ios::binary);
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
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
