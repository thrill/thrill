/*******************************************************************************
 * tests/api/read_write_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/write_lines_one.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/vfs/file_io.hpp>
#include <thrill/vfs/temporary_directory.hpp>

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

TEST(IO, ReadSingleFile) {
    auto start_func =
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

TEST(IO, ReadSingleFileLocalStorageTag) {
    auto start_func =
        [](Context& ctx) {
            auto integers = ReadLines(api::LocalStorageTag, ctx, "inputs/test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            std::vector<int> out_vec = integers.AllGather();

            int i = 1;

            ASSERT_EQ(16u * ctx.num_hosts(), out_vec.size());
            for (int element : out_vec) {
                ASSERT_EQ(element, ((i++ - 1) % 16) + 1);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(IO, ReadFolder) {
    auto start_func =
        [](Context& ctx) {
            ASSERT_EQ(ReadLines(ctx, "inputs/read_folder/*").Size(), 20);
        };

    api::RunLocalTests(start_func);
}

// need all decompressors in folder
#if THRILL_HAVE_ZLIB && THRILL_HAVE_BZIP2

TEST(IO, ReadPartOfFolderCompressed) {
#if defined(_MSC_VER)
    return;
#endif
    auto start_func =
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

#endif // THRILL_HAVE_ZLIB && THRILL_HAVE_BZIP2

TEST(IO, GenerateIntegerWriteReadBinary) {
    vfs::TemporaryDirectory tmpdir;

    api::RunLocalTests(
        [&tmpdir](api::Context& ctx) {

            // wipe directory from last test
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.net.Barrier();

            // generate a dia of integers and write them to disk
            size_t generate_size = 32000;
            {
                auto dia = Generate(
                    ctx, generate_size,
                    [](const size_t index) { return index + 42; });

                dia.WriteBinary(tmpdir.get() + "/IntegerBinary",
                                16 * 1024);
            }
            ctx.net.Barrier();

            // read the integers from disk (collectively) and compare
            {
                auto dia = api::ReadBinary<size_t>(
                    ctx,
                    tmpdir.get() + "/IntegerBinary*");

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

#if THRILL_HAVE_ZLIB

TEST(IO, GenerateIntegerWriteReadBinaryCompressed) {
#if defined(_MSC_VER)
    return;
#endif

    vfs::TemporaryDirectory tmpdir;

    api::RunLocalTests(
        [&tmpdir](api::Context& ctx) {

            // wipe directory from last test
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.net.Barrier();

            // generate a dia of integers and write them to disk
            size_t generate_size = 32000;
            {
                auto dia = Generate(
                    ctx, generate_size,
                    [](const size_t index) { return index + 42; });

                dia.WriteBinary(tmpdir.get() + "/IntegerBinary.gz",
                                16 * 1024);
            }
            ctx.net.Barrier();

            // read the integers from disk (collectively) and compare
            {
                auto dia = api::ReadBinary<size_t>(
                    ctx,
                    tmpdir.get() + "/IntegerBinary*");

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

#endif // THRILL_HAVE_ZLIB

// make weird test strings of different lengths
std::string test_string(size_t index) {
    return std::string((index * index) % 20,
                       static_cast<char>('0' + static_cast<char>(index % 100)));
}

TEST(IO, GenerateStringWriteBinary) {
    vfs::TemporaryDirectory tmpdir;

    // use pairs for easier checking and stranger string sizes.
    using Item = std::pair<size_t, std::string>;

    api::RunLocalTests(
        [&tmpdir](api::Context& ctx) {

            // wipe directory from last test
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.net.Barrier();

            // generate a dia of string Items and write them to disk
            size_t generate_size = 32000;
            {
                auto dia = Generate(
                    ctx, generate_size,
                    [](const size_t index) {
                        return Item(index, test_string(index));
                    });

                dia.WriteBinary(tmpdir.get() + "/StringBinary",
                                16 * 1024);
            }
            ctx.net.Barrier();

            // read the Items from disk (collectively) and compare
            {
                auto dia = api::ReadBinary<Item>(
                    ctx,
                    tmpdir.get() + "/StringBinary*");

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

TEST(IO, WriteAndReadBinaryEqualDIAs) {
    vfs::TemporaryDirectory tmpdir;

    auto start_func =
        [&tmpdir](Context& ctx) {
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.net.Barrier();

            auto integers = ReadLines(ctx, "inputs/test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            ASSERT_EQ(16u, integers.Size());

            integers.WriteBinary(tmpdir.get() + "/output_");

            std::string path = "outputs/testsf.out";

            ctx.net.Barrier();

            auto integers2 = api::ReadBinary<int>(
                ctx, tmpdir.get() + "/*");

            ASSERT_EQ(16u, integers2.Size());

            integers2.Map(
                [](const int& item) {
                    return std::to_string(item);
                })
            .WriteLinesOne(path);

            // Race condition as one worker might be finished while others are
            // still writing to output file.
            ctx.net.Barrier();

            std::ifstream file(path);
            size_t begin = file.tellg();
            file.seekg(0, std::ios::end);
            size_t end = file.tellg();
            ASSERT_EQ(end - begin, 39);
            file.seekg(0);
            for (int i = 1; i <= 16; i++) {
                std::string line;
                std::getline(file, line);
                ASSERT_EQ(std::to_string(i), line);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(IO, WriteAndReadBinaryEqualDIAsLocalStorage) {
    vfs::TemporaryDirectory tmpdir;

    auto start_func =
        [&tmpdir](Context& ctx) {
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.net.Barrier();

            auto integers = ReadLines(ctx, "inputs/test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            auto out_vec = integers.AllGather();

            ASSERT_EQ(16u, integers.Size());

            integers.WriteBinary(tmpdir.get() + "/output_");

            std::string path = "outputs/testsf.out";

            ctx.net.Barrier();

            auto integers2 = api::ReadBinary<int>(
                api::LocalStorageTag, ctx, tmpdir.get() + "/*");

            auto out_vec2 = integers2.AllGather();

            ASSERT_EQ(16u * ctx.num_hosts(), integers2.Size());

            size_t i = 0;
            for (int element : out_vec2) {
                ASSERT_EQ(element, out_vec[i++ % 16]);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(IO, IntegerWriteReadBinaryLinesFutures) {
    vfs::TemporaryDirectory tmpdir;

    api::RunLocalTests(
        [&tmpdir](api::Context& ctx) {

            // wipe directory from last test
            if (ctx.my_rank() == 0) {
                tmpdir.wipe();
            }
            ctx.net.Barrier();

            // generate a dia of integers and write them to disk
            size_t generate_size = 32000;
            {
                auto dia = Generate(
                    ctx,
                    generate_size,
                    [](const size_t index) { return index + 42; });

                Future<> fa = dia.WriteBinaryFuture(
                    tmpdir.get() + "/IntegerBinary", 16 * 1024);

                Future<> fb =
                    dia
                    .Map([](const size_t& i) { return std::to_string(i); })
                    .WriteLinesOneFuture(tmpdir.get() + "/IntegerLines");

                fa.wait();
                fb.wait();
            }
            ctx.net.Barrier();

            // read the binary integers from disk (collectively) and compare
            {
                auto dia = api::ReadBinary<size_t>(
                    ctx, tmpdir.get() + "/IntegerBinary*");

                std::vector<size_t> vec = dia.AllGather();

                ASSERT_EQ(generate_size, vec.size());
                // this is another action
                ASSERT_EQ(generate_size, dia.Size());

                for (size_t i = 0; i < vec.size(); ++i) {
                    ASSERT_EQ(42 + i, vec[i]);
                }
            }

            // read the text integers from disk (collectively) and compare
            {
                auto dia = api::ReadLines(
                    ctx, tmpdir.get() + "/IntegerLines*");

                std::vector<std::string> vec = dia.AllGather();

                ASSERT_EQ(generate_size, vec.size());
                // this is another action
                ASSERT_EQ(generate_size, dia.Size());

                for (size_t i = 0; i < vec.size(); ++i) {
                    ASSERT_EQ(std::to_string(42 + i), vec[i]);
                }
            }
        });
}

/******************************************************************************/
