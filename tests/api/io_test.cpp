/*******************************************************************************
 * tests/api/io_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/common/system_exception.hpp>
#include <thrill/thrill.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <random>
#include <string>
#include <vector>

#include <dirent.h>
#include <glob.h>
#include <sys/stat.h>

using namespace thrill;
using thrill::api::Context;
using thrill::api::DIARef;

/*!
 * A class which creates a temporary directory in /tmp/ and returns it via
 * get(). When the object is destroyed the temporary directory is wiped
 * non-recursively.
 */
class TemporaryDirectory
{
public:
    //! Create a temporary directory, returns its name without trailing /.
    static std::string make_directory(
        const char* sample = "/tmp/thrill-testsuite-") {

        std::string tmp_dir = std::string(sample) + "XXXXXX";
        // evil const_cast, but mkdtemp replaces the XXXXXX with something
        // unique. it also mkdirs.
        mkdtemp(const_cast<char*>(tmp_dir.c_str()));

        return tmp_dir;
    }

    //! wipe temporary directory NON RECURSIVELY!
    static void wipe_directory(const std::string& tmp_dir) {
        DIR* d = opendir(tmp_dir.c_str());
        if (d == nullptr) {
            throw common::SystemException(
                      "Could open temporary directory " + tmp_dir, errno);
        }

        struct dirent* de, entry;
        while (readdir_r(d, &entry, &de) == 0 && de != nullptr) {
            // skip ".", "..", and also hidden files (don't create them).
            if (de->d_name[0] == '.') continue;

            std::string path = tmp_dir + "/" + de->d_name;
            int r = unlink(path.c_str());
            if (r != 0)
                sLOG1 << "Could not unlink temporary file " << path
                      << ": " << strerror(errno);
        }

        closedir(d);

        if (rmdir(tmp_dir.c_str()) != 0) {
            sLOG1 << "Could not unlink temporary directory " << tmp_dir
                  << ": " << strerror(errno);
        }
    }

    TemporaryDirectory()
        : dir_(make_directory())
    { }

    ~TemporaryDirectory() {
        wipe_directory(dir_);
    }

    //! non-copyable: delete copy-constructor
    TemporaryDirectory(const TemporaryDirectory&) = delete;
    //! non-copyable: delete assignment operator
    TemporaryDirectory& operator = (const TemporaryDirectory&) = delete;

    //! return the temporary directory name
    const std::string & get() const { return dir_; }

protected:
    std::string dir_;
};

TEST(IO, ReadSingleFile) {
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

TEST(IO, ReadFolder) {
    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            ASSERT_EQ(ReadLines(ctx, "read_folder/*").Size(), 20);
        };

    api::RunLocalTests(start_func);
}

TEST(IO, ReadPartOfFolderCompressed) {
    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            // folder read_ints contains compressed and non-compressed files with integers
            // from 25 to 1 and a file 'donotread', which contains non int-castable
            // strings
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

TEST(IO, GenerateFromFileRandomIntegers) {
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
            .WriteLinesMany("out1_");

            // DIA contains as many elements as we wanted to generate
            ASSERT_EQ(generate_size, writer_size);
        });
}

TEST(IO, GenerateIntegerWriteBinary) {
    api::RunLocalTests(
        [](api::Context& ctx) {

            size_t generate_size = 320000;

            // generate a dia of integers
            auto dia = Generate(
                ctx,
                [](const size_t index) {
                    return index * 42;
                },
                generate_size);

            // write to temporary directory
            TemporaryDirectory tmpdir;

            dia.WriteBinary(tmpdir.get() + "/IO.GenerateIntegerWriteBinary",
                            16 * 1024);
        });
}

TEST(IO, GenerateStringWriteBinary) {
    api::RunLocalTests(
        [](api::Context& ctx) {

            size_t generate_size = 320000;

            auto dia = Generate(
                ctx,
                [](const size_t index) {
                    return std::to_string(index * 42);
                },
                generate_size);

            // write to temporary directory
            TemporaryDirectory tmpdir;

            dia.WriteBinary(tmpdir.get() + "/IO.GenerateStringWriteBinary",
                            16 * 1024);
        });
}

TEST(IO, WriteBinaryCorrectSize) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = ReadLines(ctx, "test1")
                            .Map([](const std::string& line) {
                                     return std::stoi(line);
                                 });

            integers.WriteBinary("binary/output_");

            ctx.Barrier();

            if (ctx.my_rank() == 0) {
                glob_t glob_result;
                struct stat filestat;
                glob("binary/*", GLOB_TILDE, nullptr, &glob_result);
                size_t directory_size = 0;

                for (unsigned int i = 0; i < glob_result.gl_pathc; ++i) {
                    const char* filepath = glob_result.gl_pathv[i];

                    if (stat(filepath, &filestat)) {
                        throw std::runtime_error(
                                  "ERROR: Invalid file " + std::string(filepath));
                    }
                    if (!S_ISREG(filestat.st_mode)) continue;

                    directory_size += filestat.st_size;

                    remove(filepath);
                }
                globfree(&glob_result);

                ASSERT_EQ(16 * sizeof(int), directory_size);
            }
        };

    api::RunLocalTests(start_func);
}

TEST(IO, ReadBinary) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::string path = "testsf.out";

            int bla = 5;
            auto integers2 = ReadBinary(ctx, "./binary" + std::to_string(ctx.num_workers()) + "/*", bla);

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
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
