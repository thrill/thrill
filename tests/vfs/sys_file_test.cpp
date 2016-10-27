/*******************************************************************************
 * tests/vfs/sys_file_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/sys_file.hpp>

#include <gtest/gtest.h>
#include <thrill/vfs/temporary_directory.hpp>

#include <string>

using namespace thrill;

TEST(SysFileTest, WriteBinaryPatternFormatter) {

    std::string str1 = vfs::FillFilePattern("test-@@@@-########", 42, 10);
    ASSERT_EQ("test-0042-00000010", str1);

    std::string str2 = vfs::FillFilePattern("test", 42, 10);
    ASSERT_EQ("test00420000000010", str2);

    std::string str3 = vfs::FillFilePattern("test.gz", 42, 10);
    ASSERT_EQ("test00420000000010.gz", str3);

    std::string str4 = vfs::FillFilePattern("blah.xyz/test", 42, 10);
    ASSERT_EQ("blah.xyz/test00420000000010", str4);
}

TEST(SysFileTest, WriteReadSingleFile) {
    vfs::TemporaryDirectory tmpdir;

    {
        vfs::WriteStreamPtr ws = vfs::SysOpenWriteStream(
            tmpdir.get() + "/test.dat");

        std::string test_string("test123abc");
        ws->write(test_string.data(), test_string.size());

        for (size_t i = 0; i < 100; ++i) {
            ws->write(&i, sizeof(i));
        }
    }
    {
        vfs::ReadStreamPtr rs = vfs::SysOpenReadStream(
            tmpdir.get() + "/test.dat");

        char buffer[10 + 1];
        rs->read(buffer, 10);
        buffer[10] = 0;
        ASSERT_EQ(std::string(buffer), "test123abc");

        for (size_t i = 0; i < 100; ++i) {
            size_t r;
            rs->read(&r, sizeof(r));
            ASSERT_EQ(r, i);
        }
    }
}

/******************************************************************************/
