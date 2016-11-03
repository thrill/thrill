/*******************************************************************************
 * tests/vfs/gzip_filter_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/gzip_filter.hpp>

#include <gtest/gtest.h>
#include <thrill/vfs/sys_file.hpp>
#include <thrill/vfs/temporary_directory.hpp>

#include <string>

using namespace thrill;

TEST(GZipFilterTest, WriteReadSingleFile) {
    vfs::TemporaryDirectory tmpdir;

    {
        vfs::WriteStreamPtr ws = vfs::SysOpenWriteStream(
            tmpdir.get() + "/test.dat.gz");

        vfs::WriteStreamPtr zs = vfs::MakeGZipWriteFilter(ws);

        std::string test_string("test123abc");
        for (size_t i = 0; i < 1000000; ++i) {
            zs->write(test_string.data(), test_string.size());
        }

        for (size_t i = 0; i < 1000000; ++i) {
            zs->write(&i, sizeof(i));
        }

        // put one more byte in
        zs->write(test_string.data(), 1);

        zs->close();
    }
    {
        vfs::ReadStreamPtr rs = vfs::SysOpenReadStream(
            tmpdir.get() + "/test.dat.gz");

        vfs::ReadStreamPtr zs = vfs::MakeGZipReadFilter(rs);

        char buffer[10 + 1];
        for (size_t i = 0; i < 1000000; ++i) {
            zs->read(buffer, 10);
            buffer[10] = 0;
            ASSERT_EQ(std::string(buffer), "test123abc");
        }

        for (size_t i = 0; i < 1000000; ++i) {
            size_t r;
            zs->read(&r, sizeof(r));
            ASSERT_EQ(r, i);
        }

        // read beyond end-of-file
        ssize_t rb = zs->read(buffer, 10);
        ASSERT_EQ(rb, 1);

        zs->close();
    }
}

/******************************************************************************/
