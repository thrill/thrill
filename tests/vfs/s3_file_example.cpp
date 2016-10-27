/*******************************************************************************
 * tests/vfs/s3_file_example.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/vfs/file_io.hpp>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <limits>
#include <sstream>
#include <string>

using namespace thrill;

int main(int argc, char* argv[]) {

    vfs::Initialize();

    if (argc >= 2 && strcmp(argv[1], "read") == 0)
    {
        vfs::ReadStreamPtr rs = vfs::OpenReadStream(
            "s3://commoncrawl/crawl-data/CC-MAIN-2016-40/"
            "segments/1474738659496.36/wet/"
            "CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc.wet.gz",
            common::Range(0, 10000));

        char buffer[1024];
        ssize_t rb = 0;
        while ((rb = rs->read(buffer, sizeof(buffer))) > 0) {
            LOG1 << "rb = " << rb;
        }

        rs->close();
    }

    /**************************************************************************/

    if (argc >= 2 && strcmp(argv[1], "write") == 0)
    {
        vfs::WriteStreamPtr ws = vfs::OpenWriteStream(
            "s3://thrill-tpch/hello.txt");

        for (size_t i = 0; i < 1000000; ++i) {
            std::string s = "hello, the world is great.\n";
            ws->write(s.data(), s.size());
        }

        ws->close();
    }

    /**************************************************************************/

    if (argc >= 2 && strcmp(argv[1], "glob") == 0)
    {
        vfs::FileList fl = vfs::Glob(
            "s3://commoncrawl/crawl-data/CC-MAIN-2016-40/"
            "segments/1474738659496.36/wet/");

        for (const vfs::FileInfo& fi : fl)
            sLOG1 << "File:" << fi.path
                  << "size" << fi.size << "size_ex_psum" << fi.size_ex_psum;
    }

    vfs::Deinitialize();

    return 0;
}

/******************************************************************************/
