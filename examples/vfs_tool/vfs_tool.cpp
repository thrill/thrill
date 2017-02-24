/*******************************************************************************
 * examples/vfs_tool/vfs_tool.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/die.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/vfs/file_io.hpp>
#include <tlx/cmdline_parser.hpp>

#include <iostream>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    clp.set_description("Simple VFS tool for Thrill");

    std::string op;
    clp.add_param_string("op", op, "operation: glob|read|write");

    std::vector<std::string> paths;
    clp.add_param_stringlist("paths", paths, "file path(s)");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    vfs::Initialize();

    if (op == "glob")
    {
        vfs::FileList fl = vfs::Glob(paths);

        if (fl.size() == 0)
            std::cout << "No files returned in glob." << std::endl;

        for (const vfs::FileInfo& fi : fl) {
            std::cout << fi.path
                      << " type " << fi.type
                      << " size " << fi.size
                      << " size_ex_psum " << fi.size_ex_psum
                      << '\n';
        }
    }
    else if (op == "read")
    {
        vfs::FileList fl = vfs::Glob(paths);

        for (const vfs::FileInfo& fi : fl) {
            vfs::ReadStreamPtr rs = vfs::OpenReadStream(fi.path);

            char buffer[1024 * 1024];
            ssize_t rb = 0;
            while ((rb = rs->read(buffer, sizeof(buffer))) > 0) {
                std::cout.write(buffer, rb);
            }
        }
    }
    else if (op == "write")
    {
        die_unless(paths.size() == 1);

        vfs::WriteStreamPtr ws = vfs::OpenWriteStream(paths[0]);

        char buffer[1024 * 1024];
        while (std::cin.read(buffer, sizeof(buffer)), std::cin.gcount()) {
            ws->write(buffer, std::cin.gcount());
        }
    }

    vfs::Deinitialize();

    return 0;
}

/******************************************************************************/
