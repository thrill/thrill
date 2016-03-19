/*******************************************************************************
 * tests/io/config_file_test.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/config_file.hpp>

#include <thrill/common/die.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/io/block_manager.hpp>

#include <gtest/gtest.h>

using namespace thrill;

TEST(IO_ConfigFile, Test1) {
    // test disk_config parser:

    io::DiskConfig cfg;

    cfg.parse_line("disk=/var/tmp/thrill.tmp, 100 GiB , syscall unlink direct=on");

    die_unequal(cfg.path, "/var/tmp/thrill.tmp");
    die_unequal(cfg.size, 100 * 1024 * 1024 * uint64_t(1024));
    die_unequal(cfg.fileio_string(), "syscall direct=on unlink_on_open");

    // test disk_config parser:

    cfg.parse_line("disk=/var/tmp/thrill.tmp, 100 , wincall queue=5 delete_on_exit direct=on");

    die_unequal(cfg.path, "/var/tmp/thrill.tmp");
    die_unequal(cfg.size, 100 * 1024 * uint64_t(1024));
    die_unequal(cfg.fileio_string(), "wincall delete_on_exit direct=on queue=5");
    die_unequal(cfg.queue, 5);
    die_unequal(cfg.direct, io::DiskConfig::DIRECT_ON);

    // bad configurations

    die_unless_throws(
        cfg.parse_line("disk=/var/tmp/thrill.tmp, 100 GiB, wincall_fileperblock unlink direct=on"),
        std::runtime_error);

    die_unless_throws(
        cfg.parse_line("disk=/var/tmp/thrill.tmp,0x,syscall"),
        std::runtime_error);
}

#if !THRILL_WINDOWS

TEST(IO_ConfigFile, Test2) {
    // test user-supplied configuration

    io::Config* config = io::Config::get_instance();

    {
        io::DiskConfig disk1("/tmp/thrill-1.tmp", 100 * 1024 * 1024,
                             "syscall");
        disk1.unlink_on_open = true;
        disk1.direct = io::DiskConfig::DIRECT_OFF;

        die_unequal(disk1.path, "/tmp/thrill-1.tmp");
        die_unequal(disk1.size, 100 * 1024 * uint64_t(1024));
        die_unequal(disk1.autogrow, 1);
        die_unequal(disk1.fileio_string(),
                    "syscall direct=off unlink_on_open");

        config->add_disk(disk1);

        io::DiskConfig disk2("/tmp/thrill-2.tmp", 200 * 1024 * 1024,
                             "syscall autogrow=no direct=off");
        disk2.unlink_on_open = true;

        die_unequal(disk2.path, "/tmp/thrill-2.tmp");
        die_unequal(disk2.size, 200 * 1024 * uint64_t(1024));
        die_unequal(disk2.fileio_string(),
                    "syscall autogrow=no direct=off unlink_on_open");
        die_unequal(disk2.direct, 0);

        config->add_disk(disk2);
    }

    die_unequal(config->disks_number(), 2);
    die_unequal(config->total_size(), 300 * 1024 * 1024);

    // construct block_manager with user-supplied config

    io::BlockManager* bm = io::BlockManager::get_instance();

    die_unequal(bm->get_total_bytes(), 300 * 1024 * 1024);
    die_unequal(bm->get_free_bytes(), 300 * 1024 * 1024);
}

#endif

/******************************************************************************/
