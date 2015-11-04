/*******************************************************************************
 * thrill/io/file.cpp
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

#include "ufs_platform.hpp"
#include <thrill/io/file.hpp>

namespace thrill {
namespace io {

int file::unlink(const char* path) {
    return ::unlink(path);
}

} // namespace io
} // namespace thrill

/******************************************************************************/
