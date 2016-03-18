/*******************************************************************************
 * thrill/io/create_file.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_CREATE_FILE_HEADER
#define THRILL_IO_CREATE_FILE_HEADER

#include <thrill/io/file_base.hpp>

#include <string>

namespace thrill {
namespace io {

//! create fileio object from io_impl string and a few parameters
FileBasePtr CreateFile(const std::string& io_impl,
                       const std::string& filename,
                       int options,
                       int physical_device_id = FileBase::DEFAULT_QUEUE,
                       int disk_allocator_id = FileBase::NO_ALLOCATOR);

// prototype
class DiskConfig;

//! create fileio object from disk_config parameter
FileBasePtr CreateFile(DiskConfig& config, int mode,
                       int disk_allocator_id = FileBase::NO_ALLOCATOR);

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_CREATE_FILE_HEADER

/******************************************************************************/
