/*******************************************************************************
 * thrill/vfs/s3_file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_S3_FILE_HEADER
#define THRILL_VFS_S3_FILE_HEADER

#include <thrill/vfs/file_io.hpp>

#include <string>

namespace thrill {
namespace vfs {

ReadStreamPtr S3OpenReadStream(
    const std::string& path, const common::Range& range);

WriteStreamPtr S3OpenWriteStream(
    const std::string& path);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_S3_FILE_HEADER

/******************************************************************************/
