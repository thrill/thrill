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

#include <thrill/api/context.hpp>
#include <thrill/vfs/file_io.hpp>

#include <string>

namespace thrill {
namespace vfs {

std::shared_ptr<ReadStream> S3OpenReadStream(
    const FileInfo& file, const api::Context& ctx,
    const common::Range& my_range, bool compressed);

std::shared_ptr<WriteStream> S3OpenWriteStream(
    const std::string& path, const api::Context& ctx);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_S3_FILE_HEADER

/******************************************************************************/
