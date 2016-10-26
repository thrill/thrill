/*******************************************************************************
 * thrill/vfs/s3_file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
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

/******************************************************************************/

void S3Initialize();
void S3Deinitialize();

/******************************************************************************/

void S3Glob(const std::string& path, FileList& filelist);

ReadStreamPtr S3OpenReadStream(
    const std::string& path, const common::Range& range);

WriteStreamPtr S3OpenWriteStream(
    const std::string& path);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_S3_FILE_HEADER

/******************************************************************************/
