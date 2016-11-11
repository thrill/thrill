/*******************************************************************************
 * thrill/vfs/hdfs3_file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_HDFS3_FILE_HEADER
#define THRILL_VFS_HDFS3_FILE_HEADER

#include <thrill/vfs/file_io.hpp>

#include <string>

namespace thrill {
namespace vfs {

/******************************************************************************/

void Hdfs3Initialize();
void Hdfs3Deinitialize();

/******************************************************************************/

void Hdfs3Glob(
    const std::string& path, const GlobType& gtype, FileList& filelist);

ReadStreamPtr Hdfs3OpenReadStream(
    const std::string& path, const common::Range& range = common::Range());

WriteStreamPtr Hdfs3OpenWriteStream(
    const std::string& path);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_HDFS3_FILE_HEADER

/******************************************************************************/
