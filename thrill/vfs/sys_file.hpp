/*******************************************************************************
 * thrill/vfs/sys_file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_SYS_FILE_HEADER
#define THRILL_VFS_SYS_FILE_HEADER

#include <thrill/vfs/file_io.hpp>

#include <string>

namespace thrill {
namespace vfs {

/*!
 * Glob a path and augment the FileList with matching file names.
 */
void SysGlob(const std::string& path, const GlobType& gtype,
             FileList& filelist);

/*!
 * Open file for reading and return file descriptor. Handles compressed files by
 * calling a decompressor in a pipe, like "cat $f | gzip -dc |" in bash.
 *
 * \param path Path to open
 *
 * \param range Byte range to read. begin of range is use to seek to, end can be
 * 0 for reading the whole file. Depending on the underlying fs, one can read
 * past end without errors, it is not enforced.
 */
ReadStreamPtr SysOpenReadStream(
    const std::string& path, const common::Range& range = common::Range());

/*!
 * Open file for writing and return file descriptor. Handles compressed files by
 * calling a compressor in a pipe, like "| gzip -d > $f" in bash.
 *
 * \param path Path to open
 */
WriteStreamPtr SysOpenWriteStream(const std::string& path);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_SYS_FILE_HEADER

/******************************************************************************/
