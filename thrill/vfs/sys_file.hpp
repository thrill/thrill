/*******************************************************************************
 * thrill/vfs/sys_file.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_SYS_FILE_HEADER
#define THRILL_VFS_SYS_FILE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/vfs/file_io.hpp>

#include <string>

namespace thrill {
namespace vfs {

/*!
 * Open file for reading and return file descriptor. Handles compressed files by
 * calling a decompressor in a pipe, like "cat $f | gzip -dc |" in bash.
 *
 * \param path Path to open
 */
std::shared_ptr<AbstractFile> SysOpenReadStream(const std::string& path);

/*!
 * Open file for writing and return file descriptor. Handles compressed files by
 * calling a compressor in a pipe, like "| gzip -d > $f" in bash.
 *
 * \param path Path to open
 */
std::shared_ptr<AbstractFile> SysOpenWriteStream(const std::string& path);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_SYS_FILE_HEADER

/******************************************************************************/
