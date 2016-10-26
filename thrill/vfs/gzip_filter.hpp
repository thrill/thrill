/*******************************************************************************
 * thrill/vfs/gzip_filter.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_GZIP_FILTER_HEADER
#define THRILL_VFS_GZIP_FILTER_HEADER

#include <thrill/vfs/file_io.hpp>

#include <string>

namespace thrill {
namespace vfs {

ReadStreamPtr MakeGZipReadFilter(const ReadStreamPtr& stream);

WriteStreamPtr MakeGZipWriteFilter(const WriteStreamPtr& stream);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_GZIP_FILTER_HEADER

/******************************************************************************/
