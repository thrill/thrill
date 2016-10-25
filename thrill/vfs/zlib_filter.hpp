/*******************************************************************************
 * thrill/vfs/zlib_filter.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_VFS_ZLIB_FILTER_HEADER
#define THRILL_VFS_ZLIB_FILTER_HEADER

#include <thrill/vfs/file_io.hpp>

#include <string>

namespace thrill {
namespace vfs {

ReadStreamPtr MakeZLibReadFilter(const ReadStreamPtr& stream);

WriteStreamPtr MakeZLibWriteFilter(const WriteStreamPtr& stream);

} // namespace vfs
} // namespace thrill

#endif // !THRILL_VFS_ZLIB_FILTER_HEADER

/******************************************************************************/
