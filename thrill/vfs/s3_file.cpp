/*******************************************************************************
 * thrill/vfs/s3_file.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/vfs/s3_file.hpp>

#include <thrill/common/porting.hpp>
#include <thrill/common/string.hpp>
#include <thrill/common/system_exception.hpp>

#include <algorithm>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace vfs {

#if THRILL_USE_AWS

#else   // !THRILL_USE_AWS

ReadStreamPtr S3OpenReadStream(
    const std::string& /* path */, const common::Range& /* range */) {
    return nullptr;
}

WriteStreamPtr S3OpenWriteStream(const std::string& /* path */) {
    return nullptr;
}

#endif

} // namespace vfs
} // namespace thrill

/******************************************************************************/
