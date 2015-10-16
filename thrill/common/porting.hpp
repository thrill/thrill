/*******************************************************************************
 * thrill/common/porting.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_PORTING_HEADER
#define THRILL_COMMON_PORTING_HEADER

#if defined(_MSC_VER)
#include <BaseTsd.h>
using ssize_t = SSIZE_T;
#endif

namespace thrill {
namespace common {

//! set FD_CLOEXEC on file descriptor (if possible)
void PortSetCloseOnExec(int fd);

//! create a pair of pipe file descriptors
void MakePipe(int out_pipefds[2]);

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_PORTING_HEADER

/******************************************************************************/
