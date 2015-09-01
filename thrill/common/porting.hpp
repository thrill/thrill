/*******************************************************************************
 * thrill/common/porting.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_PORTING_HEADER
#define THRILL_COMMON_PORTING_HEADER

namespace thrill {
namespace common {

//! create a pair of pipe file descriptors
void make_pipe(int out_pipefds[2]);

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_PORTING_HEADER

/******************************************************************************/
