/*******************************************************************************
 * thrill/common/defines.hpp
 *
 * Define macros.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_DEFINES_HEADER
#define THRILL_COMMON_DEFINES_HEADER

namespace thrill {
namespace common {

#if defined(__GNUC__) || defined(__clang__)
#define THRILL_LIKELY(c)   __builtin_expect((c), 1)
#define THRILL_UNLIKELY(c) __builtin_expect((c), 0)
#else
#define THRILL_LIKELY(c)   c
#define THRILL_UNLIKELY(c) c
#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_DEFINES_HEADER

/******************************************************************************/
