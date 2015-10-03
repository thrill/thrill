/*******************************************************************************
 * thrill/common/defines.hpp
 *
 * Define macros.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
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

// detect ThreadSanitizer
#ifndef THRILL_HAVE_THREAD_SANITIZER

#if defined(__has_feature)

// this works with clang
#if __has_feature(thread_sanitizer)
#define THRILL_HAVE_THREAD_SANITIZER 1
#else
#define THRILL_HAVE_THREAD_SANITIZER 0
#endif

#else

// gcc's sanitizers cannot be detected?
#define THRILL_HAVE_THREAD_SANITIZER 0

#endif

#endif // THRILL_HAVE_THREAD_SANITIZER

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_DEFINES_HEADER

/******************************************************************************/
