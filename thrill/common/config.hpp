/*******************************************************************************
 * thrill/common/config.hpp
 *
 * Global configuration flags.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_CONFIG_HEADER
#define THRILL_COMMON_CONFIG_HEADER

namespace thrill {
namespace common {

//! global ndebug flag as a boolean, NDEBUG means no debug in Release mode.
#if NDEBUG
static constexpr bool g_ndebug = true;
#else
static constexpr bool g_ndebug = false;
#endif

//! debug mode is active, if NDEBUG is false.
static constexpr bool g_debug_mode = !g_ndebug;

//! global flag to enable code parts doing self-verification. Later this may be
//! set false if NDEBUG is set in production mode.
static constexpr bool g_self_verify = g_debug_mode;

//! global flag to enable stats.
#if ENABLE_STATS
static constexpr bool g_enable_stats = true;
#else
static constexpr bool g_enable_stats = false;
#endif

//! Finding cache line size is hard - we assume 64 byte.
static constexpr unsigned g_cache_line_size = 64;

#if !defined(_MSC_VER)
#define THRILL_HAVE_NET_TCP 1
#endif

#if __linux__
#define THRILL_HAVE_LINUXAIO_FILE 1
#endif

#if defined(_MSC_VER)
#define THRILL_WINDOWS 1
#define THRILL_MSVC 1
#endif

#ifndef THRILL_WINDOWS
#define THRILL_HAVE_MMAP_FILE 1
#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_CONFIG_HEADER

/******************************************************************************/
