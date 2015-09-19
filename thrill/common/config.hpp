/*******************************************************************************
 * thrill/common/config.hpp
 *
 * Global configuration flags.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_CONFIG_HEADER
#define THRILL_COMMON_CONFIG_HEADER

namespace thrill {
namespace common {

//! global ndebug flag as a boolean, NDEBUG means no debug in Release mode.
#if NDEBUG
static const bool g_ndebug = true;
#else
static const bool g_ndebug = false;
#endif

//! debug mode is active, if NDEBUG is false.
static const bool g_debug_mode = !g_ndebug;

//! global flag to enable code parts doing self-verification. Later this may be
//! set false if NDEBUG is set in production mode.
static const bool g_self_verify = g_debug_mode;

//! global flag to enable stats.
#if ENABLE_STATS
static const bool g_enable_stats = true;
#else
static const bool g_enable_stats = false;
#endif

#if !defined(_MSC_VER)
#define THRILL_HAVE_NET_TCP 1
#endif

#define THRILL_HAVE_NET_MPI 1

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_CONFIG_HEADER

/******************************************************************************/
