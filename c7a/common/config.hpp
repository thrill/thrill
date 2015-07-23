/*******************************************************************************
 * c7a/common/config.hpp
 *
 * Global configuration flags.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_CONFIG_HEADER
#define C7A_COMMON_CONFIG_HEADER

namespace c7a {
namespace common {

//! global flag to enable code parts doing self-verification. Later this may be
//! set false if NDEBUG is set in production mode.
static const bool g_self_verify = true;

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_CONFIG_HEADER

/******************************************************************************/
