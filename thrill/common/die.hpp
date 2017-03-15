/*******************************************************************************
 * thrill/common/die.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_DIE_HEADER
#define THRILL_COMMON_DIE_HEADER

#include <thrill/common/logger.hpp>

#include <sstream>
#include <stdexcept>
#include <string>

namespace thrill {
namespace common {

//! Print via logger and abort().
#define die(msg)                                             \
    do {                                                     \
        LOG1 << msg << " @ " << __FILE__ << ':' << __LINE__; \
        abort();                                             \
    } while (false)

//! Check condition X and die miserably if false. Same as assert() except this
//! is also active in Release mode.
#define die_unless(X) \
    do { if (!(X)) die("Assertion \"" #X "\" failed"); } while (false)

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging.
#define die_unequal(X, Y)                               \
    do {                                                \
        auto x = (X);                                   \
        auto y = (Y);                                   \
        if (x != y)                                     \
            die("Inequality: " #X " != " #Y " : "       \
                "\"" << x << "\" != \"" << y << "\"");  \
    } while (false)

//! Check that code throws an Exception
#define die_unless_throws(code, Exception)                        \
    do {                                                          \
        bool t_ = false; try { code; }                            \
        catch (const Exception&) { t_ = true; }                   \
        if (t_) break;                                            \
        die("UNLESS-THROWS: " #code " - NO EXCEPTION " #Exception \
            " @ " __FILE__ ":" << __LINE__);                      \
        abort();                                                  \
    } while (false)

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging. Only active if NDEBUG is not defined.
#ifdef NDEBUG
#define assert_equal(X, Y)
#else
#define assert_equal(X, Y)  die_unequal(X, Y)
#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_DIE_HEADER

/******************************************************************************/
