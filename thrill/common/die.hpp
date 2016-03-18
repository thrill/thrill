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

#include <sstream>
#include <stdexcept>
#include <string>

namespace thrill {
namespace common {

//! Instead of abort(), throw the output the message via an exception.
#define die(msg)                                                \
    do {                                                        \
        std::ostringstream die_oss;                             \
        die_oss << msg << " @ " << __FILE__ << ':' << __LINE__; \
        throw std::runtime_error(die_oss.str());                \
    } while (0)

//! Check condition X and die miserably if false. Same as assert() except this
//! is also active in Release mode.
#define die_unless(X) \
    do { if (!(X)) die("Assertion \"" #X "\" failed"); } while (0)

//! Check that X == Y or die miserably, but output the values of X and Y for
//! better debugging.
#define die_unequal(X, Y)                              \
    do {                                               \
        if ((X) != (Y))                                \
            die("Inequality: " #X " != " #Y " : "      \
                "\"" << X << "\" != \"" << Y << "\""); \
    } while (0)

//! Check that code throws an Exception
#define die_unless_throws(code, Exception)                        \
    do {                                                          \
        bool t_ = false; try { code; }                            \
        catch (const Exception&) { t_ = true; }                   \
        if (t_) break;                                            \
        die("UNLESS-THROWS: " #code " - NO EXCEPTION " #Exception \
            " @ " __FILE__ ":" << __LINE__);                      \
        abort();                                                  \
    } while (0)

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_DIE_HEADER

/******************************************************************************/
