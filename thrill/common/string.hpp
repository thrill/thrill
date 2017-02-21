/*******************************************************************************
 * thrill/common/string.hpp
 *
 * Some string helper functions
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STRING_HEADER
#define THRILL_COMMON_STRING_HEADER

#include <tlx/define.hpp>
#include <tlx/unused.hpp>

#include <array>
#include <cstdarg>
#include <cstdlib>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace thrill {
namespace common {

/*!
 * Helper for using sprintf to format into std::string and also to_string
 * converters.
 *
 * \param max_size maximum length of output string, longer ones are truncated.
 * \param fmt printf format and additional parameters
 */
template <typename String = std::string>
String str_snprintf(size_t max_size, const char* fmt, ...)
TLX_ATTRIBUTE_FORMAT_PRINTF(2, 3);

template <typename String>
String str_snprintf(size_t max_size, const char* fmt, ...) {
    // allocate buffer on stack
    char* s = static_cast<char*>(alloca(max_size));

    va_list args;
    va_start(args, fmt);

    const int len = std::vsnprintf(s, max_size, fmt, args);

    va_end(args);

    return String(s, s + len);
}

/*!
 * Helper for using sprintf to format into std::string and also to_string
 * converters.
 *
 * \param fmt printf format and additional parameters
 */
template <typename String = std::string>
String str_sprintf(const char* fmt, ...)
TLX_ATTRIBUTE_FORMAT_PRINTF(1, 2);

template <typename String>
String str_sprintf(const char* fmt, ...) {
    // allocate buffer on stack
    char* s = static_cast<char*>(alloca(256));

    va_list args;
    va_start(args, fmt);

    int len = std::vsnprintf(s, 256, fmt, args); // NOLINT

    if (len >= 256) {
        // try again.
        s = static_cast<char*>(alloca(len + 1));

        len = std::vsnprintf(s, len + 1, fmt, args);
    }

    va_end(args);

    return String(s, s + len);
}

//! Use ostream to output any type as string. You generally DO NOT want to use
//! this, instead create a larger ostringstream.
template <typename Type>
static inline
std::string to_str(const Type& t) {
    std::ostringstream oss;
    oss << t;
    return oss.str();
}

/*!
 * Template transformation function which uses std::istringstream to parse any
 * istreamable type from a std::string. Returns true only if the whole string
 * was parsed.
 */
template <typename Type>
static inline
bool from_str(const std::string& str, Type& outval) {
    std::istringstream is(str);
    is >> outval;
    return is.eof();
}

/******************************************************************************/
//! Number parsing helpers, wraps strto{f,d,ld,l,ul,ll,ull}() via type switch.

template <typename T>
T from_cstr(const char* nptr, char** endptr = nullptr, int base = 10);

/*----------------------------------------------------------------------------*/
// specializations for floating point types

// float
template <>
inline
float from_cstr<float>(const char* nptr, char** endptr, int) {
    return std::strtof(nptr, endptr);
}

// double
template <>
inline
double from_cstr<double>(const char* nptr, char** endptr, int) {
    return std::strtod(nptr, endptr);
}

// long double
template <>
inline
long double from_cstr<long double>(const char* nptr, char** endptr, int) {
    return std::strtold(nptr, endptr);
}

/*----------------------------------------------------------------------------*/
// specializations for integral types

// long
template <>
inline
long from_cstr<long>(const char* nptr, char** endptr, int base) {
    return std::strtol(nptr, endptr, base);
}
// unsigned long
template <>
inline
unsigned long from_cstr<unsigned long>(
    const char* nptr, char** endptr, int base) {
    return std::strtoul(nptr, endptr, base);
}

// long long
template <>
inline
long long from_cstr<long long>(const char* nptr, char** endptr, int base) {
    return std::strtoll(nptr, endptr, base);
}
// unsigned long long
template <>
inline
unsigned long long from_cstr<unsigned long long>(
    const char* nptr, char** endptr, int base) {
    return std::strtoull(nptr, endptr, base);
}

/******************************************************************************/
// Split and Join

//! Logging helper to print arrays as [a1,a2,a3,...]
template <typename T, size_t N>
static std::string VecToStr(const std::array<T, N>& data) {
    std::ostringstream oss;
    oss << '[';
    for (typename std::array<T, N>::const_iterator it = data.begin();
         it != data.end(); ++it)
    {
        if (it != data.begin()) oss << ',';
        oss << *it;
    }
    oss << ']';
    return oss.str();
}

//! Logging helper to print vectors as [a1,a2,a3,...]
template <typename T>
static std::string VecToStr(const std::vector<T>& data) {
    std::ostringstream oss;
    oss << '[';
    for (typename std::vector<T>::const_iterator it = data.begin();
         it != data.end(); ++it)
    {
        if (it != data.begin()) oss << ',';
        oss << *it;
    }
    oss << ']';
    return oss.str();
}

/*!
 * Generate a random string of given length. The set of available
 * bytes/characters is given as the second argument. Each byte is equally
 * probable. Uses the pseudo-random number generator from stdlib; take care to
 * seed it using srand() before calling this function.
 *
 * \param size     length of result
 * \param rng      Random number generator to use, e.g. std::default_random_engine.
 * \param letters  character set to choose from
 * \return         random string of given length
 */
template <typename RandomEngine = std::default_random_engine>
static inline std::string
RandomString(std::string::size_type size, RandomEngine rng,
             const std::string& letters
                 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz") {
    std::string out;
    out.resize(size);

    std::uniform_int_distribution<size_t> distribution(0, letters.size() - 1);

    for (size_t i = 0; i < size; ++i)
        out[i] = letters[distribution(rng)];

    return out;
}

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_STRING_HEADER

/******************************************************************************/
