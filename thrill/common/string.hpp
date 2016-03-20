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

#include <cstdarg>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace thrill {
namespace common {

/*!
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \param size  length of binary data
 * \return      string of hexadecimal pairs
 */
std::string Hexdump(const void* const data, size_t size);

//! Dump a (binary) item as a sequence of hexadecimal pairs.
template <typename Type>
std::string HexdumpItem(const Type& t) {
    return Hexdump(&t, sizeof(t));
}

/*!
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param str  binary data to output in hex
 * \return     string of hexadecimal pairs
 */
std::string Hexdump(const std::string& str);

/*!
 * Checks if the given match string is located at the start of this string.
 */
static inline
bool StartsWith(const std::string& str, const std::string& match) {
    if (match.size() > str.size()) return false;
    return std::equal(match.begin(), match.end(), str.begin());
}

/*!
 * Checks if the given match string is located at the end of this string.
 */
static inline
bool EndsWith(const std::string& str, const std::string& match) {
    if (match.size() > str.size()) return false;
    return std::equal(match.begin(), match.end(),
                      str.end() - match.size());
}

/*!
 * Helper for using sprintf to format into std::string and also to_string
 * converters.
 *
 * \param max_size maximum length of output string, longer ones are truncated.
 * \param fmt printf format and additional parameters
 */
template <typename String = std::string>
String str_snprintf(size_t max_size, const char* fmt, ...)
#if defined(__GNUC__) || defined(__clang__)
__attribute__ ((format(printf, 2, 3))) // NOLINT
#endif
;                                      // NOLINT

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
#if defined(__GNUC__) || defined(__clang__)
__attribute__ ((format(printf, 1, 2))) // NOLINT
#endif
;                                      // NOLINT

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

/*!
 * Split the given string at each separator character into distinct
 * substrings. Multiple consecutive separators are considered individually and
 * will result in empty split substrings.
 *
 * \param str    string to split
 * \param sep    separator character
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
std::vector<std::string> Split(
    const std::string& str, char sep,
    std::string::size_type limit = std::string::npos);

/*!
 * Split the given string at each separator string into distinct
 * substrings. Multiple consecutive separators are considered individually and
 * will result in empty split substrings.
 *
 * \param str     string to split
 * \param sepstr  separator string, NOT a set of characters!
 * \param limit   maximum number of parts returned
 * \return        vector containing each split substring
 */
std::vector<std::string> Split(
    const std::string& str, const std::string& sepstr,
    std::string::size_type limit = std::string::npos);

//! Split a string by given separator string. Returns a vector of strings with
//! at least min_fields and at most limit_fields
std::vector<std::string>
Split(const std::string& str, const std::string& sep,
      unsigned int min_fields,
      unsigned int limit_fields = std::numeric_limits<unsigned int>::max());

/*!
 * Join a sequence of strings by some glue string between each pair from the
 * sequence. The sequence in given as a range between two iterators.
 *
 * \param glue  string to glue
 * \param first the beginning iterator of the range to join
 * \param last  the ending iterator of the range to join
 * \return      string constructed from the range with the glue between them.
 */
template <typename Iterator, typename Glue>
static inline
std::string Join(const Glue& glue, Iterator first, Iterator last) {
    std::ostringstream oss;
    if (first == last) return oss.str();

    oss << *first++;

    while (first != last) {
        oss << glue;
        oss << *first++;
    }

    return oss.str();
}

/*!
 * Join a Container (like a vector) of strings using some glue string between
 * each pair from the sequence.
 *
 * \param glue  string to glue
 * \param parts the vector of strings to join
 * \return      string constructed from the vector with the glue between them.
 */
template <typename Container, typename Glue>
static inline
std::string Join(const Glue& glue, const Container& parts) {
    return Join(glue, parts.begin(), parts.end());
}

/*!
 * Replace all occurrences of needle in str. Each needle will be replaced with
 * instead, if found. The replacement is done in the given string and a
 * reference to the same is returned.
 *
 * \param str           the string to process
 * \param needle        string to search for in str
 * \param instead       replace needle with instead
 * \return              reference to str
 */
std::string& ReplaceAll(std::string& str, const std::string& needle,
                        const std::string& instead);

/*!
 * Trims the given string in-place on the left and right. Removes all
 * characters in the given drop array, which defaults to " \r\n\t".
 *
 * \param str   string to process
 * \param drop  remove these characters
 * \return      reference to the modified string
 */
std::string& Trim(std::string& str, const std::string& drop = " \r\n\t");

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
