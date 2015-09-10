/*******************************************************************************
 * thrill/common/string.hpp
 *
 * Some string helper functions
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STRING_HEADER
#define THRILL_COMMON_STRING_HEADER

#include <cstdarg>
#include <random>
#include <string>
#include <vector>

namespace thrill {
namespace common {

/**
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \param size  length of binary data
 * \return      string of hexadecimal pairs
 */
std::string hexdump(const void* const data, size_t size);

/**
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param str  binary data to output in hex
 * \return     string of hexadecimal pairs
 */
std::string hexdump(const std::string& str);

/**
 * Checks if the given match string is located at the start of this string.
 */
static inline
bool starts_with(const std::string& str, const std::string& match) {
    if (match.size() > str.size()) return false;
    return std::equal(match.begin(), match.end(), str.begin());
}

/**
 * Checks if the given match string is located at the end of this string.
 */
static inline
bool ends_with(const std::string& str, const std::string& match) {
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
__attribute__ ((format(printf, 2, 3)));

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

/**
 * Split the given string at each separator character into distinct
 * substrings. Multiple consecutive separators are considered individually and
 * will result in empty split substrings.
 *
 * \param str    string to split
 * \param sep    separator character
 * \param limit  maximum number of parts returned
 * \return       vector containing each split substring
 */
static inline
std::vector<std::string> split(
    const std::string& str, char sep,
    std::string::size_type limit = std::string::npos) {

    std::vector<std::string> out;
    if (limit == 0) return out;

    std::string::const_iterator it = str.begin(), last = it;

    for ( ; it != str.end(); ++it)
    {
        if (*it == sep)
        {
            if (out.size() + 1 >= limit)
            {
                out.push_back(std::string(last, str.end()));
                return out;
            }

            out.push_back(std::string(last, it));
            last = it + 1;
        }
    }

    out.push_back(std::string(last, it));
    return out;
}

/**
 * Split the given string at each separator string into distinct
 * substrings. Multiple consecutive separators are considered individually and
 * will result in empty split substrings.
 *
 * \param str     string to split
 * \param sepstr  separator string, NOT a set of characters!
 * \param limit   maximum number of parts returned
 * \return        vector containing each split substring
 */
static inline
std::vector<std::string> split(
    const std::string& str, const std::string& sepstr,
    std::string::size_type limit = std::string::npos) {

    std::vector<std::string> out;
    if (limit == 0) return out;
    if (sepstr.empty()) return out;

    std::string::const_iterator it = str.begin(), last = it;

    for ( ; it + sepstr.size() < str.end(); ++it)
    {
        if (std::equal(sepstr.begin(), sepstr.end(), it))
        {
            if (out.size() + 1 >= limit)
            {
                out.push_back(std::string(last, str.end()));
                return out;
            }

            out.push_back(std::string(last, it));
            last = it + sepstr.size();
        }
    }

    out.push_back(std::string(last, str.end()));
    return out;
}

/**
 * Join a sequence of strings by some glue string between each pair from the
 * sequence. The sequence in given as a range between two iterators.
 *
 * \param glue  string to glue
 * \param first the beginning iterator of the range to join
 * \param last  the ending iterator of the range to join
 * \return      string constructed from the range with the glue between them.
 */
template <typename Iterator>
static inline
std::string join(const std::string& glue, Iterator first, Iterator last) {
    std::string out;
    if (first == last) return out;

    out.append(*first);
    ++first;

    while (first != last)
    {
        out.append(glue);
        out.append(*first);
        ++first;
    }

    return out;
}

/**
 * Join a Container (like a vector) of strings using some glue string between
 * each pair from the sequence.
 *
 * \param glue  string to glue
 * \param parts the vector of strings to join
 * \return      string constructed from the vector with the glue between them.
 */
template <typename Container>
static inline
std::string join(const std::string& glue, const Container& parts) {
    return join(glue, parts.begin(), parts.end());
}

/**
 * Replace all occurrences of needle in str. Each needle will be replaced with
 * instead, if found. The replacement is done in the given string and a
 * reference to the same is returned.
 *
 * \param str           the string to process
 * \param needle        string to search for in str
 * \param instead       replace needle with instead
 * \return              reference to str
 */
static inline
std::string & replace_all(std::string& str, const std::string& needle,
                          const std::string& instead) {
    std::string::size_type lastpos = 0, thispos;

    while ((thispos = str.find(needle, lastpos)) != std::string::npos)
    {
        str.replace(thispos, needle.size(), instead);
        lastpos = thispos + instead.size();
    }
    return str;
}

/**
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
