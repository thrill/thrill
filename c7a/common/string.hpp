/*******************************************************************************
 * c7a/common/string.hpp
 *
 * Some string helper functions
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_STRING_HEADER
#define C7A_COMMON_STRING_HEADER

#include <string>
#include <vector>

namespace c7a {
namespace common {

/**
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \param size  length of binary data
 * \return      string of hexadecimal pairs
 */
static inline std::string hexdump(const void* data, size_t size) {
    const unsigned char* cdata = static_cast<const unsigned char*>(data);

    std::string out;
    out.resize(size * 2);

    static const char xdigits[16] = {
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    std::string::iterator oi = out.begin();
    for (const unsigned char* si = cdata; si != cdata + size; ++si)
    {
        *oi++ = xdigits[(*si & 0xF0) >> 4];
        *oi++ = xdigits[(*si & 0x0F)];
    }

    return out;
}

/**
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param str  binary data to output in hex
 * \return     string of hexadecimal pairs
 */
static inline std::string hexdump(const std::string& str) {
    return hexdump(str.data(), str.size());
}

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

/**
 * Split the given string at each separator character into distinct
 * substrings. Multiple consecutive separators are considered individually and
 * will result in empty split substrings.
 *
 * @param str    string to split
 * @param sep    separator character
 * @param limit  maximum number of parts returned
 * @return       vector containing each split substring
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
 * @param str     string to split
 * @param sepstr  separator string, NOT a set of characters!
 * @param limit   maximum number of parts returned
 * @return        vector containing each split substring
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
 * @param glue  string to glue
 * @param first the beginning iterator of the range to join
 * @param last  the ending iterator of the range to join
 * @return      string constructed from the range with the glue between them.
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
 * @param glue  string to glue
 * @param parts the vector of strings to join
 * @return      string constructed from the vector with the glue between them.
 */
template <typename Container>
static inline
std::string join(const std::string& glue, const Container& parts) {
    return join(glue, parts.begin(), parts.end());
}

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_STRING_HEADER

/******************************************************************************/
