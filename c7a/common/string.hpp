/*******************************************************************************
 * c7a/common/string.hpp
 *
 * Some string helper functions
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_STRING_HEADER
#define C7A_COMMON_STRING_HEADER

#include <string>

namespace c7a {
/**
 * Dump a (binary) string as a sequence of hexadecimal pairs.
 *
 * \param data  binary data to output in hex
 * \param size  length of binary data
 * \return      string of hexadecimal pairs
 */
static inline std::string hexdump(const void* data, size_t size)
{
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

static inline std::string hexdump(const std::string& str)
{
    return hexdump(str.data(), str.size());
}
} // namespace c7a

#endif // !C7A_COMMON_STRING_HEADER

/******************************************************************************/
