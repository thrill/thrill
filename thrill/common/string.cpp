/*******************************************************************************
 * thrill/common/string.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/common/string.hpp>

#include <string>

namespace thrill {
namespace common {

std::string Hexdump(const void* const data, size_t size) {
    const unsigned char* const cdata
        = static_cast<const unsigned char* const>(data);

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

std::string Hexdump(const std::string& str) {
    return Hexdump(str.data(), str.size());
}

} // namespace common
} // namespace thrill

/******************************************************************************/
