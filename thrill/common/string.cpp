/*******************************************************************************
 * thrill/common/string.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/string.hpp>

#include <string>
#include <vector>

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

std::vector<std::string> Split(
    const std::string& str, char sep, std::string::size_type limit) {

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

std::vector<std::string> Split(
    const std::string& str, const std::string& sepstr,
    std::string::size_type limit) {

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

std::vector<std::string>
Split(const std::string& str, const std::string& sep,
      unsigned int min_fields, unsigned int limit_fields) {
    std::vector<std::string> result;
    if (str.empty()) {
        result.resize(min_fields);
        return result;
    }

    std::string::size_type cur_pos(0), last_pos(0);
    while (1)
    {
        if (result.size() + 1 == limit_fields)
            break;

        cur_pos = str.find(sep, last_pos);
        if (cur_pos == std::string::npos)
            break;

        result.push_back(
            str.substr(last_pos,
                       std::string::size_type(cur_pos - last_pos)));

        last_pos = cur_pos + sep.size();
    }

    std::string sub = str.substr(last_pos);
    result.push_back(sub);

    if (result.size() < min_fields)
        result.resize(min_fields);

    return result;
}

std::string& ReplaceAll(std::string& str, const std::string& needle,
                        const std::string& instead) {
    std::string::size_type lastpos = 0, thispos;

    while ((thispos = str.find(needle, lastpos)) != std::string::npos)
    {
        str.replace(thispos, needle.size(), instead);
        lastpos = thispos + instead.size();
    }
    return str;
}

std::string& Trim(std::string& str, const std::string& drop) {
    std::string::size_type pos = str.find_last_not_of(drop);
    if (pos != std::string::npos) {
        str.erase(pos + 1);
        pos = str.find_first_not_of(drop);
        if (pos != std::string::npos) str.erase(0, pos);
    }
    else
        str.erase(str.begin(), str.end());

    return str;
}

} // namespace common
} // namespace thrill

/******************************************************************************/
