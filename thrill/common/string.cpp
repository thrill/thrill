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

#include <iomanip>
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

/******************************************************************************/

//! Parse a string like "343KB" or " 44 GiB " into the corresponding size in
//! bytes.
bool ParseSiIecUnits(const char* str, uint64_t& size, char default_unit) {
    char* endptr;
    size = strtoul(str, &endptr, 10);
    if (!endptr) return false;                    // parse failed, no number

    while (endptr[0] == ' ') ++endptr;            // skip over spaces

    // multiply with base ^ power
    unsigned int base = 1000;
    unsigned int power = 0;

    if (endptr[0] == 'k' || endptr[0] == 'K')
        power = 1, ++endptr;
    else if (endptr[0] == 'm' || endptr[0] == 'M')
        power = 2, ++endptr;
    else if (endptr[0] == 'g' || endptr[0] == 'G')
        power = 3, ++endptr;
    else if (endptr[0] == 't' || endptr[0] == 'T')
        power = 4, ++endptr;
    else if (endptr[0] == 'p' || endptr[0] == 'P')
        power = 5, ++endptr;

    // switch to power of two (only if power was set above)
    if ((endptr[0] == 'i' || endptr[0] == 'I') && power != 0)
        base = 1024, ++endptr;

    // byte indicator
    if (endptr[0] == 'b' || endptr[0] == 'B') {
        ++endptr;
    }
    else if (power == 0)
    {
        // no explicit power indicator, and no 'b' or 'B' -> apply default unit
        switch (default_unit)
        {
        default: break;
        case 'k': power = 1, base = 1000;
            break;
        case 'm': power = 2, base = 1000;
            break;
        case 'g': power = 3, base = 1000;
            break;
        case 't': power = 4, base = 1000;
            break;
        case 'p': power = 5, base = 1000;
            break;
        case 'K': power = 1, base = 1024;
            break;
        case 'M': power = 2, base = 1024;
            break;
        case 'G': power = 3, base = 1024;
            break;
        case 'T': power = 4, base = 1024;
            break;
        case 'P': power = 5, base = 1024;
            break;
        }
    }

    // skip over spaces
    while (endptr[0] == ' ') ++endptr;

    // multiply size
    for (unsigned int p = 0; p < power; ++p)
        size *= base;

    return (endptr[0] == 0);
}

//! Format number as something like 1 TB
std::string FormatSiUnits(uint64_t number) {
    // may not overflow, std::numeric_limits<uint64_t>::max() == 16 EiB
    double multiplier = 1000.0;
    static const char* SI_endings[] = {
        "", "k", "M", "G", "T", "P", "E"
    };
    unsigned int scale = 0;
    double number_d = static_cast<double>(number);
    while (number_d >= multiplier) {
        number_d /= multiplier;
        ++scale;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(3) << number_d
        << ' ' << SI_endings[scale];
    return out.str();
}

//! Format number as something like 1 TiB
std::string FormatIecUnits(uint64_t number) {
    // may not overflow, std::numeric_limits<uint64_t>::max() == 16 EiB
    double multiplier = 1024.0;
    static const char* IEC_endings[] = {
        "", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei"
    };
    unsigned int scale = 0;
    double number_d = static_cast<double>(number);
    while (number_d >= multiplier) {
        number_d /= multiplier;
        ++scale;
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(3) << number_d
        << ' ' << IEC_endings[scale];
    return out.str();
}

} // namespace common
} // namespace thrill

/******************************************************************************/
