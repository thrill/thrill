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

/******************************************************************************/

std::string EscapeHtml(const std::string& str) {
    std::string os;
    os.reserve(str.size());

    for (std::string::const_iterator si = str.begin(); si != str.end(); ++si)
    {
        if (*si == '&') os += "&amp;";
        else if (*si == '<') os += "&lt;";
        else if (*si == '>') os += "&gt;";
        else if (*si == '"') os += "&quot;";
        else os += *si;
    }

    return os;
}

/******************************************************************************/

//! Parse a string like "343KB" or " 44 GiB " into the corresponding size in
//! bytes.
bool ParseSiIecUnits(const char* str, uint64_t& size, char default_unit) {
    char* endptr;
    size = strtoul(str, &endptr, 10);
    if (!endptr) return false;                  // parse failed, no number

    while (*endptr == ' ') ++endptr;            // skip over spaces

    // multiply with base ^ power
    unsigned int base = 1000;
    unsigned int power = 0;

    if (*endptr == 'k' || *endptr == 'K')
        power = 1, ++endptr;
    else if (*endptr == 'm' || *endptr == 'M')
        power = 2, ++endptr;
    else if (*endptr == 'g' || *endptr == 'G')
        power = 3, ++endptr;
    else if (*endptr == 't' || *endptr == 'T')
        power = 4, ++endptr;
    else if (*endptr == 'p' || *endptr == 'P')
        power = 5, ++endptr;

    // switch to power of two (only if power was set above)
    if ((*endptr == 'i' || *endptr == 'I') && power != 0)
        base = 1024, ++endptr;

    // byte indicator
    if (*endptr == 'b' || *endptr == 'B') {
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
    while (*endptr == ' ') ++endptr;

    // multiply size
    for (unsigned int p = 0; p < power; ++p)
        size *= base;

    return (*endptr == 0);
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
