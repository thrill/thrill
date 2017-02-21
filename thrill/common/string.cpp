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

} // namespace common
} // namespace thrill

/******************************************************************************/
