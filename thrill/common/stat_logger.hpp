/*******************************************************************************
 * thrill/common/stat_logger.hpp
 *
 * Logger for Stat-Lines, which creates basic JSON lines
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STAT_LOGGER_HEADER
#define THRILL_COMMON_STAT_LOGGER_HEADER

#include <thrill/common/logger.hpp>

#include <iostream>
#include <string>
#include <type_traits>

namespace thrill {
namespace common {

static const bool stats_enabled = false;

template <bool Enabled>
class StatLogger
{ };

template <>
class StatLogger<true>
{
private:
    //! collector stream
    std::basic_ostringstream<
        char, std::char_traits<char>, LoggerAllocator<char> > oss_;

    size_t elements_ = 0;

public:
    StatLogger() {
        oss_ << "{";
    }

    //! output and escape std::string
    StatLogger& operator << (const std::string& str) {
        if (elements_ > 0) {
            if (elements_ % 2 == 0) {
                oss_ << ",";
            }
            else {
                oss_ << ":";
            }
        }
        oss_ << "\"";
        // from: http://stackoverflow.com/a/7725289
        for (auto iter = str.begin(); iter != str.end(); iter++) {
            switch (*iter) {
            case '\\': oss_ << "\\\\";
                break;
            case '"': oss_ << "\\\"";
                break;
            case '/': oss_ << "\\/";
                break;
            case '\b': oss_ << "\\b";
                break;
            case '\f': oss_ << "\\f";
                break;
            case '\n': oss_ << "\\n";
                break;
            case '\r': oss_ << "\\r";
                break;
            case '\t': oss_ << "\\t";
                break;
            default: oss_ << *iter;
                break;
            }
        }
        elements_++;
        oss_ << "\"";
        return *this;
    }

    //! output any type, including io manipulators
    template <typename AnyType>
    StatLogger& operator << (const AnyType& at) {
        if (elements_ > 0) {
            if (elements_ % 2 == 0) {
                oss_ << ",";
            }
            else {
                oss_ << ":";
            }
        }
        elements_++;
        if (std::is_integral<AnyType>::value || std::is_floating_point<AnyType>::value) {
            oss_ << at;
        }
        else if (std::is_same<AnyType, bool>::value) {
            if (at) {
                oss_ << "true";
            }
            else {
                oss_ << "false";
            }
        }
        else {
            oss_ << "\"" << at << "\"";
        }
        return *this;
    }

    //! destructor: output a } and a newline
    ~StatLogger() {
        assert(elements_ % 2 == 0);
        oss_ << "}\n";
        std::cout << oss_.str();
    }
};

template <>
class StatLogger<false>
{

public:
    template <typename AnyType>
    StatLogger& operator << (const AnyType&) {
        return *this;
    }
};

#define STAT_NO_RANK ::thrill::common::StatLogger<::thrill::common::stats_enabled>()

//! Creates a common::StatLogger with {"WorkerID":my rank in the beginning
#define STAT(ctx) ::thrill::common::StatLogger<::thrill::common::stats_enabled>() << "worker_id" << ctx.my_rank()
#define STATC ::thrill::common::StatLogger<::thrill::common::stats_enabled>() << "worker_id" << context_.my_rank()

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_STAT_LOGGER_HEADER

/******************************************************************************/
