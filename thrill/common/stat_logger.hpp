/*******************************************************************************
 * thrill/common/stat_logger.hpp
 *
 * Logger for Stat-Lines, which creates basic JSON lines
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STAT_LOGGER_HEADER
#define THRILL_COMMON_STAT_LOGGER_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/logger.hpp>

#include <string>
#include <type_traits>
#include <iostream>

namespace thrill {
namespace common {

static const bool stats_enabled = true;

template <bool Enabled>
class StatLogger
{ };

template <>
class StatLogger<true>
{
protected:
    //! collector stream
    std::basic_ostringstream<
        char, std::char_traits<char>, LoggerAllocator<char> > oss_;

    size_t elements_ = 0;

public:
    explicit StatLogger(size_t rank) {
        oss_ << "{\"WorkerID\":" << rank;
        elements_ = 2;
    }

    StatLogger() {
        oss_ << '{';
    }

    //! output and escape std::string
    StatLogger& operator << (const std::string& str) {
        if (elements_ > 0) {
            if (elements_ % 2 == 0) {
                oss_ << ',';
            }
            else {
                oss_ << ':';
            }
        }
        oss_ << '"';
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
        oss_ << '"';
        return *this;
    }

    //! output any type, including io manipulators
    template <typename AnyType>
    StatLogger& operator << (const AnyType& at) {
        if (elements_ > 0) {
            if (elements_ % 2 == 0) {
                oss_ << ',';
            }
            else {
                oss_ << ':';
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
            oss_ << '"' << at << '"';
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

#define STAT ::thrill::common::StatLogger<::thrill::common::stats_enabled>()
#define STATC(rank) ::thrill::common::StatLogger<::thrill::common::stats_enabled>(rank)

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_STAT_LOGGER_HEADER

/******************************************************************************/
