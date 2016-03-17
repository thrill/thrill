/*******************************************************************************
 * thrill/common/json_logger.hpp
 *
 * Logger for statistical output in JSON format for post-processing.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_JSON_LOGGER_HEADER
#define THRILL_COMMON_JSON_LOGGER_HEADER

#include <array>
#include <cassert>
#include <fstream>
#include <initializer_list>
#include <sstream>
#include <string>
#include <vector>

namespace thrill {
namespace common {

// forward declarations
class JsonLine;
template <typename Type>
struct JsonLinePutSwitch;

//! A special class to output verbatim text
class JsonVerbatim
{
public:
    explicit JsonVerbatim(const std::string& str = std::string())
        : str_(str) { }
    std::string str_;
};

//! A template to make writing temporary arrays easy: Array<int>{ 1, 2, 3 }.
template <typename Type>
using Array = Type[];

/*!
 * JsonLogger is a receiver of JSON output objects for logging.
 */
class JsonLogger
{
public:
    //! open JsonLogger with ofstream uninitialized to discard log output.
    JsonLogger() = default;

    //! open JsonLogger with ofstream
    explicit JsonLogger(const std::string& path);

    //! open JsonLogger with a super logger
    explicit JsonLogger(JsonLogger* super) : super_(super) { }

    //! open JsonLogger with a super logger and some additional common key:value
    //! pairs
    template <typename ... Args>
    explicit JsonLogger(JsonLogger* super, const Args& ... args);

    //! create new JsonLine instance which will be written to this logger.
    JsonLine line();

    template <typename Type>
    JsonLine operator << (const Type& t);

    //! method called by output objects
    void Output() {
        os_ << '}' << std::endl;
    }

public:
    //! output to superior JsonLogger
    JsonLogger* super_ = nullptr;

    //! direct output stream for top loggers
    std::ofstream os_;

    //! common items outputted to each line
    JsonVerbatim common_;

    //! friends for sending to os_
    friend class JsonLine;

    template <typename Type>
    friend struct JsonLinePutSwitch;
};

/*!
 * JsonLine is an object used to aggregate a set of key:value pairs for output
 * into a JSON log.
 */
class JsonLine
{
public:
    //! when destructed this object is delivered to the output.
    JsonLogger* logger_;

    //! reference to output stream
    std::ostream& os_;

    //! items counter for output stream
    size_t items_ = 0;

    //! ctor: bind output
    explicit JsonLine(JsonLogger* logger, std::ostream& os)
        : logger_(logger), os_(os) { }

    // //! ctor: initialize with a list of key:value pairs of variadic type.
    // template <typename ... Args>
    // explicit JsonLine(const Args& ... args) {
    //     using ForeachExpander = int[];
    //     (void)ForeachExpander { (operator << (args), 0) ... };
    // }

    //! non-copyable: delete copy-constructor
    JsonLine(const JsonLine&) = delete;
    //! non-copyable: delete assignment operator
    JsonLine& operator = (const JsonLine&) = delete;
    //! move-constructor: unlink pointer
    JsonLine(JsonLine&& o)
        : logger_(o.logger_), os_(o.os_), items_(o.items_)
    { o.logger_ = nullptr; }

    //! output any type
    template <typename Type>
    JsonLine& operator << (Type const& t);

    //! destructor: deliver to output
    ~JsonLine() {
        if (logger_) {
            assert(items_ % 2 == 0);
            logger_->Output();
        }
    }

    //! put an items separator (either ',' or ':') and increment counter.
    void PutSeparator() {
        if (items_ > 0) {
            os_ << (items_ % 2 == 0 ? ',' : ':');
        }
        items_++;
    }

    void PutEscapedChar(char ch) {
        // from: http://stackoverflow.com/a/7725289
        switch (ch) {
        case '\\': os_ << '\\' << '\\';
            break;
        case '"': os_ << '\\' << '"';
            break;
        case '/': os_ << '\\' << '/';
            break;
        case '\b': os_ << '\\' << 'b';
            break;
        case '\f': os_ << '\\' << 'f';
            break;
        case '\n': os_ << '\\' << 'n';
            break;
        case '\r': os_ << '\\' << 'r';
            break;
        case '\t': os_ << '\\' << 't';
            break;
        default: os_ << ch;
            break;
        }
    }
};

/******************************************************************************/
// Template specializations for JsonLine

static inline
JsonLine& Put(JsonLine& line, bool const& value) {
    line.os_ << (value ? "true" : "false");
    return line;
}

static inline
JsonLine& Put(JsonLine& line, int const& value) {
    line.os_ << value;
    return line;
}

static inline
JsonLine& Put(JsonLine& line, unsigned int const& value) {
    line.os_ << value;
    return line;
}

static inline
JsonLine& Put(JsonLine& line, long const& value) {
    line.os_ << value;
    return line;
}

static inline
JsonLine& Put(JsonLine& line, unsigned long const& value) {
    line.os_ << value;
    return line;
}

static inline
JsonLine& Put(JsonLine& line, long long const& value) {
    line.os_ << value;
    return line;
}

static inline
JsonLine& Put(JsonLine& line, unsigned long long const& value) {
    line.os_ << value;
    return line;
}

static inline
JsonLine& Put(JsonLine& line, double const& value) {
    line.os_ << value;
    return line;
}

static inline
JsonLine& Put(JsonLine& line, const char* const& str) {
    line.os_ << '"';
    for (const char* s = str; *s; ++s) line.PutEscapedChar(*s);
    line.os_ << '"';
    return line;
}

static inline
JsonLine& Put(JsonLine& line, std::string const& str) {
    line.os_ << '"';
    for (std::string::const_iterator i = str.begin(); i != str.end(); ++i)
        line.PutEscapedChar(*i);
    line.os_ << '"';
    return line;
}

template <typename Type, std::size_t N>
static inline
JsonLine& Put(JsonLine& line, const Type(&arr)[N]) {
    line.os_ << '[';
    for (size_t i = 0; i < N; ++i) {
        if (i != 0) line.os_ << ',';
        Put(line, arr[i]);
    }
    line.os_ << ']';
    return line;
}

template <typename Type>
static inline
JsonLine& Put(JsonLine& line, std::initializer_list<Type> const& list) {
    line.os_ << '[';
    for (typename std::initializer_list<Type>::const_iterator it = list.begin();
         it != list.end(); ++it) {
        if (it != list.begin())
            line.os_ << ',';
        Put(line, *it);
    }
    line.os_ << ']';
    return line;
}

template <typename Type>
static inline
JsonLine& Put(JsonLine& line, std::vector<Type> const& vec) {
    line.os_ << '[';
    for (typename std::vector<Type>::const_iterator it = vec.begin();
         it != vec.end(); ++it) {
        if (it != vec.begin())
            line.os_ << ',';
        Put(line, *it);
    }
    line.os_ << ']';
    return line;
}

template <typename Type, std::size_t N>
static inline
JsonLine& Put(JsonLine& line, std::array<Type, N> const& arr) {
    line.os_ << '[';
    for (typename std::array<Type, N>::const_iterator it = arr.begin();
         it != arr.end(); ++it) {
        if (it != arr.begin())
            line.os_ << ',';
        Put(line, *it);
    }
    line.os_ << ']';
    return line;
}

static inline
JsonLine& Put(JsonLine& line, JsonVerbatim const& verbatim) {
    // undo increment of item counter
    --line.items_;
    line.os_ << verbatim.str_;
    return line;
}

//! template << forwards to ::Put for ADL type switching
template <typename Type>
inline JsonLine& JsonLine::operator << (Type const& t) {
    PutSeparator();
    return Put(*this, t);
}

/******************************************************************************/
// JsonLogger

template <typename ... Args>
JsonLogger::JsonLogger(JsonLogger* super, const Args& ... args)
    : JsonLogger(super) {

    std::ostringstream oss;
    {
        // use JsonLine writer without a Logger to generate a valid string.
        JsonLine json(nullptr, oss);
        using ForeachExpander = int[];
        (void)ForeachExpander {
            (json << (args), 0) ...
        };
    }
    common_ = JsonVerbatim(oss.str());
}

template <typename Type>
inline JsonLine JsonLogger::operator << (const Type& t) {
    JsonLine out = line();
    out << t;
    return out;
}

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_JSON_LOGGER_HEADER

/******************************************************************************/
