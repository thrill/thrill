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

#include <cassert>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

namespace thrill {
namespace common {

// forward declaration
class JsonLine;

/*!
 * JsonLogger is a receiver of JSON output objects for logging.
 */
class JsonLogger
{
public:
    JsonLogger() { }

    //! method called by output objects
    void Output(JsonLine& output);

    //! create new JsonLine instance which will be written to this logger.
    template <typename Type>
    JsonLine operator << (Type const& t);

    //! collector stream
    std::ostringstream oss_;

    //! elements counter
    size_t elements_ = 0;
};

/*!
 * JsonLine is an object used to aggregate a set of key:value pairs for output
 * into a JSON log.
 */
class JsonLine
{
public:
    //! when destructed this object is delivered to the output.
    JsonLogger* out_;

    //! ctor: bind output
    explicit JsonLine(JsonLogger* output = nullptr)
        : out_(output) { }

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
    //! move-constructor: default
    JsonLine(JsonLine&&) = default;
    //! move-assignment operator: default
    JsonLine& operator = (JsonLine&&) = default;

    //! output any type
    template <typename Type>
    JsonLine& operator << (Type const& t);

    //! output any type, decay array types to pointers
    template <typename Type>
    JsonLine & PutDecay(Type const& t);

    //! output any type
    template <typename Type>
    JsonLine & Put(Type const& t);

    //! destructor: deliver to output
    ~JsonLine() {
        assert(out_->elements_ % 2 == 0);
        if (out_) out_->Output(*this);
    }

    //! put an elements separator (either ',' or ':') and increment counter.
    void PutSeparator() {
        if (out_->elements_ > 0) {
            out_->oss_ << (out_->elements_ % 2 == 0 ? ',' : ':');
        }
        out_->elements_++;
    }

    void PutEscapedChar(char ch) {
        // from: http://stackoverflow.com/a/7725289
        switch (ch) {
        case '\\': out_->oss_ << '\\' << '\\';
            break;
        case '"': out_->oss_ << '\\' << '"';
            break;
        case '/': out_->oss_ << '\\' << '/';
            break;
        case '\b': out_->oss_ << '\\' << 'b';
            break;
        case '\f': out_->oss_ << '\\' << 'f';
            break;
        case '\n': out_->oss_ << '\\' << 'n';
            break;
        case '\r': out_->oss_ << '\\' << 'r';
            break;
        case '\t': out_->oss_ << '\\' << 't';
            break;
        default: out_->oss_ << ch;
            break;
        }
    }
};

/******************************************************************************/
// Template specializations for JsonLine

template <>
JsonLine& JsonLine::Put(bool const& value) {
    out_->oss_ << (value ? "true" : "false");
    return *this;
}

template <>
JsonLine& JsonLine::Put(int const& value) {
    out_->oss_ << value;
    return *this;
}

template <>
JsonLine& JsonLine::Put(double const& value) {
    out_->oss_ << value;
    return *this;
}

template <>
JsonLine& JsonLine::Put(const char* const& str) {
    out_->oss_ << '"';
    for (const char* s = str; *s; ++s) PutEscapedChar(*s);
    out_->oss_ << '"';
    return *this;
}

template <>
JsonLine& JsonLine::Put(std::string const& str) {
    out_->oss_ << '"';
    for (std::string::const_iterator i = str.begin(); i != str.end(); ++i)
        PutEscapedChar(*i);
    out_->oss_ << '"';
    return *this;
}

// template <>
// JsonLine& JsonLine::Put(JsonLine const& obj) {
//     out_->oss_ << '{' << obj.oss_.str() << '}';
//     return *this;
// }

//! template switch for partial template specializations of Put().
template <typename Type>
struct JsonLinePutSwitch;

//! partial template specialization for std::vector<T>
template <typename Type>
struct JsonLinePutSwitch<std::vector<Type> >
{
    static void Put(JsonLine& out, std::vector<Type> const& vec) {
        out.out_->oss_ << '[';
        for (typename std::vector<Type>::const_iterator it = vec.begin();
             it != vec.end(); ++it) {
            if (it != vec.begin())
                out.out_->oss_ << ',';
            out.PutDecay(*it);
        }
        out.out_->oss_ << ']';
    }
};

//! fallback template in case the direct specializations above do not match.
template <typename Type>
JsonLine& JsonLine::Put(Type const& obj) {
    JsonLinePutSwitch<Type>::Put(*this, obj);
    return *this;
}

// due to problems with outputting const char[N], borrowed from
// http://stackoverflow.com/questions/6559622/template-specialization-for...

template <typename Type>
struct ArrayToPointerDecay
{
    using type = Type;
};

template <typename Type, std::size_t N>
struct ArrayToPointerDecay<Type[N]>
{
    using type = const Type *;
};

template <typename Type>
JsonLine& JsonLine::PutDecay(const Type& t) {
    using Decayed = typename ArrayToPointerDecay<Type>::type;
    return Put<Decayed>(t);
}

template <typename Type>
JsonLine& JsonLine::operator << (const Type& t) {
    PutSeparator();
    return PutDecay(t);
}

/******************************************************************************/
// JsonLogger

void JsonLogger::Output(JsonLine& output) {
    std::cout << '{' << oss_.str() << '}' << std::endl;
}

template <typename Type>
JsonLine JsonLogger::operator << (const Type& t) {
    oss_.str("");
    elements_ = 0;

    JsonLine out(this);
    out << t;
    return out;
}

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_JSON_LOGGER_HEADER

/******************************************************************************/
