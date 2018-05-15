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

#include <tlx/meta/call_foreach.hpp>

#include <array>
#include <cassert>
#include <initializer_list>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

namespace thrill {
namespace common {

// forward declarations
class JsonLine;
class ScheduleThread;
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

//! A special class to output verbatim text
class JsonBeginObj
{
public:
    explicit JsonBeginObj(const std::string& str = std::string())
        : str_(str) { }
    std::string str_;
};

//! A special class to output verbatim text
class JsonEndObj
{
public:
    JsonEndObj() { }
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

    //! open JsonLogger with ofstream. if path is empty, output goes to stdout
    explicit JsonLogger(const std::string& path);

    //! open JsonLogger with a super logger
    explicit JsonLogger(JsonLogger* super);

    //! open JsonLogger with a super logger and some additional common key:value
    //! pairs
    template <typename... Args>
    explicit JsonLogger(JsonLogger* super, const Args& ... args);

    //! create new JsonLine instance which will be written to this logger.
    JsonLine line();

    template <typename Type>
    JsonLine operator << (const Type& t);

    //! launch background profiler
    void StartProfiler();

public:
    //! output to superior JsonLogger
    JsonLogger* super_ = nullptr;

    //! direct output stream for top loggers
    std::unique_ptr<std::ostream> os_;

    //! mutex to lock logger output
    std::mutex mutex_;

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
    //! ctor: bind output
    JsonLine(JsonLogger* logger, std::ostream& os)
        : logger_(logger), os_(os) {
        if (logger)
            lock_ = std::unique_lock<std::mutex>(logger_->mutex_);
    }

    //! non-copyable: delete copy-constructor
    JsonLine(const JsonLine&) = delete;
    //! non-copyable: delete assignment operator
    JsonLine& operator = (const JsonLine&) = delete;
    //! move-constructor: unlink pointer
    JsonLine(JsonLine&& o)
        : logger_(o.logger_), lock_(std::move(o.lock_)),
          os_(o.os_), items_(o.items_), sub_dict_(o.sub_dict_)
    { o.logger_ = nullptr; }

    struct ArrayTag { };
    struct DictionaryTag { };

    //! output any type
    template <typename Type>
    JsonLine& operator << (Type const& t);

    JsonLine& operator << (const JsonBeginObj& t) {
        // write key
        operator << (t.str_);
        PutSeparator();
        os_ << '{';
        items_ = 0;
        return *this;
    }

    JsonLine& operator << (const JsonEndObj&) {
        os_ << '}';
        return *this;
    }

    //! destructor: deliver to output
    ~JsonLine() {
        Close();
    }

    //! close the line
    void Close() {
        if (logger_ && items_ != 0) {
            assert(items_ % 2 == 0);
            os_ << '}' << std::endl;
            items_ = 0;
        }
        else if (!logger_ && sub_dict_) {
            os_ << '}';
            sub_dict_ = false;
        }
        else if (!logger_ && sub_array_) {
            os_ << ']';
            sub_array_ = false;
        }
    }

    //! number of items already put
    size_t items() const { return items_; }

    //! return JsonLine has sub-dictionary of this one
    template <typename Key>
    JsonLine sub(const Key& key) {
        // write key
        operator << (key);
        PutSeparator();
        os_ << '{';
        return JsonLine(DictionaryTag(), *this);
    }

    //! return JsonLine has sub-dictionary of this one
    template <typename Key>
    JsonLine arr(const Key& key) {
        // write key
        operator << (key);
        PutSeparator();
        os_ << '[';
        return JsonLine(ArrayTag(), *this);
    }

    //! return JsonLine has sub-dictionary of this one
    JsonLine obj() {
        if (items_ > 0)
            os_ << ',';
        os_ << '{';
        items_++;
        return JsonLine(DictionaryTag(), *this);
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

private:
    //! when destructed this object is delivered to the output.
    JsonLogger* logger_ = nullptr;

    //! lock on the logger output stream
    std::unique_lock<std::mutex> lock_;

    //! construct sub-dictionary
    JsonLine(struct DictionaryTag, JsonLine& parent)
        : os_(parent.os_), sub_dict_(true) { }

    //! construct sub-dictionary
    JsonLine(struct ArrayTag, JsonLine& parent)
        : os_(parent.os_), sub_array_(true) { }

public:
    //! reference to output stream
    std::ostream& os_;

    //! items counter for output stream
    size_t items_ = 0;

    //! indicator for sub-dictionaries.
    bool sub_dict_ = false;

    //! indicator for sub-array.
    bool sub_array_ = false;
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

template <typename... Args>
JsonLogger::JsonLogger(JsonLogger* super, const Args& ... args)
    : JsonLogger(super) {

    std::ostringstream oss;
    {
        // use JsonLine writer without a Logger to generate a valid string.
        JsonLine json(nullptr, oss);
        // -tb: do not use tlx::vexpand() because the order of argument
        // evaluation is undefined.
        tlx::call_foreach([&json](const auto& a) { json << a; }, args...);
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
