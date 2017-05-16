/*******************************************************************************
 * thrill/core/delta_stream.hpp
 *
 * Encode ascending values as deltas and deliver them to another stream.
 * Contains both delta stream writer and stream reader.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_DELTA_STREAM_HEADER
#define THRILL_CORE_DELTA_STREAM_HEADER

#include <thrill/core/bit_stream.hpp>

namespace thrill {
namespace core {

/******************************************************************************/
// DeltaStreamWriter

template <typename StreamWriter, typename Type, Type offset_ = Type()>
class DeltaStreamWriter
{
public:
    explicit DeltaStreamWriter(
        StreamWriter& writer, const Type& initial = Type())
        : writer_(writer), delta_(initial) { }

    void Put(const Type& value) {
        assert(value >= delta_ + offset_);
        writer_.Put(value - delta_ - offset_);
        delta_ = value;
    }

private:
    //! output writer
    StreamWriter& writer_;

    //! delta for output
    Type delta_ = Type();
};

/******************************************************************************/
// DeltaStreamReader

template <typename StreamReader, typename Type, Type offset_ = Type()>
class DeltaStreamReader
{
public:
    explicit DeltaStreamReader(
        StreamReader& reader, const Type& initial = Type())
        : reader_(reader), delta_(initial) { }

    bool HasNext() {
        return reader_.HasNext();
    }

    template <typename Type2>
    Type Next() {
        static_assert(std::is_same<Type, Type2>::value, "Invalid Next() call");
        Type value = reader_.template Next<Type>();
        delta_ += value + offset_;
        return delta_;
    }

private:
    //! output reader
    StreamReader& reader_;

    //! delta for output
    Type delta_ = Type();
};

/******************************************************************************/

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_DELTA_STREAM_HEADER

/******************************************************************************/
