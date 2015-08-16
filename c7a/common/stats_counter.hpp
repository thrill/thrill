/*******************************************************************************
 * c7a/common/stats_counter.hpp
 *
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2014 Thomas Keh <thomas.keh@student.kit.edu>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_STATS_COUNTER_HEADER
#define C7A_COMMON_STATS_COUNTER_HEADER

#include <algorithm>

namespace c7a {
namespace common {

/*!
 * This class provides a statistical counter that can easily be deactivated
 * using a boolean template switch.  It's basically a wrapper for an counter
 * type, like unsigned long long value. If further operators are needed, they
 * should be added.
 */
template <typename _ValueType, bool Active = true>
class StatsCounter
{ };

template <typename _ValueType>
class StatsCounter<_ValueType, true>
{
public:
    //! The counter's value type
    using ValueType = _ValueType;

protected:
    //! The counter's value
    ValueType value_;

public:
    //! The constructor. Initializes the counter to 0.
    StatsCounter(const ValueType& initial = ValueType()) // NOLINT
        : value_(initial)
    { }

    //! Whether the counter is active
    bool Real() const { return true; }

    //! Increases the counter by right.
    StatsCounter& operator += (const ValueType& right) {
        value_ += right;
        return *this;
    }

    //! Increases the counter by 1 (prefix).
    StatsCounter& operator ++ () {
        ++value_;
        return *this;
    }

    //! Increases the counter by 1 (postfix).
    StatsCounter operator ++ (int) { // NOLINT
        StatsCounter copy = *this;
        ++value_;
        return copy;
    }

    //! Set the counter to other if other is larger than the current counter
    //! value.
    void set_max(const ValueType& other) {
        value_ = std::max(value_, other);
    }

    /*!
     * Cast to counter_type: Returns the counter's value as a regular integer
     * value.  This can be used as a getter as well as for printing with
     * std::out.
     */
    operator ValueType () const
    {
        return value_;
    }

    ValueType value() const {
        return value_;
    }
};

template <typename _ValueType>
class StatsCounter<_ValueType, false>
{
public:
    //! The counter's value type
    using ValueType = _ValueType;

public:
    StatsCounter(const ValueType& = ValueType()) // NOLINT
    { }

    //! Whether the counter is active
    bool Real() const { return false; }

    StatsCounter& operator += (const ValueType&)
    { return *this; }

    StatsCounter& operator ++ ()
    { return *this; }

    StatsCounter& operator ++ (int) // NOLINT
    { return *this; }

    void set_max(const ValueType&)
    { }

    operator ValueType () const
    {
        return ValueType();
    }

    ValueType value() const {
        return ValueType();
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_STATS_COUNTER_HEADER

/******************************************************************************/
