/*******************************************************************************
 * thrill/common/timed_counter.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_TIMED_COUNTER_HEADER
#define THRILL_COMMON_TIMED_COUNTER_HEADER

#include <algorithm>
#include <chrono>
#include <vector>

namespace thrill {
namespace common {

//! TimedCounter counts the number of \ref Trigger() invokes.
//! The time points of these invocations are stored.
class TimedCounter
{
public:
    using TimePoint = std::chrono::high_resolution_clock::time_point;

    // no copy ctor
    TimedCounter(const TimedCounter& that) = delete;
    // move is okay
    TimedCounter(TimedCounter&& rhs) {
        occurences_ = std::move(rhs.occurences_);
    }

    TimedCounter() { }

    //! Adds the occurences of another TimedCounter to this instance.
    //! Occurences will be sorted to be ascending
    TimedCounter& operator += (const TimedCounter& rhs) {
        for (auto & o : rhs.Occurences())
            occurences_.push_back(o);
        std::sort(occurences_.begin(), occurences_.end());
        return *this;
    }

    //! Adds occurences of two instances and sorts them ascending
    TimedCounter operator + (const TimedCounter& rhs) {
        TimedCounter t;
        t += rhs;
        t += *this;
        return t;
    }

    //! Registers a new Occurence on this TimedCounter
    void Trigger() {
        occurences_.push_back(timestamp());
    }

    //! Drops all Occurences of this timer.
    void Reset() {
        occurences_.clear();
    }

    //! Returns the number of Occurences on this timer
    size_t Count() const {
        return occurences_.size();
    }

    //! Returns the Occurences of this timer.
    std::vector<TimePoint> Occurences() const {
        return occurences_;
    }

private:
    inline TimePoint timestamp() {
        return std::chrono::high_resolution_clock::now();
    }

    std::vector<TimePoint> occurences_;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_TIMED_COUNTER_HEADER

/******************************************************************************/
