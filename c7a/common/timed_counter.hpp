/*******************************************************************************
 * c7a/common/timed_counter.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_TIMED_COUNTER_HEADER
#define C7A_COMMON_TIMED_COUNTER_HEADER

#include <chrono>
#include <vector>
#include <algorithm> //sort

namespace c7a {
namespace common {

//! TimedCounter counts the number of \ref Trigger() invokes.
//! The time points of these invocations are stored.
class TimedCounter
{
public:
    typedef std::chrono::high_resolution_clock::time_point TimePoint;

    //no copy ctor
    TimedCounter(const TimedCounter& that) = delete;
    //move is okay
    TimedCounter(TimedCounter && rhs) {
        occurences_ = rhs.occurences_;
    }

    explicit TimedCounter() { }

    //! Adds the occurences of another TimedCounter to this instance.
    //! Occurences will be sorted to be ascending
    TimedCounter& operator += (const TimedCounter& rhs) {
        for (auto& o : rhs.Occurences())
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

using TimedCounterPtr = std::shared_ptr<TimedCounter>;

} //namespace common
} //namespace c7a
#endif // !C7A_COMMON_TIMED_COUNTER_HEADER

/******************************************************************************/
