/*******************************************************************************
 * c7a/common/timed_counter.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_TIMED_COUNTER_HEADER
#define C7A_COMMON_TIMED_COUNTER_HEADER

#include <chrono>
#include <vector>
#include <algorithm> //sort

namespace c7a {
namespace common {

class TimedCounter
{
public:
    typedef std::chrono::high_resolution_clock::time_point TimePoint;

    TimedCounter& operator += (const TimedCounter& rhs) {
        for (auto& o : rhs.Occurences())
            occurences_.push_back(o);
        std::sort(occurences_.begin(), occurences_.end());
        return *this;
    }

    TimedCounter operator + (const TimedCounter& rhs) {
        TimedCounter t;
        t += rhs;
        t += *this;
        return t;
    }

    void Trigger() {
        occurences_.push_back(timestamp());
    }

    void Reset() {
        occurences_.clear();
    }

    size_t Count() const {
        return occurences_.size();
    }

    std::vector<TimePoint> Occurences() const {
        return occurences_;
    }

private:
    inline TimePoint timestamp() {
        return std::chrono::high_resolution_clock::now();
    }

    std::vector<TimePoint> occurences_;
};
} //namespace common
} //namespace c7a
#endif // !C7A_COMMON_TIMED_COUNTER_HEADER

/******************************************************************************/
