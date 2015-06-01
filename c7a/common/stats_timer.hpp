/*******************************************************************************
 * c7a/common/stats_timer.hpp
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
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_STATS_TIMER_HEADER
#define C7A_COMMON_STATS_TIMER_HEADER

#include <chrono>
#include <cassert>
#include <ostream>

namespace c7a {
namespace common {

/*!
 * This class provides a statistical stop watch timer that can easily be
 * deactivated using a boolean template switch.
 *
 * It uses std::chrono to get the current time when start() is called. Then,
 * after some processing, the function stop() functions can be called, or
 * seconds() and other accessors can be called directly.
 */
template <bool Active = true>
class StatsTimer
{ };

template <>
class StatsTimer<true>
{
public:
    typedef std::chrono::steady_clock steady_clock;
    typedef std::chrono::steady_clock::time_point time_point;

    typedef std::chrono::microseconds duration;

protected:
    //! boolean whether the timer is currently running
    bool running_;

    //! total accumulated time in microseconds.
    duration accumulated_;

    //! last start time of the stop watch
    time_point last_start_;

public:
    //! Initialize and optionally immediately start the timer
    explicit StatsTimer(bool start_immediately = false)
        : running_(false),
          accumulated_() {
        if (start_immediately) Start();
    }

    //! Whether the timer is real
    bool Real() const { return true; }

    //! start timer
    void Start() {
        assert(!running_);
        running_ = true;
        last_start_ = steady_clock::now();
    }

    //! stop timer
    void Stop() {
        assert(running_);
        running_ = false;
        accumulated_ += std::chrono::duration_cast<duration>(
            steady_clock::now() - last_start_);
    }

    //! return accumulated time
    void Reset() {
        accumulated_ = duration(0);
        last_start_ = steady_clock::now();
    }

    //! return currently accumulated time
    duration Accumulated() const {
        duration d = accumulated_;

        if (running_)
            d += std::chrono::duration_cast<duration>(
                steady_clock::now() - last_start_);

        return d;
    }

    //! return currently accumulated time in microseconds
    std::chrono::microseconds::rep Microseconds() const {
        return std::chrono::duration_cast<std::chrono::microseconds>(
            Accumulated()).count();
    }

    //! return currently accumulated time in milliseconds
    std::chrono::milliseconds::rep Milliseconds() const {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            Accumulated()).count();
    }

    //! return currently accumulated time in seconds
    std::chrono::seconds::rep Seconds() const {
        return std::chrono::duration_cast<std::chrono::seconds>(
            Accumulated()).count();
    }

    //! accumulate elapsed time from another timer
    StatsTimer& operator += (const StatsTimer& tm) {
        accumulated_ += tm.accumulated_;
        return *this;
    }

    //! direct <<-operator for ostream. Can be used for printing with std::cout.
    friend std::ostream& operator << (std::ostream& os, const StatsTimer& t) {
        return os << t.Milliseconds() << "ms";
    }
};

template <>
class StatsTimer<false>
{
public:
    typedef std::chrono::steady_clock steady_clock;
    typedef std::chrono::steady_clock::time_point time_point;

    typedef std::chrono::microseconds duration;

public:
    //! Initialize and optionally immediately start the timer
    explicit StatsTimer(bool = false)
    { }

    //! Whether the timer is real
    bool Real() const { return false; }

    //! start timer
    void Start()
    { }

    //! stop timer
    void Stop()
    { }

    //! return accumulated time
    void Reset()
    { }

    //! return currently accumulated time
    duration Accumulated() const {
        return duration();
    }

    //! return currently accumulated time in microseconds
    std::chrono::microseconds::rep Microseconds() const {
        return 0;
    }

    //! return currently accumulated time in milliseconds
    std::chrono::milliseconds::rep Milliseconds() const {
        return 0;
    }

    //! return currently accumulated time in milliseconds
    std::chrono::seconds::rep Seconds() const {
        return 0;
    }

    //! accumulate elapsed time from another timer
    StatsTimer& operator += (const StatsTimer&) {
        return *this;
    }

    //! direct <<-operator for ostream. Can be used for printing with std::cout.
    friend std::ostream& operator << (std::ostream& os, const StatsTimer&) {
        return os << "<invalid>" << "ms";
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_STATS_TIMER_HEADER

/******************************************************************************/
