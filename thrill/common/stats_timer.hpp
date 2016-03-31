/*******************************************************************************
 * thrill/common/stats_timer.hpp
 *
 * Distributed under the Boost Software License, Version 1.0.
 * (See accompanying file LICENSE_1_0.txt or copy at
 * http://www.boost.org/LICENSE_1_0.txt)
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2014 Thomas Keh <thomas.keh@student.kit.edu>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_STATS_TIMER_HEADER
#define THRILL_COMMON_STATS_TIMER_HEADER

#include <thrill/common/atomic_movable.hpp>
#include <thrill/common/json_logger.hpp>

#include <cassert>
#include <chrono>
#include <ostream>

namespace thrill {
namespace common {

/*!
 * This class provides a statistical stop watch timer that can easily be
 * deactivated using a boolean template switch.
 *
 * It uses std::chrono to get the current time when start() is called. Then,
 * after some processing, the function stop() functions can be called, or
 * seconds() and other accessors can be called directly.
 */
template <bool Active>
class StatsTimerBase;

template <>
class StatsTimerBase<true>
{
public:
    using steady_clock = std::chrono::steady_clock;
    using time_point = std::chrono::steady_clock::time_point;

    using duration = std::chrono::microseconds;

protected:
    //! boolean whether the timer is currently running
    common::AtomicMovable<bool> running_;

    //! total accumulated time in microseconds.
    duration accumulated_;

    //! last start time of the stop watch
    time_point last_start_;

public:
    //! Initialize and optionally immediately start the timer
    explicit StatsTimerBase(bool start_immediately)
        : running_(false), accumulated_() {
        if (start_immediately) Start();
    }

    //! Whether the timer is real
    bool Real() const { return true; }

    //! Whether the timer is running
    bool running() const {
        return running_;
    }

    //! start timer
    StatsTimerBase& Start() {
        assert(!running_);
        running_ = true;
        last_start_ = steady_clock::now();
        return *this;
    }

    //! start timer only if it not running
    StatsTimerBase& StartEventually() {
        if (!running_) {
            running_ = true;
            last_start_ = steady_clock::now();
        }
        return *this;
    }

    //! stop timer
    StatsTimerBase& Stop() {
        assert(running_);
        running_ = false;
        accumulated_ += std::chrono::duration_cast<duration>(
            steady_clock::now() - last_start_);
        return *this;
    }

    //! stop timer if it is running
    StatsTimerBase& StopEventually() {
        if (running_)
            Stop();
        return *this;
    }

    //! return accumulated time
    StatsTimerBase& Reset() {
        accumulated_ = duration(0);
        last_start_ = steady_clock::now();
        return *this;
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

    //! return currently accumulated time in seconds as double with microseconds
    //! precision
    double SecondsDouble() const {
        return static_cast<double>(Microseconds()) / 1e6;
    }

    //! accumulate elapsed time from another timer
    StatsTimerBase& operator += (const StatsTimerBase& tm) {
        accumulated_ += tm.accumulated_;
        return *this;
    }

    //! direct <<-operator for ostream. Can be used for printing with std::cout.
    friend std::ostream& operator << (std::ostream& os, const StatsTimerBase& t) {
        return os << static_cast<double>(t.Microseconds()) / 1e6;
    }

    friend JsonLine& Put(JsonLine& line, const StatsTimerBase& t) {
        return Put(line, t.Microseconds());
    }
};

template <>
class StatsTimerBase<false>
{
public:
    using steady_clock = std::chrono::steady_clock;
    using time_point = std::chrono::steady_clock::time_point;

    using duration = std::chrono::microseconds;

    //! Initialize and optionally immediately start the timer
    explicit StatsTimerBase(bool /* autostart */) { }

    //! Whether the timer is real
    bool Real() const { return false; }

    //! Whether the timer is running
    bool running() const {
        return false;
    }

    //! start timer
    StatsTimerBase& Start() {
        return *this;
    }

    //! start timer only if it not running
    StatsTimerBase& StartEventually() {
        return *this;
    }

    //! stop timer
    StatsTimerBase& Stop() {
        return *this;
    }

    //! stop timer if it is running
    StatsTimerBase& StopEventually() {
        return *this;
    }

    //! return accumulated time
    StatsTimerBase& Reset() {
        return *this;
    }

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
    StatsTimerBase& operator += (const StatsTimerBase&) {
        return *this;
    }

    //! direct <<-operator for ostream. Can be used for printing with std::cout.
    friend std::ostream& operator << (std::ostream& os, const StatsTimerBase&) {
        return os << "<invalid>";
    }
};

template <bool Active>
class StatsTimerBaseStarted : public StatsTimerBase<Active>
{
public:
    //! Initialize and automatically start the timer
    StatsTimerBaseStarted()
        : StatsTimerBase<Active>(/* start_immediately */ true) { }
};

template <bool Active>
class StatsTimerBaseStopped : public StatsTimerBase<Active>
{
public:
    //! Initialize but do NOT automatically start the timer
    StatsTimerBaseStopped()
        : StatsTimerBase<Active>(/* start_immediately */ false) { }
};

using StatsTimer = StatsTimerBase<true>;
using StatsTimerStart = StatsTimerBaseStarted<true>;
using StatsTimerStopped = StatsTimerBaseStopped<true>;

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_STATS_TIMER_HEADER

/******************************************************************************/
