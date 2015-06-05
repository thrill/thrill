/*******************************************************************************
 * c7a/common/stats.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_STATS_HEADER
#define C7A_COMMON_STATS_HEADER

#include <list>

#include <c7a/common/stats_timer.hpp>
#include <c7a/common/timed_counter.hpp>

namespace c7a {
namespace common {

//! Helper to create \ref Counter "Counters", \ref TimedCounter "TimedCounters" and \ref timer "Timers".
//! Counters and such can be named on creation. The name is a simple mechanism to make the output more
//! user-friendly. Names are not used to identify Counters uniquely.
//!
//! This class provides methods for printing Counters and such.
//! Times are printed relative to the creation of this instance. It is worth mentioning that it makes
//! sense to have exactly one Stats instance for the whole program to see all timer values relative to
//! the program start.
//!
//! All Counters and such are held locally until the destructor is called.
//! Depending on the configuration all Counters and such will be printed to sLOG
class Stats
{
public:
    using NamedTimedcounter = std::pair<std::string, TimedCounter>;
    typedef decltype (std::chrono::high_resolution_clock::now ()) TimeStamp;

    Stats() :
        program_start_(std::chrono::high_resolution_clock::now()) { }

    Stats(const Stats& rhs) = delete;
    Stats(Stats&&) = delete;
    Stats& operator = (const Stats&) = delete;

    TimedCounterPtr CreateTimedCounter(std::string label) {
        timed_counters_.emplace_back(std::make_pair(label, std::make_shared<TimedCounter>()));
        return std::get<1>(timed_counters_.back());
    }

    TimerPtr CreateTimer(std::string label, bool auto_start = false) {
        timers_.emplace_back(std::make_pair(label, std::make_shared<StatsTimer<true>>(auto_start)));
        return std::get<1>(timers_.back());
    }

    void AddReport(std::string label, std::string content) {
        reports_.emplace_back(std::make_pair(label, content));
    }

    ~Stats() {
        if (dump_to_log_) {
            for (auto& ntc : timed_counters_) {
                std::cout << PrintTimedCounter(std::get<1>(ntc), std::get<0>(ntc)) << std::endl;
            }
            for (auto& timer : timers_) {
                std::cout << PrintStatsTimer(std::get<1>(timer), std::get<0>(timer)) << std::endl;
            }
            for (auto& report : reports_) {
                std::cout << std::get<0>(report) << ":" << std::get<1>(report) << std::endl;
            }
        }
    }

    //! Returns the string-representation of a \ref TimedCounter in one line.
    //! format is 'TimedCounter(NAME): 3 [123, 456, 789, ]' or
    //! 'TimedCounter(NAME): 0'.
    //! Default name is 'unnamed'.
    std::string PrintTimedCounter(const TimedCounterPtr & tc, std::string name = "unnamed") {
        std::stringstream ss;
        ss << "TimedCounter(" << name << "): " << tc->Count();
        if (tc->Count() > 0) {
            ss << " [";
            for (auto& x : tc->Occurences())
                ss << Relative(x) << "ms, ";
            ss.seekp(ss.str().length() - 2);
            ss << "]";
        }
        return ss.str();
    }

    std::string PrintStatsTimer(const TimerPtr& timer, std::string name = "unnamed") {
        std::stringstream ss;
        ss << "Timer(" << name << "): " << *timer;
        return ss.str();
    }

private:
    static const bool dump_to_log_ = true;
    std::list<std::tuple<std::string, TimedCounterPtr> > timed_counters_;
    std::list<std::tuple<std::string, TimerPtr > > timers_;
    std::list<std::tuple<std::string, std::string> > reports_;
    const TimeStamp program_start_;

    //! relative duration in microseconds to creation of this instance.
    inline long Relative(const TimeStamp& time_point) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(time_point - program_start_).count();
    }
};
} //namespace common
} //namespace c7a
#endif // !C7A_COMMON_STATS_HEADER

/******************************************************************************/
