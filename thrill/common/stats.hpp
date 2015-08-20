/*******************************************************************************
 * thrill/common/stats.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_STATS_HEADER
#define C7A_COMMON_STATS_HEADER

#include <thrill/common/stats_timer.hpp>
#include <thrill/common/timed_counter.hpp>

#include <cmath> //sqrt
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <utility>

//! Macros to check if shared pointers to stats objects are valid
#define START_TIMER(timer)      if (timer) timer->Start();
#define STOP_TIMER(timer)       if (timer) timer->Stop();
#define Trigger(timed_counter)  if (timed_counter) timed_counter->Trigger();

namespace c7a {
namespace common {

//! Helper to create Counters, TimedCounters and Timers.
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
template <bool Active>
class Stats
{ };

template <>
class Stats<true>
{
public:
    using NamedTimedcounter = std::pair<std::string, TimedCounter>;
    using TimeStamp = decltype(std::chrono::high_resolution_clock::now());

    Stats(const Stats& rhs) = delete;
    Stats(Stats&&) = delete;
    Stats& operator = (const Stats&) = delete;

    Stats() :
        program_start_(std::chrono::high_resolution_clock::now()) { }

    TimedCounterPtr CreateTimedCounter(const std::string& group, const std::string& label) {
        auto result = timed_counters_.insert(std::make_pair(group, std::make_pair(label, std::make_shared<TimedCounter>())));
        return result->second.second;
    }

    TimerPtr CreateTimer(const std::string& group, const std::string& label, bool auto_start = false) {
        auto result = timers_.insert(std::make_pair(group, std::make_pair(label, std::make_shared<StatsTimer<true> >(auto_start))));
        return result->second.second;
    }

    void AddReport(const std::string& group, const std::string& label, const std::string& content) {
        reports_.insert(std::make_pair(group, std::make_pair(label, content)));
    }

    ~Stats() {
        std::set<std::string> group_names;
        for (const auto& it : timed_counters_)
            group_names.insert(it.first);
        for (const auto& it : timers_)
            group_names.insert(it.first);
        for (const auto& it : reports_)
            group_names.insert(it.first);
        for (const auto& g : group_names)
            std::cout << PrintGroup(g) << std::endl;
    }

    std::string PrintGroup(const std::string& group_name) {
        std::ostringstream ss;
        auto group_timed_counters = timed_counters_.equal_range(group_name);
        for (auto group_it = group_timed_counters.first; group_it != group_timed_counters.second; group_it++)
            ss << group_name << "; " << PrintTimedCounter(group_it->second.second, group_it->second.first) << std::endl;

        auto group_timers = timers_.equal_range(group_name);
        for (auto group_it = group_timers.first; group_it != group_timers.second; group_it++)
            ss << group_name << "; " << PrintStatsTimer(group_it->second.second, group_it->second.first) << std::endl;
        auto stats = PrintStatsTimerAverage(group_name);
        if (!stats.empty())
            ss << group_name << ";" << stats << std::endl;

        auto group_reports = reports_.equal_range(group_name);
        for (auto group_it = group_reports.first; group_it != group_reports.second; group_it++)
            ss << group_name << "; " << group_it->second.first << "; " << group_it->second.second << std::endl;
        return ss.str();
    }

    //! Returns the string-representation of a \ref TimedCounter in one line.
    //! format is 'TimedCounter(NAME): 3 [123, 456, 789, ]' or
    //! 'TimedCounter(NAME): 0'.
    //! Default name is 'unnamed'.
    std::string PrintTimedCounter(const TimedCounterPtr& tc, std::string name = "unnamed") {
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
        ss << "timer; " << name << "; " << *timer;
        return ss.str();
    }

    std::string PrintStatsTimerAverage(const std::string group_name) {
        std::stringstream ss;
        std::chrono::microseconds::rep sum = 0;
        std::chrono::microseconds::rep mean = 0;
        std::chrono::microseconds::rep sum_deviation = 0;
        size_t count = 0;
        auto group_timers = timers_.equal_range(group_name);
        for (auto group_it = group_timers.first; group_it != group_timers.second; group_it++) {
            sum += group_it->second.second->Microseconds();
            count++;
        }
        if (count < 1) {
            return "";
        }
        mean = sum / count;
        for (auto group_it = group_timers.first; group_it != group_timers.second; group_it++) {
            auto val = group_it->second.second->Microseconds();
            sum_deviation += (val - mean) * (val - mean);
        }
        auto deviation = std::sqrt(sum_deviation / count);
        ss << " total; " << sum << "; count; " << count << "; avg; " << mean << "; std-dev; " << deviation;
        return ss.str();
    }

private:
    std::multimap<std::string, std::pair<std::string, TimedCounterPtr> > timed_counters_;
    std::multimap<std::string, std::pair<std::string, TimerPtr> > timers_;
    std::multimap<std::string, std::pair<std::string, std::string> > reports_;
    const TimeStamp program_start_;

    //! relative duration in microseconds to creation of this instance.
    inline long Relative(const TimeStamp& time_point) {
        return std::chrono::duration_cast<std::chrono::milliseconds>(time_point - program_start_).count();
    }
};

template <>
class Stats<false>
{
public:
    Stats() = default;
    Stats(const Stats& rhs) = delete;
    Stats(Stats&&) = delete;
    Stats& operator = (const Stats&) = delete;

    TimedCounterPtr CreateTimedCounter(const std::string& /* group */, const std::string& /*label*/) {
        return TimedCounterPtr();
    }

    TimerPtr CreateTimer(const std::string& /*group*/, const std::string& /*label*/) {
        return TimerPtr();
    }

    TimerPtr CreateTimer(const std::string& /*group*/, const std::string& /*label*/, bool /*auto_start */) {
        return TimerPtr();
    }

    void AddReport(const std::string& /*group*/, const std::string& /*label*/, const std::string& /*content*/) {
        // do nothing
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_STATS_HEADER

/******************************************************************************/
