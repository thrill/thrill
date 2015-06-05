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

#include <set>
#include <map>
#include <sstream>
#include <cmath> //sqrt

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

    TimedCounterPtr CreateTimedCounter(const std::string& group, const std::string& label) {
        auto result = timed_counters_.insert(std::make_pair(group, std::make_pair(label, std::make_shared<TimedCounter>())));
        return result->second.second;
    }

    TimerPtr CreateTimer(const std::string& group, const std::string& label, bool auto_start = false) {
        auto result = timers_.insert(std::make_pair(group, std::make_pair(label, std::make_shared<StatsTimer<true>>(auto_start))));
        return result->second.second;
    }

    void AddReport(const std::string& group, const std::string& label, const std::string& content) {
        reports_.insert(std::make_pair(group, std::make_pair(label, content)));
    }

    ~Stats() {
        if (dump_to_log_) {
            std::set<std::string> group_names;
            for (const auto& it : timed_counters_)
                group_names.insert(it.first);
            for (const auto& it : timers_)
                group_names.insert(it.first);
            for (const auto& it : reports_)
                group_names.insert(it.first);
            for(const auto& g : group_names)
                std::cout << PrintGroup(g) << std::endl;
        }
    }

    std::string PrintGroup(const std::string& group_name) {
        std::stringstream ss;
        ss << "[" << group_name << "]" << std::endl;

        auto group_timed_counters = timed_counters_.equal_range(group_name);
        for(auto group_it = group_timed_counters.first; group_it != group_timed_counters.second; group_it++)
            ss << "\t" << PrintTimedCounter(group_it->second.second, group_it->second.first) << std::endl;

        auto group_timers = timers_.equal_range(group_name);
        for(auto group_it = group_timers.first; group_it != group_timers.second; group_it++)
            ss << "\t" << PrintStatsTimer(group_it->second.second, group_it->second.first) << std::endl;
        auto stats = PrintStatsTimerAverage(group_name);
        if (!stats.empty())
            ss<< "\t" << stats << std::endl;

        auto group_reports = reports_.equal_range(group_name);
        for(auto group_it = group_reports.first; group_it != group_reports.second; group_it++)
            ss << "\t" << group_it->second.first << ": " << group_it->second.second << std::endl;
        return ss.str();
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

    std::string PrintStatsTimerAverage(const std::string group_name) {
        std::stringstream ss;
        std::chrono::microseconds::rep sum = 0;
        std::chrono::microseconds::rep mean = 0;
        std::chrono::microseconds::rep sum_deviation = 0;
        size_t count = 0;
        auto group_timers = timers_.equal_range(group_name);
        for(auto group_it = group_timers.first; group_it != group_timers.second; group_it++) {
            sum += group_it->second.second->Microseconds();
            count++;
        }
        if (count < 1) {
            return "";
        }
        mean = sum / count;
        for(auto group_it = group_timers.first; group_it != group_timers.second; group_it++) {
            auto val = group_it->second.second->Microseconds();
            sum_deviation += (val - mean) * (val - mean);
        }
        auto deviation = sqrt(sum_deviation / count);
        ss << "total: " << sum << ", count: " << count << ", avg: " << mean << ", std-dev: " << deviation;
        return ss.str();
    }

private:
    static const bool dump_to_log_ = true;
    std::multimap<std::string, std::pair<std::string, TimedCounterPtr> > timed_counters_;
    std::multimap<std::string, std::pair<std::string, TimerPtr> > timers_;
    std::multimap<std::string, std::pair<std::string, std::string> > reports_;
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
