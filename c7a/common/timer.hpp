/*******************************************************************************
 * c7a/common/timer.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_TIMER_HEADER
#define C7A_COMMON_TIMER_HEADER

#include <ctime>
#include <ratio>
#include <chrono>

class timer
{
public:
    timer() {
        t_duration = std::chrono::duration<double>(0);
    }

    timer& operator += (const timer& rhs) {
        t_duration += rhs.t_duration;
        return *this;
    }

    timer operator + (const timer& rhs) {
        timer t;
        t.t_start = t_start < rhs.t_start ? t_start : rhs.t_start;
        t.t_duration = t_duration + rhs.t_duration;
        return t;
    }

    void start() {
        t_start = timestamp();
    }

    void stop() {
        t_duration += std::chrono::duration_cast<std::chrono::duration<double> >(timestamp() - t_start);
    }

    void reset() {
        t_duration = std::chrono::duration<double>(0);
    }

    double get_time() {
        return t_duration.count();
    }

private:
    inline std::chrono::high_resolution_clock::time_point timestamp() {
        return std::chrono::high_resolution_clock::now();
    }

    std::chrono::high_resolution_clock::time_point t_start;
    std::chrono::duration<double> t_duration;
};

#endif // !C7A_COMMON_TIMER_HEADER

/******************************************************************************/
