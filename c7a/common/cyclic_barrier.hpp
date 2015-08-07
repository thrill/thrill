/*******************************************************************************
 * c7a/common/cyclic_barrier.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_CYCLIC_BARRIER_HEADER
#define C7A_COMMON_CYCLIC_BARRIER_HEADER

#include <c7a/common/atomic_movable.hpp>
#include <condition_variable>
#include <mutex>

namespace c7a {
namespace common {

/**
 * @brief Implements a cyclic barrier that can be shared between threads.
 */
class Barrier
{

private:
    common::MutexMovable m;
    common::ConditionVariableAnyMovable event;
    const int threadCount;
    int counts[2];
    int current;

public:
    /**
     * @brief Creates a new barrier that waits for n threads.
     *
     * @param n The count of threads to wait for.
     */
    explicit Barrier(int n) : threadCount(n), current(0) {
        counts[0] = 0;
        counts[1] = 0;
    }

    /**
     * @brief Waits for n threads to arrive.
     * @details This method blocks and returns as soon as n threads are waiting inside the method.
     */
    void Await() {
        std::atomic_thread_fence(std::memory_order_release);
        m.lock();
        int localCurrent = current;
        counts[localCurrent]++;

        if (counts[localCurrent] < threadCount) {
            while (counts[localCurrent] < threadCount) {
                event.wait(m);
            }
        }
        else {
            current = current ? 0 : 1;
            counts[current] = 0;
            event.notify_all();
        }
        m.unlock();
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_CYCLIC_BARRIER_HEADER

/******************************************************************************/
