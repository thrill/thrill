/*******************************************************************************
 * tests/common/timed_counter_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/timed_counter.hpp>

#include <gtest/gtest.h>

#include <thread>

using namespace thrill::common;

TEST(TimedCounter, TriggerIncreasesCount) {
    TimedCounter candidate;

    auto a = candidate.Count();
    candidate.Trigger();
    auto b = candidate.Count();
    ASSERT_GT(b, a);
}

TEST(TimedCounter, TriggerCreatesOccurence) {
    TimedCounter candidate;

    candidate.Trigger();
    ASSERT_EQ(1u, candidate.Occurences().size());
}

TEST(TimedCounter, OccurencesAreOrderedAscending) {
    using namespace std::literals;
    TimedCounter candidate;

    candidate.Trigger();
    std::this_thread::sleep_for(1ms);
    candidate.Trigger();
    auto occurences = candidate.Occurences();
    ASSERT_TRUE(occurences[0] < occurences[1]);
}

TEST(TimedCounter, OccurencesAreOrderedAscendingAfterMerging) {
    using namespace std::literals;
    TimedCounter candidateA;
    TimedCounter candidateB;

    candidateA.Trigger();
    std::this_thread::sleep_for(1ms);
    candidateB.Trigger();
    std::this_thread::sleep_for(1ms);
    candidateA.Trigger();

    auto merged = candidateA + candidateB;
    auto occurences = merged.Occurences();

    ASSERT_TRUE(occurences[0] < occurences[1]);
    ASSERT_TRUE(occurences[1] < occurences[2]);
}

/******************************************************************************/
