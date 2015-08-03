/*******************************************************************************
 * tests/api/wordcount_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/api/allgather.hpp>
#include <c7a/api/scatter.hpp>
#include <c7a/common/string.hpp>
#include <c7a/examples/word_count.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace c7a;

TEST(WordCount, WordCountSmallFileCorrectResults) {

    using examples::WordCountPair;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::vector<std::string> input_vec = {
                "test",
                "this",
                "might be",
                "a test",
                "a test",
                "a test"
            };

            auto lines = Scatter(ctx, input_vec);

            auto red_words = examples::WordCount(lines);

            std::vector<WordCountPair> words = red_words.AllGather();

            auto compare_function = [](WordCountPair wp1, WordCountPair wp2) {
                                        return wp1.first < wp2.first;
                                    };

            std::sort(words.begin(), words.end(), compare_function);

            ASSERT_EQ(5u, words.size());

            std::string this_str = "this";
            std::string might = "might";
            std::string be = "be";
            std::string a = "a";
            std::string test = "test";

            ASSERT_EQ(WordCountPair(a, 3), words[0]);
            ASSERT_EQ(WordCountPair(be, 1), words[1]);
            ASSERT_EQ(WordCountPair(might, 1), words[2]);
            ASSERT_EQ(WordCountPair(test, 4), words[3]);
            ASSERT_EQ(WordCountPair(this_str, 1), words[4]);
        };

    api::ExecuteLocalTests(start_func);
}

TEST(WordCount, Generate1024DoesNotCrash) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) { examples::WordCountGenerated(ctx, 1024); };

    api::ExecuteLocalTests(start_func);
}

TEST(WordCount, ReadBaconDoesNotCrash) {
    std::function<void(Context&)> start_func =
        [](Context& ctx) { examples::WordCountBasic(ctx); };

    api::ExecuteLocalTests(start_func);
}

/******************************************************************************/
