/*******************************************************************************
 * tests/api/wordcount_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/net/endpoint.hpp>
#include <c7a/c7a.hpp>
#include <c7a/api/bootstrap.hpp>
#include <c7a/common/string.hpp>
#include <examples/word_count_user_program.cpp>

#include <map>

#include "gtest/gtest.h"

using namespace c7a::core;
using namespace c7a::net;

using c7a::api::Context;
using c7a::api::DIARef;

TEST(WordCount, WordCountSmallFileCorrectResults) {

    using WordPair = std::pair<std::string, int>;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
        auto lines = ReadLines(
            ctx,
            "wordcounttest",
            [](const std::string& line) {
                return line;
            });

        auto red_words = word_count_user(lines);

        std::vector<WordPair> words;

        red_words.AllGather(&words);

        auto compare_function = [](WordPair wp1, WordPair wp2) {
            return wp1.first < wp2.first;
        };

        std::sort(words.begin(), words.end(), compare_function);

        ASSERT_EQ(5u, words.size());

        std::string this_str = "this";
        std::string might = "might";
        std::string be = "be";
        std::string a = "a";
        std::string test = "test";

        ASSERT_EQ(std::make_pair(a, 3), words[0]);
        ASSERT_EQ(std::make_pair(be, 1), words[1]);
        ASSERT_EQ(std::make_pair(might, 1), words[2]);
        ASSERT_EQ(std::make_pair(test, 4), words[3]);
        ASSERT_EQ(std::make_pair(this_str, 1), words[4]);
    };

    c7a::api::ExecuteLocalTests(start_func);
}

TEST(WordCount, Generate1024DoesNotCrash) {

    std::random_device random_device;
    std::default_random_engine generator(random_device());
    std::uniform_int_distribution<int> distribution(2, 4);
    size_t workers = distribution(generator);
    size_t port_base = 8080;

    std::function<void(Context&)> start_func =
        [](Context& ctx) { word_count_generated(ctx, 1024); };

    c7a::api::ExecuteLocalThreads(workers, port_base, start_func);
}

TEST(WordCount, ReadBaconDoesNotCrash) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) { word_count(ctx); };

    c7a::api::ExecuteLocalTests(start_func);
}

/******************************************************************************/
