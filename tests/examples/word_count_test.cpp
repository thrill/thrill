/*******************************************************************************
 * tests/examples/word_count_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/word_count/word_count.hpp>

#include <thrill/api/allgather.hpp>
#include <thrill/api/distribute_from.hpp>
#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/string.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;

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

            auto lines = DistributeFrom(ctx, input_vec, 0);

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

    api::RunLocalTests(start_func);
}

TEST(WordCount, Generate1024DoesNotCrash) {

    using examples::WordCountPair;

    size_t size = 1024;

    std::function<void(Context&)> start_func =
        [&size](Context& ctx) {
            ctx.enable_consume();

            auto lines = GenerateFromFile(
                ctx, "inputs/headwords",
                [](const std::string& line) {
                    return line;
                },
                size);

            auto reduced_words = examples::WordCount(lines);

            reduced_words.Map(
                [](const WordCountPair& wc) {
                    return wc.first + ": " + std::to_string(wc.second);
                })
            .WriteLinesMany(
                "outputs/wordcount-");
        };

    api::RunLocalTests(start_func);
}

TEST(WordCount, ReadBaconDoesNotCrash) {

    using examples::WordCountPair;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            ctx.enable_consume();

            auto lines = ReadLines(ctx, "inputs/wordcount.in");

            auto red_words = examples::WordCount(lines);

            red_words.Map(
                [](const WordCountPair& wc) {
                    return wc.first + ": " + std::to_string(wc.second);
                })
            .WriteLinesMany(
                "outputs/wordcount-");
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
