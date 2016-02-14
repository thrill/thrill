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

static const std::vector<examples::WordCountPair> bacon_ipsum_correct = {
    { "alcatra", 32 }, { "amet", 4 }, { "andouille", 16 }, { "bacon", 36 },
    { "ball", 16 }, { "beef", 40 }, { "belly", 24 }, { "biltong", 24 },
    { "boudin", 12 }, { "bresaola", 12 }, { "brisket", 24 }, { "capicola", 24 },
    { "chicken", 4 }, { "chop", 20 }, { "chuck", 24 }, { "corned", 16 },
    { "cow", 8 }, { "cupim", 20 }, { "dolor", 4 }, { "doner", 32 },
    { "drumstick", 20 }, { "fatback", 28 }, { "filet", 12 }, { "flank", 28 },
    { "frankfurter", 12 }, { "ground", 8 }, { "ham", 40 }, { "hamburger", 16 },
    { "hock", 8 }, { "ipsum", 4 }, { "jerky", 28 }, { "jowl", 28 },
    { "kevin", 36 }, { "kielbasa", 20 }, { "landjaeger", 32 },
    { "leberkas", 24 }, { "loin", 12 }, { "meatball", 12 }, { "meatloaf", 28 },
    { "mignon", 12 }, { "pancetta", 24 }, { "pastrami", 16 }, { "picanha", 24 },
    { "pig", 20 }, { "porchetta", 28 }, { "pork", 64 }, { "prosciutto", 24 },
    { "ribeye", 20 }, { "ribs", 32 }, { "round", 8 }, { "rump", 40 },
    { "salami", 20 }, { "sausage", 16 }, { "shank", 12 }, { "shankle", 4 },
    { "short", 16 }, { "shoulder", 12 }, { "sirloin", 8 }, { "spare", 8 },
    { "steak", 8 }, { "strip", 8 }, { "swine", 16 }, { "t-bone", 16 },
    { "tail", 28 }, { "tenderloin", 20 }, { "tip", 16 }, { "tongue", 12 },
    { "tri-tip", 28 }, { "turducken", 16 }, { "turkey", 20 }, { "venison", 20 }
};

TEST(WordCount, BaconIpsum) {

    using examples::WordCountPair;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            ctx.enable_consume();

            auto lines = ReadLines(ctx, "inputs/wordcount.in");

            std::vector<WordCountPair> result =
                examples::WordCount(lines).AllGather();

            // sort result, because reducing delivers any order
            std::sort(result.begin(), result.end());

            ASSERT_EQ(bacon_ipsum_correct, result);
        };

    api::RunLocalTests(start_func);
}

TEST(WordCount, BaconIpsumFastString) {

    using examples::FastWordCountPair;

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            ctx.enable_consume();

            auto lines = ReadLines(ctx, "inputs/wordcount.in");

            std::vector<FastWordCountPair> result =
                examples::FastWordCount(lines).AllGather();

            // sort result, because reducing delivers any order
            std::sort(result.begin(), result.end());

            ASSERT_EQ(result.size(), bacon_ipsum_correct.size());
            for (size_t i = 0; i < result.size(); ++i) {
                ASSERT_EQ(result[i].first, bacon_ipsum_correct[i].first);
                ASSERT_EQ(result[i].second, bacon_ipsum_correct[i].second);
            }
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
