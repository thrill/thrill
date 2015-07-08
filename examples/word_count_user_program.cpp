/*******************************************************************************
 * examples/word_count_user_program.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/common/string.hpp>
#include <c7a/c7a.hpp>

using c7a::Context;
using c7a::DIARef;

template <typename InStack>
auto word_count_user(DIARef<std::string, std::string, InStack>& input) {

    using WordCount = std::pair<std::string, int>;

    auto word_pairs = input.FlatMap(
        [](std::string line, auto emit) {
            /* map lambda */
            for (const std::string& word : c7a::common::split(line, ' ')) {
                if (word.size() != 0)
                    emit(WordCount(word, 1));
            }
        });

    return word_pairs.ReduceBy(
        [](const WordCount& in) {
            /* reduction key: the word string */
            return in.first;
        },
        [](WordCount a, WordCount b) {
            /* associative reduction operator: add counters */
            return WordCount(a.first, a.second + b.second);
        });
}

//! The WordCount user program
int word_count(Context& ctx) {
    using WordCount = std::pair<std::string, int>;

    auto lines = ReadLines(
        ctx,
        "wordcount.in",
        [](const std::string& line) {
            return line;
        });

    auto red_words = word_count_user(lines);

    red_words.WriteToFileSystem(
        "wordcount_" + std::to_string(ctx.rank()) + ".out",
        [](const WordCount& item) {
            return item.first + ": " + std::to_string(item.second);
        });

    return 0;
}

int word_count_generated(Context& ctx, size_t size) {
    using WordCount = std::pair<std::string, int>;

    auto lines = GenerateFromFile(
        ctx,
        "headwords",
        [](const std::string& line) {
            return line;
        },
        size);

    auto reduced_words = word_count_user(lines);

    reduced_words.WriteToFileSystem(
        "wordcount_" + std::to_string(ctx.rank()) + ".out",
        [](const WordCount& item) {
            return item.first + ": " + std::to_string(item.second);
        });
    return 0;
}

/******************************************************************************/
