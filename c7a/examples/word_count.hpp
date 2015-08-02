/*******************************************************************************
 * c7a/examples/word_count.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_EXAMPLES_WORD_COUNT_HEADER
#define C7A_EXAMPLES_WORD_COUNT_HEADER

#include <c7a/c7a.hpp>
#include <c7a/common/string.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>

namespace c7a {
namespace examples {

using WordCount = std::pair<std::string, int>;

template <typename InStack>
auto word_count_user(DIARef<std::string, InStack>&input) {
    using WordCount = std::pair<std::string, int>;

    auto word_pairs = input.template FlatMap<WordCount>(
        [](std::string line, auto emit) -> void {
                /* map lambda */
            for (const std::string& word : common::split(line, ' ')) {
                if (word.size() != 0)
                    emit(WordCount(word, 1));
            }
        });

    return word_pairs.ReduceBy(
        [](const WordCount& in) -> std::string {
            /* reduction key: the word string */
            return in.first;
        },
        [](const WordCount& a, const WordCount& b) -> WordCount {
            /* associative reduction operator: add counters */
            return WordCount(a.first, a.second + b.second);
        });
}

//! The WordCount user program
int word_count(Context& ctx) {

    auto lines = ReadLines(
        ctx, "wordcount.in",
        [](const std::string& line) {
            return line;
        });

    auto red_words = word_count_user(lines);

    red_words.Map(
        [](const WordCount& wc) {
            return wc.first + ": " + std::to_string(wc.second) + "\n";
        })
    .WriteToFileSystem(
        "wordcount_" + std::to_string(ctx.rank()) + ".out");

    return 0;
}

int word_count_generated(Context& ctx, size_t size) {

    auto lines = GenerateFromFile(
        ctx, "headwords",
        [](const std::string& line) {
            return line;
        },
        size);

    auto reduced_words = word_count_user(lines);

    reduced_words.Map(
        [](const WordCount& wc) {
            return wc.first + ": " + std::to_string(wc.second) + "\n";
        })
    .WriteToFileSystem(
        "wordcount_" + std::to_string(ctx.rank()) + ".out");
    return 0;
}

} // namespace examples
} // namespace c7a

#endif // !C7A_EXAMPLES_WORD_COUNT_HEADER

/******************************************************************************/
