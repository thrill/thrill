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

#include <c7a/api/generate_from_file.hpp>
#include <c7a/api/read_lines.hpp>
#include <c7a/api/reduce.hpp>
#include <c7a/api/size.hpp>
#include <c7a/api/write_lines_many.hpp>
#include <c7a/common/string.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>

namespace c7a {
namespace examples {

using WordCountPair = std::pair<std::string, size_t>;

//! The WordCount user program: reads a DIA containing std::string words, and
//! returns a DIA containing WordCountPairs.
template <typename InStack>
auto WordCount(const DIARef<std::string, InStack>&input) {

    auto word_pairs = input.template FlatMap<WordCountPair>(
        [](const std::string& line, auto emit) -> void {
                /* map lambda: emit each word */
            for (const std::string& word : common::split(line, ' ')) {
                if (word.size() != 0)
                    emit(WordCountPair(word, 1));
            }
        });

    return word_pairs.ReduceBy(
        [](const WordCountPair& in) -> std::string {
            /* reduction key: the word string */
            return in.first;
        },
        [](const WordCountPair& a, const WordCountPair& b) -> WordCountPair {
            /* associative reduction operator: add counters */
            return WordCountPair(a.first, a.second + b.second);
        });
}

size_t WordCountBasic(Context& ctx) {

    auto lines = ReadLines(ctx, "wordcount.in");

    auto red_words = WordCount(lines);

    red_words.Map(
        [](const WordCountPair& wc) {
            return wc.first + ": " + std::to_string(wc.second) + "\n";
        })
    .WriteLinesMany(
        "wordcount_" + std::to_string(ctx.rank()) + ".out");

    return 0;
}

size_t WordCountGenerated(Context& ctx, size_t size) {

    auto lines = GenerateFromFile(
        ctx, "headwords",
        [](const std::string& line) {
            return line;
        },
        size);

    auto reduced_words = WordCount(lines);

    reduced_words.Map(
        [](const WordCountPair& wc) {
            return wc.first + ": " + std::to_string(wc.second) + "\n";
        })
    .WriteLinesMany(
        "wordcount_" + std::to_string(ctx.rank()) + ".out");

    return 42;
}

} // namespace examples
} // namespace c7a

#endif // !C7A_EXAMPLES_WORD_COUNT_HEADER

/******************************************************************************/
