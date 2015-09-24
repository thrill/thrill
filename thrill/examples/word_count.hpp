/*******************************************************************************
 * thrill/examples/word_count.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_WORD_COUNT_HEADER
#define THRILL_EXAMPLES_WORD_COUNT_HEADER

#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>

namespace thrill {
namespace examples {

using WordCountPair = std::pair<std::string, size_t>;

//! The WordCount user program: reads a DIA containing std::string words, and
//! returns a DIA containing WordCountPairs.
template <typename InStack>
auto WordCount(const DIARef<std::string, InStack>&input) {

    auto word_pairs = input.template FlatMap<WordCountPair>(
        [](const std::string& line, auto emit) -> void {
                /* map lambda: emit each word */
            for (const std::string& word : common::Split(line, ' ')) {
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

    auto lines = ReadLines(ctx, "inputs/wordcount.in");

    auto red_words = WordCount(lines);

    red_words.Map(
        [](const WordCountPair& wc) {
            return wc.first + ": " + std::to_string(wc.second);
        })
    .WriteLinesMany(
        "outputs/wordcount-");

    return 0;
}

size_t WordCountGenerated(Context& ctx, size_t size) {

    auto lines = GenerateFromFile(
        ctx, "inputs/headwords",
        [](const std::string& line) {
            return line;
        },
        size);

    auto reduced_words = WordCount(lines);

    reduced_words.Map(
        [](const WordCountPair& wc) {
            return wc.first + ": " + std::to_string(wc.second);
        })
    .WriteLinesMany(
        "outputs/wordcount-");

    return 42;
}

} // namespace examples
} // namespace thrill

#endif // !THRILL_EXAMPLES_WORD_COUNT_HEADER

/******************************************************************************/
