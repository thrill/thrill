/*******************************************************************************
 * examples/word_count.hpp
 *
 * This file contains the WordCount core example. See word_count_run.cpp for how
 * to run it on different inputs.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_WORD_COUNT_HEADER
#define THRILL_EXAMPLES_WORD_COUNT_HEADER

#include <thrill/api/reduce.hpp>
#include <thrill/common/string.hpp>

#include <string>
#include <utility>

namespace examples {

using namespace thrill; // NOLINT

using WordCountPair = std::pair<std::string, size_t>;

//! The WordCount user program: reads a DIA containing std::string words, and
//! returns a DIA containing WordCountPairs.
template <typename InStack>
auto WordCount(const DIA<std::string, InStack>&input) {

    auto word_pairs = input.template FlatMap<WordCountPair>(
        [](const std::string& line, auto emit) -> void {
            /* map lambda: emit each word */
            common::SplitCallback(
                line, ' ', [&](const auto begin, const auto end)
                { emit(WordCountPair(std::string(begin, end), 1)); });
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

} // namespace examples

#endif // !THRILL_EXAMPLES_WORD_COUNT_HEADER

/******************************************************************************/
