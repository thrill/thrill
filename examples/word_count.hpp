/*******************************************************************************
 * examples/word_count.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/thrill.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <iostream>

using thrill::DIA;
using thrill::Context;

using namespace thrill; // NOLINT

namespace examples {

using WordCountPair = std::pair<std::string, size_t>;

//! The WordCount user program: reads a DIA containing std::string words, and
//! returns a DIA containing WordCountPairs.
template <typename InStack>
auto WordCount(const DIA<std::string, InStack> &input) {

    auto word_pairs = input.template FlatMap<WordCountPair>(
            [](const std::string &line, auto emit) -> void {
                /* map lambda: emit each word */
                thrill::common::SplitCallback(
                    line, ' ', [&](const auto begin, const auto end)
                    { emit(WordCountPair(std::string(begin, end), 1)); });
            });

    return word_pairs.ReduceBy(
            [](const WordCountPair &in) -> std::string {
                /* reduction key: the word string */
                return in.first;
            },
            [](const WordCountPair &a, const WordCountPair &b) -> WordCountPair {
                /* associative reduction operator: add counters */
                return WordCountPair(a.first, a.second + b.second);
            });
}

}

/******************************************************************************/
