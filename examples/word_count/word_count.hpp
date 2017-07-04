/*******************************************************************************
 * examples/word_count/word_count.hpp
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
#ifndef THRILL_EXAMPLES_WORD_COUNT_WORD_COUNT_HEADER
#define THRILL_EXAMPLES_WORD_COUNT_WORD_COUNT_HEADER

#include <thrill/api/reduce_by_key.hpp>
#include <thrill/common/string_view.hpp>

#include <string>
#include <utility>

namespace examples {
namespace word_count {

using namespace thrill; // NOLINT

using WordCountPair = std::pair<std::string, size_t>;

//! The most basic WordCount user program: reads a DIA containing std::string
//! words, and returns a DIA containing WordCountPairs.
template <typename InputStack>
auto WordCount(const DIA<std::string, InputStack>& input) {

    auto word_pairs = input.template FlatMap<WordCountPair>(
        [](const std::string& line, auto emit) -> void {
            /* map lambda: emit each word */
            common::SplitView(
                line, ' ', [&](const common::StringView& sv) {
                    if (sv.size() == 0) return;
                    emit(WordCountPair(sv.ToString(), 1));
                });
        });

    return word_pairs.ReduceByKey(
        [](const WordCountPair& in) -> std::string {
            /* reduction key: the word string */
            return in.first;
        },
        [](const WordCountPair& a, const WordCountPair& b) -> WordCountPair {
            /* associative reduction operator: add counters */
            return WordCountPair(a.first, a.second + b.second);
        });
}

/******************************************************************************/

using HashWord = std::pair<size_t, std::string>;
using HashWordCount = std::pair<HashWord, size_t>;

struct HashWordHasher {
    size_t operator () (const HashWord& w) const {
        // return first which is the hash of the word
        return w.first;
    }
};

//! The second WordCount user program: reads a DIA containing std::string words,
//! creates hash values from the words prior to reducing by hash and
//! word. Returns a DIA containing WordCountPairs.
template <typename InputStack>
auto HashWordCountExample(const DIA<std::string, InputStack>& input) {

    std::hash<std::string> string_hasher;

    auto r =
        input
        .template FlatMap<std::string>(
            [](const std::string& line, auto emit) {
                /* map lambda: emit each word */
                common::SplitView(
                    line, ' ', [&](const common::StringView& sv) {
                        if (sv.size() == 0) return;
                        emit(sv.ToString());
                    });
            })
        .Map([&](const std::string& word) {
                 return HashWordCount(HashWord(string_hasher(word), word), 1);
             })
        .ReduceByKey(
            [](const HashWordCount& in) {
                /* reduction key: the word string */
                return in.first;
            },
            [](const HashWordCount& a, const HashWordCount& b) {
                /* associative reduction operator: add counters */
                return HashWordCount(a.first, a.second + b.second);
            },
            api::DefaultReduceConfig(), HashWordHasher())
        .Map([](const HashWordCount& in) {
                 return WordCountPair(in.first.second, in.second);
             });
    return r;
}

} // namespace word_count
} // namespace examples

#endif // !THRILL_EXAMPLES_WORD_COUNT_WORD_COUNT_HEADER

/******************************************************************************/
