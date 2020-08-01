/*******************************************************************************
 * examples/inverted_index/inverted_index.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2018 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_INVERTED_INDEX_INVERTED_INDEX_HEADER
#define THRILL_EXAMPLES_INVERTED_INDEX_INVERTED_INDEX_HEADER

#include <examples/word_count/random_text_writer.hpp>

#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/common/string_view.hpp>

#include <iostream>
#include <string>
#include <utility>
#include <string>
#include <vector>
#include <algorithm>
#include <sstream>
#include <iterator>
#include <random>

using thrill::common::StringView;
using thrill::common::SplitView;

namespace examples {
namespace inverted_index {

using namespace thrill; // NOLINT

// ((word, doc), count)
using WordDocPair = std::pair<std::string, std::string>;
using WordDocCountPair = std::pair<WordDocPair, size_t>;

// (word, (doc, count))
using WordDocPairMapped = std::pair<std::string, size_t>;
using WordDocCountPairMapped = std::pair<std::string, WordDocPairMapped>;

using InvIndexDocs = std::vector<WordDocPairMapped>;
using InvIndexWord = std::pair<std::string, InvIndexDocs>;

// todo: represent document with size_t instead of std::string
std::string random_string(size_t length)
{
    auto randchar = []() -> char
    {
        const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
        const size_t max_index = (sizeof(charset) - 1);
        return charset[rand() % max_index];
    };
    std::string str(length, 0);
    std::generate_n(str.begin(), length, randchar);
    return str;
}

void InvertedIndex(thrill::Context& ctx,
               std::string output, size_t num_docs, size_t num_words) {
    std::default_random_engine rng(std::random_device { } ());

    auto lines = Generate(
        ctx, num_docs,
        [&](size_t /* index */) {
            return examples::word_count::RandomTextWriterGenerate(num_words, rng);
        });

    auto dia1 =
        lines
        .template FlatMap<WordDocPair>(
            [&](const std::string& line, auto emit) {
                std::string docName = random_string(10);
                SplitView(line, ' ', [&](StringView sv) {
                    emit(WordDocPair(sv.ToString(), docName));
                });
            });

    // create a tuple with count 1 => ((word, doc), 1)
    auto dia2 = dia1.Map([](const WordDocPair& wordDoc) { 
        return WordDocCountPair(wordDoc, 1); });

    // group all (word, doc) pairs and sum the counts => ((word, doc), count)
    auto key_fn = [](const WordDocCountPair& a) { 
        return a.first.first + a.first.second; };
    auto red_fn = [](const WordDocCountPair& a, 
        const WordDocCountPair& b) { 
        return WordDocCountPair(a.first, a.second + b.second); };
    auto dia3 = dia2.ReduceByKey(key_fn, red_fn);

    // transform tuple => (word, (doc, count))
    auto dia4 = dia3.Map([](const WordDocCountPair& wordDoc) { 
        return WordDocCountPairMapped(wordDoc.first.first, 
            WordDocPairMapped(wordDoc.first.second, wordDoc.second)); });

    // group by words
    auto dia5 = dia4.GroupByKey<InvIndexWord>(
        [](const WordDocCountPairMapped& p) -> std::string { 
            return p.first; 
        },
        [](auto& r, const std::string word) {
            std::vector<WordDocPairMapped> docs;
            while (r.HasNext()) {
                docs.push_back(r.Next().second);
            }
            return std::make_pair(word, docs);
        });

    dia5.Map([](const InvIndexWord& rp) {
        std::ostringstream oss;
        for (auto &i : rp.second) {
            oss << "(" << i.first << ", " << i.second << ") ";
        }
        return rp.first + ": " + oss.str();
    }).WriteLines(output);
}

} // namespace inverted_index
} // namespace examples

#endif // !THRILL_EXAMPLES_INVERTED_INDEX_INVERTED_INDEX_HEADER

/******************************************************************************/
