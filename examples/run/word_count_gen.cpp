/*******************************************************************************
 * examples/word_count_gen.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/common/string.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/groupby_index.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <examples/word_count.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <iostream>

using namespace thrill; // NOLINT

using WordCountPair = std::pair<std::string, size_t>;

int main(int argc, char *argv[]) {

    common::CmdlineParser clp;

    clp.SetVerboseProcess(false);

    unsigned int elements = 1000;
    clp.AddUInt('s', "elements", "S", elements,
                "Create wordcount example with S generated words");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func = [elements](api::Context &ctx) {

        auto lines = GenerateFromFile(
                ctx, "inputs/headwords",
                [](const std::string &line) {
                    return line;
                },
                elements);

        auto reduced_words = examples::WordCount(lines);

        reduced_words.Map([](const WordCountPair &wc) {
            return wc.first + ": " + std::to_string(wc.second);
        }).WriteLinesMany("outputs/wordcount-");

        return 0;
    };

    return api::Run(start_func);
}

/******************************************************************************/
