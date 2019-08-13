/*******************************************************************************
 * examples/word_count/word_count_sequential.cpp
 *
 * A simple sequential word count implementation using std::unordered_map.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/stats_timer.hpp>
#include <tlx/string/split_view.hpp>

#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {
    if (argc == 1) {
        std::cerr << "Usage: " << argv[0] << " <files>" << std::endl;
        return 0;
    }

    common::StatsTimerStart timer;

    std::unordered_map<std::string, size_t> count_map;

    for (int argi = 1; argi < argc; ++argi) {

        std::ifstream in(argv[argi]);
        if (!in.good()) {
            std::cerr << "Could not open " << argv[argi] << std::endl;
            abort();
        }

        std::string line;
        while (std::getline(in, line, '\n'))
        {
            tlx::split_view(
                ' ', line, [&](const tlx::string_view& sv) {
                    if (sv.size() == 0) return;
                    ++count_map[sv.to_string()];
                });
        }
    }

    std::cerr << "word_counting done: " << timer << " s" << std::endl;

    for (auto& p : count_map) {
        std::cout << p.first << ": " << p.second << std::endl;
    }

    std::cerr << "after output: " << timer << " s" << std::endl;

    return 0;
}

/******************************************************************************/
