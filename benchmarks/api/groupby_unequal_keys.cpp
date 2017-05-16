/*******************************************************************************
 * benchmarks/api/groupby_unequal_keys.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/dia.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/stats_timer.hpp>
#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <limits>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

// using thrill::DIARef;
using thrill::Context;

using namespace thrill; // NOLINT

int main(int argc, char* argv[]) {
    tlx::CmdlineParser clp;

    int n;
    clp.add_param_int("n", n, "Iterations");

    std::string input;
    clp.add_param_string("input", input,
                         "input file pattern");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    auto start_func =
        [n, &input](api::Context& ctx) {

            auto modulo_keyfn = [](size_t in) {
                                    if (in < std::numeric_limits<size_t>::max() / 5) {
                                        return (size_t)0;
                                    }
                                    return (size_t)(in % 100);
                                };

            auto median_fn = [](auto& r, std::size_t) {
                                 std::vector<std::size_t> all;
                                 while (r.HasNext()) {
                                     all.push_back(r.Next());
                                 }
                                 std::sort(std::begin(all), std::end(all));
                                 return all[all.size() / 2 - 1];
                             };

            auto in = api::ReadBinary<size_t>(ctx, input).Keep();
            in.Size();

            // group by to compute median
            thrill::common::StatsTimerStart timer;
            for (int i = 0; i < n; i++) {
                in.GroupByKey<size_t>(modulo_keyfn, median_fn).Size();
            }
            timer.Stop();

            LOG1 << "\n"
                 << "RESULT"
                 << " name=total"
                 << " rank=" << ctx.my_rank()
                 << " time=" << static_cast<double>(timer.Milliseconds()) / static_cast<double>(n)
                 << " filename=" << input;
        };

    return api::Run(start_func);
}

/******************************************************************************/
