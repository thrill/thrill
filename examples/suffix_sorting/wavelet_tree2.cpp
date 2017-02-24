/*******************************************************************************
 * examples/suffix_sorting/wavelet_tree2.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Simon Gog <gog@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

static constexpr bool debug = true;

using namespace thrill; // NOLINT

template <typename InputDIA>
auto ConstructWaveletTree(const InputDIA &input_dia) {

    uint64_t max_value = input_dia.Max();
    sLOG << "max_value" << max_value;

    uint64_t level = common::IntegerLog2Ceil(max_value);
    uint64_t mask = (~uint64_t(0)) << level;
    uint64_t maskbit = uint64_t(1) << level;

    using PairBI = std::pair<uint8_t, uint64_t>;

    auto wt = input_dia.template FlatMap<PairBI>(
        [level](const uint64_t& x, auto emit) {
            for (size_t i = 0; i <= level; ++i) {
                emit(std::make_pair(i, x));
            }
        });
    auto wt2 = wt.Sort([mask](const PairBI& a, const PairBI& b) {
                           if (a.first != b.first) {
                               return a.first < b.first;
                           }
                           else {
                               return (a.second & (mask >> a.first)) < (b.second & (mask >> a.first));
                           }
                       });

    if (debug)
        wt2.Map([](const PairBI& x) {
                    return std::to_string(x.first) + " " + std::to_string(x.second);
                }).Print("wt");

    auto binary_wt = wt2.Window(
        DisjointTag, 64,
        [maskbit](size_t, const std::vector<PairBI>& v) {
            uint64_t x = 0;
            for (size_t i = 0; i < v.size(); ++i) {
                uint64_t b = (v[i].second & ((maskbit) >> v[i].first)) != 0;
                x |= (b << i);
            }
            return x;
        });

    if (debug)
        binary_wt.Print("BINARY_WT");

    binary_wt.WriteBinary("BINARY_WT");
}

int main(int argc, char* argv[]) {
    tlx::CmdlineParser cp;

    cp.set_author("Timo Bingmann <tb@panthema.net>");
    cp.set_author("Simon Gog <gog@kit.edu>");

    std::string input_path;

    cp.add_opt_param_string("input", input_path,
                            "Path to input file.");

    if (!cp.process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            if (input_path.size()) {
                auto input_dia = ReadBinary<uint64_t>(ctx, input_path);
                ConstructWaveletTree(input_dia);
            }
            else {
                std::default_random_engine rng(std::random_device { } ());
                auto input_dia =
                    Generate(ctx, 32,
                             [&](size_t) { return uint64_t(rng() % 32); });
                ConstructWaveletTree(input_dia);
            }
        });
}

/******************************************************************************/
