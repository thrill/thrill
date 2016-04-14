/*******************************************************************************
 * examples/suffix_sorting/wavelet_tree.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
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
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace examples {
namespace suffix_sorting {

static constexpr bool debug = false;

using namespace thrill; // NOLINT

template <typename InputDIA>
auto ConstructWaveletTree(const InputDIA &input_dia) {
    uint64_t max_value = input_dia.Keep().Max();
    sLOG << "max_value" << max_value;

    uint64_t level = common::IntegerLog2Ceil(max_value);
    uint64_t mask = (~uint64_t(0)) << level;
    uint64_t maskbit = uint64_t(1) << level;

    DIA<uint8_t> wt = input_dia.Collapse();
//    if (debug) wt.Print("wt");

    while (mask != (~uint64_t(0))) {
        // switch to next level
        --level;
        mask = (mask >> 1) | 0x8000000000000000llu;
        maskbit >>= 1;

        sLOG << "maskbit" << maskbit << "mask" << std::hex << mask;

        wt.Keep().Window(DisjointTag, 64, [maskbit](size_t, const std::vector<uint8_t>& v)
            {
                uint64_t x = 0;
                for(size_t i=0; i < v.size(); ++i){
                    bool b = ((bool)(v[i] & maskbit));
                    if ( b ) {
                        x |= b << i;
                    }
                }
                return x;
            })
        .WriteBinary(common::str_sprintf("wt-bin-%02u-", unsigned(level)));

        wt = wt.Sort(
            [mask](const uint8_t& a, const uint8_t& b) {
                return (a & mask) < (b & mask);
            });

        if (debug) wt.Print("wt");
    }
}

} // namespace suffix_sorting
} // namespace examples

int main(int argc, char* argv[]) {

    using namespace thrill; // NOLINT
    using namespace examples::suffix_sorting;

    common::CmdlineParser cp;

    cp.SetAuthor("Timo Bingmann <tb@panthema.net>");

    std::string input_path;

    cp.AddOptParamString("input", input_path,
                         "Path to input file.");

    if (!cp.Process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            ctx.enable_consume();

            if (input_path.size()) {
                auto input_dia = ReadBinary<uint8_t>(ctx, input_path);
                ConstructWaveletTree(input_dia);
            }
            else {
                std::string bwt = "aaaaaaaaaaabbbbaaaaaaaccccdddaacacaffatttttttttttyyyyaaaaa";
                auto input_dia =
                    Generate(ctx,
                             [&](size_t i) { return (uint8_t)bwt[i]; },
                             bwt.size());
                ConstructWaveletTree(input_dia);
            }
        });
}

/******************************************************************************/
