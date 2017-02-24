/*******************************************************************************
 * examples/suffix_sorting/rl_bwt.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Simon Gog <gog@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/equal_to_dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/ring_buffer.hpp>
#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

//! A pair (rank, index)
template <typename IndexType, typename CharType>
struct IndexChar {
    IndexType index;
    CharType  c;

    friend std::ostream& operator << (
        std::ostream& os, const IndexChar& ri) {
        return os << '(' << ri.index << '|' << ri.c << ')';
    }
} TLX_ATTRIBUTE_PACKED;

template <typename InputDIA>
auto ConstructRLBWT(const InputDIA &input_dia) {

    Context& ctx = input_dia.ctx();

    using ValueType = typename InputDIA::ValueType;
    using PairIC = IndexChar<uint8_t, ValueType>;

    size_t input_size = input_dia.Size();

    if (input_size < 2) {   // handle special case
        std::vector<uint8_t> length(input_size, 1);
        DIA<uint8_t> rl = EqualToDIA(ctx, length);
        return input_dia.Zip(rl,
                             [](const ValueType& c, const uint8_t& i) {
                                 return PairIC { i, c };
                             });
    }

    auto rl_bwt = input_dia.template FlatWindow<PairIC>(DisjointTag, 256, [input_size](size_t, const std::vector<ValueType>& v, auto emit) {
                                                            size_t i = 0;
                                                            size_t run_start = 0;
                                                            while (++i < v.size()) {
                                                                if (v[i - 1] != v[i]) {
                                                                    emit(PairIC { static_cast<uint8_t>(i - run_start - 1), v[i - 1] });
                                                                    run_start = i;
                                                                }
                                                            }
                                                            emit(PairIC { static_cast<uint8_t>(i - run_start - 1), v[i - 1] });
                                                        });
    return rl_bwt;
}

int main(int argc, char* argv[]) {
    tlx::CmdlineParser cp;

    cp.set_author("Simon Gog <gog@kit.edu>");

    std::string input_path;
    size_t output_result = 0;

    cp.add_opt_param_string("input", input_path,
                            "Path to input file.");
    cp.add_opt_param_size_t("output_result", output_result,
                            "Output result.");

    if (!cp.process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            if (input_path.size()) {
                auto input_dia = ReadBinary<uint8_t>(ctx, input_path);
                auto output_dia = ConstructRLBWT(input_dia);
                if (output_result)
                    output_dia.Print("rl_bwt");
                sLOG1 << "RLE size = " << output_dia.Size();
            }
            else {
                std::string bwt = "aaaaaaaaaaabbbbaaaaaaaccccdddaacacaffatttttttttttyyyyaaaaa";
                auto input_dia =
                    Generate(ctx, bwt.size(),
                             [&](size_t i) { return (uint8_t)bwt[i]; });
                auto output_dia = ConstructRLBWT(input_dia);
                if (output_result)
                    output_dia.Print("rl_bwt");
            }
        });
}

/******************************************************************************/
