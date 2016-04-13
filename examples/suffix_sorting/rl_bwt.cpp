/*******************************************************************************
 * examples/suffix_sorting/wavelet_tree2.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Simon Gog <gog@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/collapse.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/print.hpp>
#include <thrill/common/ring_buffer.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/equal_to_dia.hpp>
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

static constexpr bool debug = false;

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
} THRILL_ATTRIBUTE_PACKED;



template <typename InputDIA>
auto ConstructRLBWT(const InputDIA &input_dia) {

    Context& ctx = input_dia.ctx();

    using ValueType = typename InputDIA::ValueType;
    using PairIC    = IndexChar<size_t, ValueType>;
   
    size_t input_size = input_dia.Size();

    if ( input_size < 2 ) { // handle special case
        std::vector<size_t> length(input_size, 1);
        DIA<size_t> rl = EqualToDIA(ctx, length);
        return input_dia.Zip(rl,
                [](const ValueType&c, const size_t& i){
                    return PairIC{i, c};
                });
    }

    DIA<size_t> indices = Generate(ctx,
        [](size_t index) { return index; },
        input_size);

    auto rl_bwt = input_dia.template FlatWindow<PairIC>(2, [input_size](size_t index, const common::RingBuffer<ValueType>& rb, auto emit){
                if ( index + 2 == input_size ) {
                    if ( rb[0] != rb[1] ) {
                        emit(PairIC{index,rb[0]});
                    }
                    emit(PairIC{index+1,rb[1]});
                } else if ( rb[0] != rb[1] ) {
                    emit(PairIC{index,rb[0]});
                }
            });

    if ( debug )
        rl_bwt.Print("rl_bwt");

    auto rl_bwt_size = rl_bwt.Size();
    auto rl_bwt2 = rl_bwt.template FlatWindow<PairIC>(2, [rl_bwt_size](size_t index, const common::RingBuffer<PairIC>& rb, auto emit){
                if (index == 0 ){
                    emit(PairIC{rb[0].index+1,rb[0].c});
                    if ( rl_bwt_size > 1 ) {
                        emit(PairIC{rb[1].index-rb[0].index, rb[1].c});
                    }
                } else if (index < rl_bwt_size) {
                    emit(PairIC{rb[1].index-rb[0].index, rb[1].c});
                }
            });


    return rl_bwt2;
}

int main(int argc, char* argv[]) {
    common::CmdlineParser cp;

    cp.SetAuthor("Simon Gog <gog@kit.edu>");

    std::string input_path;
    size_t output_result = 0;

    cp.AddOptParamString("input", input_path,
                         "Path to input file.");
    cp.AddOptParamSizeT("output_result", output_result,
                         "Output result.");



    if (!cp.Process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            if (input_path.size()) {
                auto input_dia = ReadBinary<uint8_t>(ctx, input_path);
                auto output_dia = ConstructRLBWT(input_dia);
                if ( output_result )
                    output_dia.Print("rl_bwt");
            }
            else {
                std::string bwt = "aaaaaaaaaaabbbbaaaaaaaccccdddaacacaffatttttttttttyyyyaaaaa";
                auto input_dia =
                    Generate(ctx,
                             [&](size_t i) { return (uint8_t)bwt[i]; },
                             bwt.size());
                auto output_dia = ConstructRLBWT(input_dia);
                if ( output_result )
                    output_dia.Print("rl_bwt");
            }
        });
}

/******************************************************************************/
