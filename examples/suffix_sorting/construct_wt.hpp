/*******************************************************************************
 * examples/suffix_sorting/construct_wt.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2016 Simon Gog <gog@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_WT_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_WT_HEADER

#include <thrill/api/collapse.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/common/logger.hpp>
#include <tlx/math/integer_log2.hpp>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

namespace examples {
namespace suffix_sorting {

template <typename InputDIA>
auto ConstructWaveletTree(
    const InputDIA& input_dia, const std::string& output_path) {
    static constexpr bool debug = false;

    using namespace thrill; // NOLINT

    uint64_t max_value = input_dia.Keep().Max();
    sLOG << "max_value" << max_value;

    uint64_t level = tlx::integer_log2_ceil(max_value);
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

        wt.Keep().Window(
            DisjointTag, 64,
            [maskbit](size_t, const std::vector<uint8_t>& v) {
                uint64_t x = 0;
                for (size_t i = 0; i < v.size(); ++i) {
                    if (v[i] & maskbit) {
                        x |= uint64_t(1) << i;
                    }
                }
                return x;
            })
        .WriteBinary(
            output_path + common::str_sprintf("-lvl%02u-", unsigned(level)));

        wt = wt.Sort(
            [mask](const uint8_t& a, const uint8_t& b) {
                return (a & mask) < (b & mask);
            });

        if (debug) wt.Print("wt");
    }
}

} // namespace suffix_sorting
} // namespace examples

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_WT_HEADER

/******************************************************************************/
