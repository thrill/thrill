/*******************************************************************************
 * examples/suffix_sorting/just_sort.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/context.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/qsort.hpp>
#include <tlx/string/format_si_iec_units.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

//! A tuple with index (i,t_i,t_{i+1},t_{i+2},...,t_{i+6}).
template <typename AlphabetType>
struct Chars {
    AlphabetType ch[7];

    bool operator == (const Chars& b) const {
        return std::tie(ch[0], ch[1], ch[2], ch[3], ch[4], ch[5], ch[6])
               == std::tie(b.ch[0], b.ch[1], b.ch[2], b.ch[3],
                           b.ch[4], b.ch[5], b.ch[6]);
    }

    bool operator < (const Chars& b) const {
        return std::tie(ch[0], ch[1], ch[2], ch[3], ch[4], ch[5], ch[6])
               < std::tie(b.ch[0], b.ch[1], b.ch[2], b.ch[3],
                          b.ch[4], b.ch[5], b.ch[6]);
    }

    friend std::ostream& operator << (std::ostream& os, const Chars& chars) {
        return os << '[' << chars.ch[0] << ',' << chars.ch[1]
                  << ',' << chars.ch[2] << ',' << chars.ch[3]
                  << ',' << chars.ch[4] << ',' << chars.ch[5]
                  << ',' << chars.ch[6] << ']';
    }

    static Chars EndSentinel() {
        return Chars {
                   {
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest()
                   }
        };
    }
} TLX_ATTRIBUTE_PACKED;

//! A tuple with index (i,t_i,t_{i+1},t_{i+2}).
template <typename Index, typename AlphabetType>
struct IndexChars {
    Index               index;
    Chars<AlphabetType> chars;

    friend std::ostream& operator << (std::ostream& os, const IndexChars& tc) {
        return os << '[' << tc.index << '|' << tc.chars << ']';
    }
} TLX_ATTRIBUTE_PACKED;

int main(int argc, char* argv[]) {

    using namespace thrill; // NOLINT

    common::CmdlineParser cp;

    cp.SetAuthor("Timo Bingmann <tb@panthema.net>");

    uint64_t input_size = 50000000;
    cp.AddBytes('s', "input_size", input_size,
                "Number of DC7 tuples to sort.");

    std::string algo = "1";
    cp.AddString('a', "algo", algo,
                 "select sort algo: '1' pivot, '2' pivots, '3' pivots");

    if (!cp.Process(argc, argv))
        return -1;

    using IndexChars = ::IndexChars<uint32_t, uint8_t>;
    std::vector<IndexChars> input;

    std::default_random_engine rng_(123456);

    for (size_t i = 0; i < input_size; ++i) {
        input.emplace_back(
            IndexChars {
                uint8_t(rng_()), {
                    { uint8_t(rng_()), uint8_t(rng_()), uint8_t(rng_()),
                      uint8_t(rng_()), uint8_t(rng_()), uint8_t(rng_()),
                      uint8_t(rng_()) }
                }
            });
    }

    LOG1 << "Sorting " << input_size << " DC7 tuples, total size = "
         << tlx::format_iec_units(input_size * sizeof(IndexChars)) << 'B';

    return Run(
        [&](Context& ctx) {

            std::vector<IndexChars> vec = input;
            auto compare_function_ =
                [](const IndexChars& a, const IndexChars& b) {
                    return a.chars < b.chars;
                };

            ctx.net.Barrier();

            common::StatsTimerStart sort_time;
            if (algo == "1")
                std::sort(vec.begin(), vec.end(), compare_function_);
            if (algo == "2")
                common::qsort_two_pivots_yaroslavskiy(
                    vec.begin(), vec.end(), compare_function_);
            if (algo == "3")
                common::qsort_three_pivots(
                    vec.begin(), vec.end(), compare_function_);
            sort_time.Stop();

            ctx.PrintCollectiveMeanStdev(
                "sort_time", sort_time.SecondsDouble());
        });
}

/******************************************************************************/
