/*******************************************************************************
 * examples/suffix_sorting/construct_lcp.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_LCP_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_LCP_HEADER

#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/radix_sort.hpp>
#include <thrill/common/uint_types.hpp>


namespace examples {
namespace suffix_sorting {

using namespace thrill; // NOLINT
using thrill::common::RingBuffer;

template <typename Index>
struct IndexRank {
    Index index;
    Index rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& ir) {
        return os << '(' << ir.index << '|' << ir.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Char, typename Index>
struct IndexChar {
    Index index;
    Char  ch;

    friend std::ostream& operator << (std::ostream& os, const IndexChar& ic) {
        return os << '(' << ic.index << '|' << ic.ch << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Index>
struct IndexFlag {
    Index index;
    bool flag;

    friend std::ostream& operator << (std::ostream& os, const IndexFlag& idx_flag) {
        return os << '(' << idx_flag.index << '|' << (idx_flag.flag ? 't' : 'f') << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Index>
struct IndexRankFlag {
    Index index;
    Index rank;
    bool flag;

    friend std::ostream& operator << (std::ostream& os, const IndexRankFlag& irf) {
        return os << '(' << irf.index << '|' << irf.rank << '|' << (irf.flag ? 't' : 'f') << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename InputDIA, typename IndexDIA>
IndexDIA ConstructLCP(const InputDIA& input, const IndexDIA& /*suffix_array*/,
                      const InputDIA& bwt, uint64_t input_size) {

    thrill::Context& ctx = input.ctx();

    using Char = typename InputDIA::ValueType;
    using Index = typename IndexDIA::ValueType;

    using IndexChar = suffix_sorting::IndexChar<Char, Index>;
    using IndexFlag = suffix_sorting::IndexFlag<Index>;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankFlag = suffix_sorting::IndexRankFlag<Index>;

    DIA<IndexFlag> lcp =
        Generate(
            ctx,
            [](size_t /*index*/) {
                return IndexFlag { Index(0), false };
            },
            input_size);

    auto tmp_inverse_bwt =
        bwt
        .Zip(Generate(ctx, input_size),
            [](const Char& c, const size_t idx) {
                return IndexChar { Index(idx), c };
            })
        .Sort([](const IndexChar& a, const IndexChar& b) {
                if (a.ch == b.ch)
                    return a.index < b.index;
                return a.ch < b.ch;
            });

    auto intervals =
        tmp_inverse_bwt
        .Keep()
        .template FlatWindow<Index>(
            2,
            [](const size_t index, const RingBuffer<IndexChar>& rb, auto emit) {
                if (index == 0)
                    emit (Index(0));
                emit(rb[0].ch == rb[1].ch ? Index(0) : Index(1));
            })
        .PrefixSum();

    // intervals.Keep().Print("intervals");

    size_t number_intervals = intervals.Keep().Max();


    auto inverse_bwt =
        tmp_inverse_bwt
        .Zip(Generate(ctx, input_size),
            [](const IndexChar& ic, const size_t idx) {
                return IndexRank { ic.index, Index(idx) };
            })
        .Sort([](const IndexRank& a, const IndexRank& b) {
                return a.index < b.index;
            })
        .Map([](const IndexRank& ir) {
                return ir.rank;
            })
        .Cache();

    // inverse_bwt.Keep().Print("inverse_bwt");

    size_t lcp_value = 0;
    while (number_intervals + 1 < input_size) {
        lcp =
            intervals
            .Keep()
            .Zip(
                lcp,
                [](const Index& idx, const IndexFlag& idx_flag) {
                    return IndexRankFlag { idx_flag.index, idx, idx_flag.flag };
                })
            .template FlatWindow<IndexFlag>(
                2,
                [lcp_value](const size_t index, const RingBuffer<IndexRankFlag>& rb, auto emit) {
                    if (index == 0)
                        emit(IndexFlag { 0, true });
                    if (rb[0].rank != rb[1].rank && !rb[1].flag)
                        emit(IndexFlag { Index(lcp_value), true });
                    else
                        emit(IndexFlag { rb[1].index, rb[1].flag });
                });

        intervals =
            inverse_bwt
            .Keep()
            .Zip(
                intervals,
                [](const Index& pbwt, const Index& i) {
                    return IndexRank { pbwt, i };
                })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                    return a.index < b.index;
                })
            .Map([](const IndexRank& i) {
                    return i.rank;
                })
            .template FlatWindow<Index>(
                2,
                [](const size_t index, const RingBuffer<Index>& rb, auto emit) {
                    if (index == 0)
                        emit (Index(0));
                    emit(rb[0] == rb[1] ? Index(0) : Index(1));
                })
            .PrefixSum();

        number_intervals = intervals.Keep().Max();
        ++lcp_value;
    }

    lcp =
        intervals
        .Keep()
        .Zip(
            lcp,
            [](const Index& idx, const IndexFlag& idx_flag) {
                return IndexRankFlag { idx_flag.index, idx, idx_flag.flag };
            })
        .template FlatWindow<IndexFlag>(
            2,
            [lcp_value](const size_t index, const RingBuffer<IndexRankFlag>& rb, auto emit) {
                if (index == 0)
                    emit(IndexFlag { 0, true });
                if (rb[0].rank != rb[1].rank && !rb[1].flag)
                    emit(IndexFlag { Index(lcp_value), true });
                else
                    emit(IndexFlag { rb[1].index, rb[1].flag });
            });

    // lcp.Keep().Print("lcp");
    
    return lcp
              .Map([](const IndexFlag& idx_flag) {
                      return idx_flag.index;
                  })
              .Collapse();
}

} // namespace suffix_sorting
} // namespace examples

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_LCP_HEADER

/******************************************************************************/