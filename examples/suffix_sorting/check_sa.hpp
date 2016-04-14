/*******************************************************************************
 * examples/suffix_sorting/check_sa.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_CHECK_SA_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_CHECK_SA_HEADER

#include <thrill/api/generate.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>

#include <utility>

namespace examples {
namespace suffix_sorting {

template <typename InputDIA, typename SuffixArrayDIA>
bool CheckSA(const InputDIA& input, const SuffixArrayDIA& suffix_array) {

    using namespace thrill; // NOLINT
    using thrill::common::RingBuffer;

    Context& ctx = input.ctx();

    using Char = typename InputDIA::ValueType;
    using Index = typename SuffixArrayDIA::ValueType;

    //! A pair (rank, index)
    struct IndexRank {
        Index index;
        Index rank;
    } THRILL_ATTRIBUTE_PACKED;

    struct Index3 {
        Index index;
        Index next;
        Char  ch;
    } THRILL_ATTRIBUTE_PACKED;

    uint64_t input_size = input.Keep().Size();

    auto isa_pair =
        suffix_array
        // build tuples with index: (SA[i]) -> (i, SA[i]),
        .Zip(Generate(ctx, input_size),
             [](const Index& sa, const Index& i) {
                 return IndexRank { sa, i };
             })
        // take (i, SA[i]) and sort to (ISA[i], i)
        .Sort([](const IndexRank& a, const IndexRank& b) {
                  return a.index < b.index;
              });

    // Zip (ISA[i], i) with [0,n) and check that the second component was a
    // permutation of [0,n)
    Index perm_check =
        isa_pair.Keep()
        .Zip(Generate(ctx, input_size),
             [](const IndexRank& ir, const Index& index) -> Index {
                 return ir.index == index ? 0 : 1;
             })
        // sum over all boolean values.
        .Max();

    if (perm_check != Index(0)) {
        LOG1 << "Error: suffix array is not a permutation of 0..n-1.";
        return false;
    }

    using IndexPair = std::pair<Index, Index>;

    auto order_check =
        isa_pair
        // extract ISA[i]
        .Map([](const IndexRank& ir) { return ir.rank; })
        // build (ISA[i], ISA[i+1], T[i])
        .template FlatWindow<IndexPair>(
            2, [input_size](size_t index, const RingBuffer<Index>& rb, auto emit) {
                emit(IndexPair { rb[0], rb[1] });
                if (index == input_size - 2) {
                    // emit sentinel at end
                    emit(IndexPair { rb[1], input_size });
                }
            })
        .Zip(input,
             [](const std::pair<Index, Index>& isa_pair, const Char& ch) {
                 return Index3 { isa_pair.first, isa_pair.second, ch };
             })
        // and sort to (i, ISA[SA[i]+1], T[SA[i]])
        .Sort([](const Index3& a, const Index3& b) {
                  return a.index < b.index;
              });

    char order_check_sum =
        order_check
        // check that no pair violates the order
        .Window(
            2,
            [input_size](size_t index, const RingBuffer<Index3>& rb) -> char {

                if (rb[0].ch > rb[1].ch) {
                    // simple check of first character of suffix failed.
                    LOG1 << "Error: suffix array position "
                         << index << " ordered incorrectly.";
                    return 1;
                }
                else if (rb[0].ch == rb[1].ch) {
                    if (rb[1].next == Index(input_size)) {
                        // last suffix of string must be first among those with
                        // same first character
                        LOG1 << "Error: suffix array position "
                             << index << " ordered incorrectly.";
                        return 1;
                    }
                    if (rb[0].next != Index(input_size) && rb[0].next > rb[1].next) {
                        // positions SA[i] and SA[i-1] has same first character
                        // but their suffixes are ordered incorrectly: the
                        // suffix position of SA[i] is given by ISA[SA[i]]
                        LOG1 << "Error: suffix array position "
                             << index << " ordered incorrectly.";
                        return 1;
                    }
                }
                // else (rb[0].ch < rb[1].ch) -> okay.
                return 0;
            })
        .Max();

    return (order_check_sum == 0);
}

} // namespace suffix_sorting
} // namespace examples

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_CHECK_SA_HEADER

/******************************************************************************/
