/*******************************************************************************
 * examples/suffix_sorting/construct_bwt.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_BWT_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_BWT_HEADER

#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_with_index.hpp>

namespace examples {
namespace suffix_sorting {

template <typename InputDIA, typename SuffixArrayDIA>
InputDIA ConstructBWT(const InputDIA& input, const SuffixArrayDIA& suffix_array,
                      uint64_t input_size) {

    // thrill::Context& ctx = input.ctx();

    using Char = typename InputDIA::ValueType;
    using Index = typename SuffixArrayDIA::ValueType;

    struct IndexRank {
        Index index;
        Index rank;
    } THRILL_ATTRIBUTE_PACKED;

    struct IndexChar {
        Index index;
        Char  ch;
    } THRILL_ATTRIBUTE_PACKED;

    return suffix_array
           .Map([input_size](const Index& i) {
                    if (i == Index(0))
                        return Index(input_size - 1);
                    return i - Index(1);
                })
           .ZipWithIndex([](const Index& text_pos, const size_t& i) {
                             return IndexRank { text_pos, Index(i) };
                         })
           // .Zip(Generate(ctx, input_size),
           //      [](const Index& text_pos, const size_t& idx) {
           //          return IndexRank { text_pos, Index(idx) };
           //      })
           .Sort([](const IndexRank& a, const IndexRank& b) {
                     return a.index < b.index;
                 })
           .Zip(input,
                [](const IndexRank& text_order, const Char& ch) {
                    return IndexChar { text_order.rank, ch };
                })
           .Sort([](const IndexChar& a, const IndexChar& b) {
                     return a.index < b.index;
                 })
           .Map([](const IndexChar& ic) {
                    return ic.ch;
                })
           .Collapse();
}

} // namespace suffix_sorting
} // namespace examples

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_CONSTRUCT_BWT_HEADER

/******************************************************************************/
