/*******************************************************************************
 * examples/suffix_sorting/bwt_generator.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/suffix_sorting/sa_checker.hpp>

#include <thrill/api/collapse.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/uint_types.hpp>

#include <ostream>
#include <utility>

namespace examples {
namespace suffix_sorting {

using namespace thrill; // NOLINT

//! A pair (rank, index)
template <typename Index>
struct IndexRank {
    Index index;
    Index rank;

    friend std::ostream& operator << (
        std::ostream& os, const IndexRank& ri) {
        return os << '(' << ri.index << '|' << ri.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Index, typename Char>
struct IndexChar {
    Index index;
    Char  ch;

    friend std::ostream& operator << (
        std::ostream& os, const IndexChar& ic) {
        return os << '(' << ic.index << '|' << ic.ch << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename InputDIA, typename SuffixArrayDIA>
InputDIA GenerateBWT(const InputDIA& input, const SuffixArrayDIA& suffix_array) {

    Context& ctx = input.ctx();

    using Char = typename InputDIA::ValueType;
    using Index = typename SuffixArrayDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexChar = suffix_sorting::IndexChar<Index, Char>;

    uint64_t input_size = input.Size();

    DIA<Index> indices = Generate(ctx,
                                  [](size_t index) { return Index(index); },
                                  input_size);

    auto bwt =
        suffix_array
        .Map([input_size](const Index& i) {
                 if (i == Index(0))
                     return Index(input_size - 1);
                 return Index(i - 1);
             })
        .Zip(
            indices,
            [](const Index& text_pos, const Index& idx) {
                return IndexRank { text_pos, idx };
            })
        .Sort([](const IndexRank& a, const IndexRank& b) {
                  return a.index < b.index;
              })
        .Zip(
            input,
            [](const IndexRank& text_order, const Char& ch) {
                return IndexChar { text_order.rank, ch };
            })
        .Sort([](const IndexChar& a, const IndexChar& b) {
                  return a.index < b.index;
              })
        .Map([](const IndexChar& ic) {
                 return ic.ch;
             });

    return bwt.Collapse();
}

// instantiations
template DIA<uint8_t> GenerateBWT(
    const DIA<uint8_t>& input, const DIA<uint32_t>& suffix_array);
template DIA<uint8_t> GenerateBWT(
    const DIA<uint8_t>& input, const DIA<common::uint40>& suffix_array);
template DIA<uint8_t> GenerateBWT(
    const DIA<uint8_t>& input, const DIA<common::uint48>& suffix_array);
template DIA<uint8_t> GenerateBWT(
    const DIA<uint8_t>& input, const DIA<uint64_t>& suffix_array);
template DIA<uint64_t> GenerateBWT(
    const DIA<uint64_t>& input, const DIA<uint64_t>& suffix_array);

} // namespace suffix_sorting
} // namespace examples

/******************************************************************************/
