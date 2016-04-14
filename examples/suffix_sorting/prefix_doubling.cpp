/*******************************************************************************
 * examples/suffix_sorting/prefix_doubling.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/suffix_sorting/sa_checker.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/uint_types.hpp>

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

bool debug_print = false;
bool debug = false;

using namespace thrill; // NOLINT
using thrill::common::RingBuffer;

template <typename CharsType, typename Index>
struct IndexKMer {
    Index     index;
    CharsType chars;

    bool operator == (const IndexKMer& b) const {
        return chars == b.chars;
    }

    bool operator < (const IndexKMer& b) const {
        return chars < b.chars;
    }

    friend std::ostream& operator << (std::ostream& os, const IndexKMer& iom) {
        return os << '[' << iom.index << ',' << iom.chars << ']';
    }
} THRILL_ATTRIBUTE_PACKED;

//! A pair (index, rank)
template <typename Index>
struct IndexRank {
    Index index;
    Index rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& ri) {
        return os << '(' << ri.index << '|' << ri.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

//! A triple (index, rank_1, rank_2)
template <typename Index>
struct IndexRankRank {
    Index index;
    Index rank1;
    Index rank2;

    //! Two IndexRankRanks are equal iff their ranks are equal.
    bool operator == (const IndexRankRank& b) const {
        return rank1 == b.rank1 && rank2 == b.rank2;
    }

    //! A IndexRankRank is smaller than another iff either its fist rank is
    //! smaller or if the first ranks are equal if its second rank is smaller,
    //! or if both ranks are equal and the first index is _larger_ than the
    //! second. The _larger_ is due to suffixes with larger index being smaller.
    bool operator < (const IndexRankRank& b) const {
        return rank1 < b.rank1 || (
            rank1 == b.rank1 && (rank2 < b.rank2 || (
                                     rank2 == b.rank2 && index > b.index)));
    }

    friend std::ostream& operator << (std::ostream& os, const IndexRankRank& rri) {
        return os << "( i: " << rri.index << "| r1: " << rri.rank1 << "| r2: " << rri.rank2 << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Char, typename Index>
struct CharCharIndex {
    Char  ch[2];
    Index index;

    bool operator == (const CharCharIndex& b) const {
        return std::equal(ch + 0, ch + 2, b.ch + 0);
    }

    bool operator < (const CharCharIndex& b) const {
        return std::lexicographical_compare(
            ch + 0, ch + 2, b.ch + 0, b.ch + 2);
    }

    friend std::ostream& operator << (std::ostream& os, const CharCharIndex& cci) {
        return os << '[' << cci.ch[0] << ',' << cci.ch[1]
                  << '|' << cci.index << ']';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoublinDiscardingDementiev(const InputDIA& input_dia, size_t input_size) {
    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;

    auto chars_sorted =
        input_dia
        .template FlatWindow<CharCharIndex>(
            2,
            [&](size_t index, const RingBuffer<Char>& rb, auto emit) {
                emit(CharCharIndex { rb[0], rb[1], Index(index) });
                if (index == input_size - 2)
                    emit(CharCharIndex { rb[1], std::numeric_limits<Char>::lowest(), Index(index + 1) });
            })
        .Sort([](const CharCharIndex& a, const CharCharIndex& b) {
                  return a < b;
              });

    DIA<Index> renamed_ranks =
        chars_sorted
        .template FlatWindow<Index>(
            2,
            [&](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                if (index == 0) emit(Index(1));
                if (rb[0] == rb[1]) emit(Index(0));
                else emit(Index(index + 2));
                if (index == input_size - 2) {
                    if (rb[0] == rb[1]) emit(Index(0));
                    else emit(Index(index + 3));
                }
            })
        .PrefixSum([](const Index a, const Index b) {
                       return a > b ? a : b;
                   });

    DIA<IndexRank> names =
        chars_sorted
        .Zip(
            renamed_ranks,
            [](const CharCharIndex& cci, const Index r) {
                return IndexRank { cci.index, r };
            });

    while (true) { }
}

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoublingDementiev(const InputDIA& input_dia, size_t input_size) {
    // enable online consume of DIA contents if not debugging and not computing
    // the BWT.
    input_dia.context().enable_consume(!debug_print);

    LOG1 << "Running PrefixDoublingDementiev";

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;

    auto chars_sorted =
        input_dia
        .template FlatWindow<CharCharIndex>(
            2,
            [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                emit(CharCharIndex {
                         { rb[0], rb[1] }, Index(index)
                     });
                if (index == input_size - 2) {
                    // emit CharCharIndex for last suffix
                    emit(CharCharIndex {
                             { rb[1], std::numeric_limits<Char>::lowest() },
                             Index(index + 1)
                         });
                }
            })
        .Sort();

    auto renamed_ranks =
        chars_sorted.Keep()
        .template FlatWindow<Index>(
            2,
            [](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                if (index == 0) emit(Index(1));
                emit(rb[0] == rb[1] ? Index(0) : Index(1));
            })
        .PrefixSum();

    size_t max_rank = renamed_ranks.Keep().Max();

    if (max_rank == input_size) {
        auto sa =
            chars_sorted
            .Map([](const CharCharIndex& cci) {
                     return cci.index;
                 });

        return sa.Collapse();
    }

    DIA<IndexRank> names =
        chars_sorted
        .Zip(renamed_ranks,
             [](const CharCharIndex& cci, const Index r) {
                 return IndexRank { cci.index, r };
             });

    size_t iteration = 1;
    while (true) {
        auto names_sorted =
            names
            .Sort([iteration](const IndexRank& a, const IndexRank& b) {
                      Index mod_mask = (Index(1) << iteration) - 1;
                      Index div_mask = ~mod_mask;

                      if ((a.index & mod_mask) == (b.index & mod_mask))
                          return (a.index & div_mask) < (b.index & div_mask);
                      else
                          return (a.index & mod_mask) < (b.index & mod_mask);
                  });

        if (debug_print)
            names_sorted.Print("names_sorted");

        size_t next_index = size_t(1) << iteration++;

        auto triple_sorted =
            names_sorted
            .template FlatWindow<IndexRankRank>(
                2,
                [=](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (rb[0].index + Index(next_index) == rb[1].index) {
                        emit(IndexRankRank { rb[0].index, rb[0].rank, rb[1].rank });
                    }
                    else {
                        assert(rb[0].index + Index(next_index) >= Index(input_size));
                        emit(IndexRankRank { rb[0].index, rb[0].rank, Index(0) });
                    }

                    if (index == input_size - 2) {
                        assert(rb[1].index + Index(next_index) >= Index(input_size));
                        emit(IndexRankRank { rb[1].index, rb[1].rank, Index(0) });
                    }
                })
            .Sort();

        renamed_ranks =
            triple_sorted.Keep()
            .template FlatWindow<Index>(
                2,
                [](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) emit(1);
                    emit(rb[0] == rb[1] && rb[0].rank2 != Index(0) ? Index(0) : Index(1));
                })
            .PrefixSum();

        size_t max_rank = renamed_ranks.Keep().Max();
        if (input_dia.context().my_rank() == 0) {
            sLOG << "iteration" << iteration
                 << "max_rank" << max_rank
                 << "duplicates" << input_size - max_rank;
        }

        if (max_rank == input_size) {
            auto sa =
                triple_sorted
                .Map([](const IndexRankRank& irr) {
                         return irr.index;
                     });

            return sa.Collapse();
        }

        names =
            triple_sorted.Zip(
                renamed_ranks,
                [](const IndexRankRank& irr, const Index r) {
                    return IndexRank { irr.index, r };
                });
    }
}

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoubling(const InputDIA& input_dia, size_t input_size) {
    // enable online consume of DIA contents if not debugging and not computing
    // the BWT.
    input_dia.context().enable_consume(!debug_print);

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;
    using IndexKMer = suffix_sorting::IndexKMer<Index, Index>;

    enum {
        input_bit_size = sizeof(Char) << 3,
        k_fitting = sizeof(Index) / sizeof(Char)
    };

    auto one_mers_sorted =
        input_dia
        .template FlatWindow<IndexKMer>(
            k_fitting,
            [input_size](size_t index, const RingBuffer<Char>& rb, auto emit) {
                size_t result = rb[0];
                for (size_t i = 1; i < k_fitting; ++i)
                    result = (result << input_bit_size) | rb[i];
                emit(IndexKMer { Index(index), Index(result) });
                if (index == input_size - k_fitting) {
                    for (size_t i = 1; i < k_fitting; ++i) {
                        result = rb[i];
                        for (size_t j = i + 1; j < k_fitting; ++j)
                            result = (result << input_bit_size) | rb[j];
                        result <<= i * input_bit_size;
                        emit(IndexKMer { Index(index + i), Index(result) });
                    }
                }
            })
        .Sort();

    if (debug_print)
        one_mers_sorted.Print("one_mers_sorted");

    auto rebucket =
        one_mers_sorted.Keep()
        .template FlatWindow<Index>(
            2,
            [](size_t index, const RingBuffer<IndexKMer>& rb, auto emit) {
                if (index == 0) emit(Index(0));
                emit(rb[0] == rb[1] ? Index(0) : Index(index + 1));
            })
        .PrefixSum(common::maximum<Index>());

    if (debug_print)
        rebucket.Print("rebucket");

    DIA<Index> sa =
        one_mers_sorted
        .Map([](const IndexKMer& iom) {
                 return iom.index;
             })
        .Collapse();

    if (debug_print)
        sa.Print("sa");

    size_t shift_exp = 0;
    while (true) {
        DIA<IndexRank> isa =
            sa
            .Zip(rebucket,
                 [](Index s, Index r) {
                     return IndexRank { r, s };
                 })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      return a.rank < b.rank;
                  });

        if (debug_print)
            isa.Print("isa");

        size_t shift_by = (1 << shift_exp++) + 1;

        if (input_dia.context().my_rank() == 0) {
            LOG << "iteration " << shift_exp << ": shift ISA by " << shift_by - 1
                << " positions. hence the window has size " << shift_by;
        }

        DIA<IndexRankRank> triple_sorted =
            isa
            .template FlatWindow<IndexRankRank>(
                shift_by,
                [=](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    emit(IndexRankRank { rb[0].rank, rb.front().index, rb.back().index });
                    if (index == input_size - shift_by)
                        for (size_t i = 1; i < input_size - index; ++i)
                            emit(IndexRankRank { rb[i].rank, rb[i].index, 0 });
                })
            .Sort();

        if (debug_print)
            triple_sorted.Print("triple_sorted");

        // If we don't care about the number of singletons, it's sufficient to
        // test two.
        Index non_singletons =
            triple_sorted.Keep()
            .Window(
                2,
                [](size_t /* index */, const RingBuffer<IndexRankRank>& rb) -> Index {
                    return rb[0] == rb[1] && rb[0].rank2 != Index(0);
                })
            .Sum();

        sa =
            triple_sorted
            .Map([](const IndexRankRank& rri) { return rri.index; })
            .Collapse();

        if (debug_print)
            sa.Print("sa");

        sLOG0 << "non_singletons" << non_singletons;

        // If each suffix is unique regarding their 2h-prefix, we have computed
        // the suffix array and can return it.
        if (non_singletons == Index(0))
            return sa;

        rebucket =
            triple_sorted
            .template FlatWindow<Index>(
                2,
                [](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) emit(Index(0));
                    emit(rb[0] == rb[1] ? Index(0) : Index(index + 1));
                })
            .PrefixSum(common::maximum<Index>());

        if (debug_print)
            rebucket.Print("rebucket");
    }
}

template DIA<uint32_t> PrefixDoubling<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

template DIA<uint32_t> PrefixDoublingDementiev<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

} // namespace suffix_sorting
} // namespace examples

/******************************************************************************/
