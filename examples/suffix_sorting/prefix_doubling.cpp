/*******************************************************************************
 * examples/suffix_sorting/prefix_doubling.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/suffix_sorting/check_sa.hpp>
#include <examples/suffix_sorting/suffix_sorting.hpp>

#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
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

//! A triple (index, rank, status)
template <typename Index>
struct IndexRankStatus {
    Index index;
    Index rank;
    uint8_t status;

    //! Two IndexRandStatuses are equal iff their ranks are equal.
    bool operator == (const IndexRankStatus& b) const {
        return rank == b.rank;
    }

    //! A IndexRankStatus is smaller than another iff either its fist rank is
    //! smaller or if both ranks are equal and the first index is _larger_ than 
    //! the second. The _larger_ is due to suffixes with larger index being
    //! smaller.
    bool operator < (const IndexRankStatus& b) const {
        return rank < b.rank || (rank == b.rank && index > b.index);
    }

    friend std::ostream& operator << (std::ostream& os, const IndexRankStatus& irs) {
        return os << "(i: " << irs.index << "| r: " << irs.rank << "| s: " << irs.status << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoublinDiscardingDementiev(const InputDIA& input_dia, size_t input_size) {
    LOG1 << "Running PrefixDoublinDiscardingDementiev";

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;
    using IndexRankStatus = suffix_sorting::IndexRankStatus<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;

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
        chars_sorted.Keep()
        .template FlatWindow<Index>(
            2,
            [&](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                if (index == 0)
                    emit(Index(1));
                if (rb[0] == rb[1])
                    emit(Index(0));
                else
                    emit(Index(index + 2));
            })
        .PrefixSum([](const Index a, const Index b) {
                         return a > b ? a : b;
                     });

    DIA<IndexRank> names =
        chars_sorted.Keep()
        .Zip(
            renamed_ranks,
            [](const CharCharIndex& cci, const Index r) {
                return IndexRank { cci.index, r };
            });

    size_t max_rank = renamed_ranks.Max();

    if (max_rank == input_size) {
        auto sa =
            chars_sorted
            .Map([](const CharCharIndex& cci) {
                     return cci.index;
                 });

        return sa.Collapse();
    }

    DIA<IndexRankStatus> names_unique =
        names
        .template FlatWindow<IndexRankStatus>(
            3,
            [&](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                if (index == 0) {
                    uint8_t status = rb[0].rank != rb[1].rank ? 1 : 0;
                    emit(IndexRankStatus { rb[0].index, rb[0].rank, status });
                }
                if (rb[0].rank != rb[1].rank && rb[1].rank != rb[2].rank)
                    emit(IndexRankStatus { rb[1].index, rb[1].rank, 1 });
                else
                    emit(IndexRankStatus { rb[1].index, rb[1].rank, 0 });
                if (index == input_size - 3) {
                    uint8_t status = rb[1].rank != rb[2].rank ? 1 : 0;
                    emit(IndexRankStatus { rb[2].index, rb[2].rank, status });
                }
            });

    size_t iteration = 1;
    DIA<IndexRankStatus> names_unique_sorted =
        names_unique
        .Sort([iteration](const IndexRankStatus& a, const IndexRankStatus& b) {
            Index mod_mask = (Index(1) << iteration) - 1;
            Index div_mask = ~mod_mask;

            if ((a.index & mod_mask) == (b.index & mod_mask))
                return (a.index & div_mask) < (b.index & div_mask);
            else
                return (a.index & mod_mask) < (b.index & mod_mask);
        });

    if (debug_print)
        names_unique_sorted.Keep().Print("names_unique_sorted");

    Context& ctx = input_dia.context();

    DIA<IndexRank> fully_discarded = 
        Generate(
            ctx,
            [](size_t /*index*/) {
                return IndexRank { Index(0), Index(0) };
            },
            0);

    while (true) {
        ++iteration;

        DIA<IndexRank> new_decided;
        DIA<IndexRankStatus> partial_discarded;

        size_t names_size = names_unique_sorted.Keep().Size();

        if (debug_print)
            names_unique_sorted.Keep().Print("names_unique_sorted begin of loop");

        if (names_size > 2) {
            auto discarded_names =
                names_unique_sorted.Keep()
                    .template FlatWindow<IndexRankStatus>(
                        3,
                        [](size_t index, const RingBuffer<IndexRankStatus>& rb, auto emit) {
                            // Discarded names (we need to change the status since we remove it one step later)
                            if (index == 0) {
                                if (rb[0].status == 1) 
                                    emit(IndexRankStatus { rb[0].index, rb[0].rank, 2 });
                                if (rb[1].status == 1) 
                                    emit(IndexRankStatus { rb[1].index, rb[1].rank, 2 }); // Since there is just one preceding entry it's either undiscarded or unique
                            }
                            if (rb[2].status == 1 and (rb[0].status == 1 or rb[1].status == 1))
                                emit(IndexRankStatus { rb[2].index, rb[2].rank, 2 });
                            // Partially discarded names 
                            if (rb[2].status == 1 and rb[0].status == 0 and rb[1].status == 0)
                                emit(rb[2]);
                        });

            new_decided =
                discarded_names.Keep()
                .Filter([](const IndexRankStatus& irs) {
                        return irs.status == 2;
                    })
                .Map([](const IndexRankStatus& irs) {
                        return IndexRank { irs.index, irs.rank };
                    });

            partial_discarded =
                discarded_names
                .Filter([](const IndexRankStatus& irs) {
                        return irs.status == 1;
                    });
        } else {
            new_decided =
                names_unique_sorted.Keep()
                .Filter([](const IndexRankStatus& irs) {
                    return irs.status == 1;
                })
                .Map([](const IndexRankStatus& irs) {
                    return IndexRank { irs.index, irs.rank };
                });
        }

        auto undiscarded =
            names_unique_sorted
            .template FlatWindow<IndexRankRank>(
                2,
                [=](size_t index, const RingBuffer<IndexRankStatus>& rb, auto emit) {
                    if (rb[0].status == Index(0)) {
                        if (rb[0].index + (Index(1) << (iteration - 1)) == rb[1].index)
                            emit(IndexRankRank { rb[0].index, rb[0].rank, rb[1].rank });
                        else
                            emit(IndexRankRank { rb[0].index, rb[0].rank, Index(0) });
                    }
                    if ((index == names_size - 2) && (rb[1].status == Index(0)))
                        emit(IndexRankRank { rb[1].index, rb[1].rank, Index(0) });
                })
            .Sort([](const IndexRankRank& a, const IndexRankRank& b) {
                    return a < b;
                });

        fully_discarded = fully_discarded.Union(new_decided);

        if (debug_print)
            fully_discarded.Keep().Print("fully_discarded");

        size_t duplicates = undiscarded.Keep().Size();

        if (input_dia.context().my_rank() == 0) {
            sLOG1 << "iteration" << iteration - 1
                  << "duplicates" << duplicates;
        }

        if (duplicates == 0) {
            auto sa =
                fully_discarded
                .Sort([](const IndexRank& a, const IndexRank& b) {
                    return a.rank < b.rank;
                    })
                .Map([](const IndexRank& ir) {
                         return ir.index;
                    });
            return sa.Collapse();
        }

        auto rank_addition =
            undiscarded.Keep()
            .template FlatWindow<IndexRank>(
                2,
                [=](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) {
                        emit(IndexRank { Index(0), Index(0) });
                    }
                    Index i = rb[0].rank1 == rb[1].rank1 ? Index(0) : Index(index + 1);
                    Index r;
                    if (rb[0].rank1 != rb[1].rank1)
                        r = Index(index + 1);
                    else
                        r = (rb[0].rank2 == rb[1].rank2) ? Index(0) : Index(index + 1);
                    emit(IndexRank { i, r });
                })
            .PrefixSum([](const IndexRank& a, const IndexRank& b) {
                return IndexRank { 
                    std::max<Index>(a.index, b.index),
                    std::max<Index>(a.rank, b.rank)
                    };
                })
            .Map([](const IndexRank& ir) {
                    return Index(ir.rank - ir.index);
                });

        auto new_ranks =
            undiscarded
            .Zip(rank_addition,
                [](const IndexRankRank& irr, const Index& add) {
                    return IndexRank({ irr.index, irr.rank1 + add });
                });

        size_t number_new_ranks = new_ranks.Keep().Size();

        if (number_new_ranks <= 2) {
            names_unique =
                new_ranks
                .Map(
                    [](const IndexRank& name) {
                        return IndexRankStatus { name.index, name.rank, Index(1) };
                    });
        } else {
            names_unique =
                new_ranks
                .template FlatWindow<IndexRankStatus>(
                    3,
                    [=](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                        if (index == 0) {
                            uint8_t status = rb[0].rank != rb[1].rank ? 1 : 0;
                            emit(IndexRankStatus { rb[0].index, rb[0].rank, status });
                        }
                        if (rb[0].rank != rb[1].rank && rb[1].rank != rb[2].rank)
                            emit(IndexRankStatus { rb[1].index, rb[1].rank, 1 });
                        else
                            emit(IndexRankStatus { rb[1].index, rb[1].rank, 0 });
                        if (index == number_new_ranks - 3) {
                            uint8_t status = rb[1].rank != rb[2].rank ? 1 : 0;
                            emit(IndexRankStatus { rb[2].index, rb[2].rank, status });
                        }
                    });
        }

        names_unique_sorted =
            names_unique
            .Union(partial_discarded)
            .Sort([iteration](const IndexRankStatus& a, const IndexRankStatus& b) {
                    Index mod_mask = (Index(1) << iteration) - 1;
                    Index div_mask = ~mod_mask;

                    if ((a.index & mod_mask) == (b.index & mod_mask))
                        return (a.index & div_mask) < (b.index & div_mask);
                    else
                        return (a.index & mod_mask) < (b.index & mod_mask);
                });
    }
}

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoublingDementiev(const InputDIA& input_dia, size_t input_size) {
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
            names_sorted.Keep().Print("names_sorted");

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
            sLOG1 << "iteration" << iteration
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
        one_mers_sorted.Keep().Print("one_mers_sorted");

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
        rebucket.Keep().Print("rebucket");

    DIA<Index> sa =
        one_mers_sorted
        .Map([](const IndexKMer& iom) {
                 return iom.index;
             })
        .Collapse();

    if (debug_print)
        sa.Keep().Print("sa");

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
            isa.Keep().Print("isa");

        size_t shift_by = (1 << shift_exp++) + 1;

        if (input_dia.context().my_rank() == 0) {
            LOG1 << "iteration " << shift_exp << ": shift ISA by " << shift_by - 1
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
            triple_sorted.Keep().Print("triple_sorted");

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
            sa.Keep().Print("sa");

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
            rebucket.Keep().Print("rebucket");
    }
}

template DIA<uint32_t> PrefixDoubling<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

template DIA<uint32_t> PrefixDoublingDementiev<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

template DIA<uint32_t> PrefixDoublinDiscardingDementiev<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

} // namespace suffix_sorting
} // namespace examples

/******************************************************************************/
