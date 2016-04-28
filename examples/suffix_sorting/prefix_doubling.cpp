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

//! A triple with index (index, rank_1, rank_2, rank_3)
template <typename Index>
struct Index3Rank {
    Index index;
    Index rank1;
    Index rank2;
    Index rank3;

    friend std::ostream& operator << (std::ostream& os, const Index3Rank& irrr) {
        return os << "( i: " << irrr.index << "| r1: " << irrr.rank1
                  << "| r2: " << irrr.rank2 << "| r3: " << irrr.rank3 << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Char, typename Index>
struct CharCharIndex {
    Char  ch[2];
    Index index;

    bool operator == (const CharCharIndex& b) const {
        return std::tie(ch[0], ch[1]) == std::tie(b.ch[0], b.ch[1]);
    }

    bool operator < (const CharCharIndex& b) const {
        return std::tie(ch[0], ch[1]) < std::tie(b.ch[0], b.ch[1]);
    }

    friend std::ostream& operator << (std::ostream& os, const CharCharIndex& cci) {
        return os << '[' << cci.ch[0] << ',' << cci.ch[1]
                  << '|' << cci.index << ']';
    }
} THRILL_ATTRIBUTE_PACKED;

enum class Status : uint8_t {
    UNDECIDED = 0,
    UNIQUE = 1,
    FULLY_DISCARDED = 2
};

//! A triple (index, rank, status)
template <typename Index>
struct IndexRankStatus {
    Index  index;
    Index  rank;
    Status status;

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
        return os << "(i: " << irs.index << "| r: " << irs.rank << "| s: "
                  << static_cast<uint8_t>(irs.status) << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

//! A triple (index, rank, status)
template <typename Index>
struct IndexRankRankStatus {
    Index  index;
    Index  rank1;
    Index  rank2;
    Status status;

    friend std::ostream& operator << (std::ostream& os, const IndexRankRankStatus& irrs) {
        return os << "(i: " << irrs.index << "| r1: " << irrs.rank1 << "| r2: "
                  << irrs.rank2 << "| s: " << static_cast<uint8_t>(irrs.status) << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoublingDiscardingDementiev(const InputDIA& input_dia, size_t input_size, bool packed) {
    if (input_dia.ctx().my_rank() == 0)
        LOG1 << "Running PrefixDoublingDiscardingDementiev";

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;
    using IndexRankStatus = suffix_sorting::IndexRankStatus<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;
    using Index3Rank = suffix_sorting::Index3Rank<Index>;
    using IndexRankRankStatus = suffix_sorting::IndexRankRankStatus<Index>;

    size_t input_bit_size = sizeof(Char) << 3;
    size_t k_fitting = sizeof(Index) / sizeof(Char);

    size_t iteration = 1;
    if (packed) {
        iteration = 0;
        size_t tmp = k_fitting;
        while (tmp >>= 1) ++iteration;
    }

    DIA<IndexRank> names;

    if (packed) {
        auto chars_sorted =
            input_dia
            .template FlatWindow<IndexRank>(
                k_fitting,
                [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    size_t result = rb[0];
                    for (size_t i = 1; i < k_fitting; ++i)
                        result = (result << input_bit_size) | rb[i];
                    emit(IndexRank { Index(index), Index(result) });
                    if (index + k_fitting == input_size) {
                        for (size_t i = 1; i < k_fitting; ++i) {
                            result = rb[i];
                            for (size_t j = i + 1; j < k_fitting; ++j)
                                result = (result << input_bit_size) | rb[j];
                            result <<= i * input_bit_size;
                            emit(IndexRank { Index(index + i), Index(result) });
                        }
                    }
                })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                    return a.rank < b.rank;
                });

            if (debug_print)
                chars_sorted.Keep().Print("chars_sorted packed");

        names =
            chars_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (index == 0)
                        emit(IndexRank { rb[0].index, Index(1) });
                    emit(IndexRank {
                             rb[1].index, Index(rb[0].rank == rb[1].rank ? 0 : index + 2)
                         });
                })
            .PrefixSum([](const IndexRank a, const IndexRank b) {
                    return IndexRank { b.index, std::max(a.rank, b.rank) };
                });
    }
    else {
        auto chars_sorted =
            input_dia
            .template FlatWindow<CharCharIndex>(
                2,
                [](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    emit(CharCharIndex {
                             { rb[0], rb[1] }, Index(index)
                         });
                },
                [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    if (index + 1 == input_size) {
                        // emit CharCharIndex for last suffix
                        emit(CharCharIndex {
                                 { rb[0], std::numeric_limits<Char>::lowest() },
                                 Index(index)
                             });
                    }
                })
            .Sort();

        names =
            chars_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                    if (index == 0)
                        emit(IndexRank { rb[0].index, Index(1) });
                    emit(IndexRank {
                             rb[1].index, Index(rb[0] == rb[1] ? 0 : index + 2)
                         });
                })
            .PrefixSum([](const IndexRank a, const IndexRank b) {
                    return IndexRank { b.index, std::max(a.rank, b.rank) };
                });
    }

    auto names_unique =
        names
        .template FlatWindow<IndexRankStatus>(
            3,
            [=](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                if (index == 0) {
                    Status status = rb[0].rank != rb[1].rank ? Status::UNIQUE : Status::UNDECIDED;
                    emit(IndexRankStatus { rb[0].index, rb[0].rank, status });
                }
                {
                    Status status = rb[0].rank != rb[1].rank && rb[1].rank != rb[2].rank
                                    ? Status::UNIQUE : Status::UNDECIDED;

                    emit(IndexRankStatus { rb[1].index, rb[1].rank, status });
                }
                if (index == input_size - 3) {
                    Status status = rb[1].rank != rb[2].rank ? Status::UNIQUE : Status::UNDECIDED;
                    emit(IndexRankStatus { rb[2].index, rb[2].rank, status });
                }
            });

    auto names_unique_sorted =
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

    std::vector<DIA<IndexRank> > fully_discarded;

    while (true) {
        ++iteration;

        size_t names_size = names_unique_sorted.Keep().Size();

        if (debug_print)
            names_unique_sorted.Keep().Print("names_unique_sorted begin of loop");

        auto discarded_names =
            names_unique_sorted.Keep()
            .template FlatWindow<IndexRankRankStatus>(
                3,
                [=](size_t index, const RingBuffer<IndexRankStatus>& rb, auto emit) {
                    // Discarded names (we need to change the status since we remove it one step later)
                    if (index == 0) {
                        if (rb[0].status == Status::UNIQUE)
                            emit(IndexRankRankStatus { rb[0].index, rb[0].rank, Index(0), Status::FULLY_DISCARDED });
                        if (rb[1].status == Status::UNIQUE)
                            // Since there is just one preceding entry it's either undiscarded or unique
                            emit(IndexRankRankStatus { rb[1].index, rb[1].rank, Index(0), Status::FULLY_DISCARDED });
                    }
                    if (rb[2].status == Status::UNIQUE) {
                        if (rb[0].status == Status::UNIQUE || rb[1].status == Status::UNIQUE)
                            emit(IndexRankRankStatus { rb[2].index, rb[2].rank, Index(0), Status::FULLY_DISCARDED });
                        else
                            emit(IndexRankRankStatus { rb[2].index, rb[2].rank, Index(0), Status::UNIQUE });
                    }
                    if (rb[0].status == Status::UNDECIDED) {
                        if (rb[0].index + (Index(1) << (iteration - 1)) == rb[1].index)
                            emit(IndexRankRankStatus { rb[0].index, rb[0].rank, rb[1].rank, Status::UNDECIDED });
                        else
                            emit(IndexRankRankStatus { rb[0].index, rb[0].rank, Index(0), Status::UNDECIDED });
                    }
                },
                [=](size_t index, const RingBuffer<IndexRankStatus>& rb, auto emit) {
                    if (index == 0) {
                        if (rb[0].status == Status::UNIQUE)
                            emit(IndexRankRankStatus { rb[0].index, rb[0].rank, Index(0), Status::FULLY_DISCARDED });
                        else     // (rb[0].status == Status::UNDECIDED)
                            emit(IndexRankRankStatus { rb[0].index, rb[0].rank, Index(0), Status::UNDECIDED });
                        if (rb[1].status == Status::UNIQUE)
                            emit(IndexRankRankStatus { rb[1].index, rb[1].rank, Index(0), Status::FULLY_DISCARDED });
                        else     // (rb[1].status == Status::UNDECIDED)
                            emit(IndexRankRankStatus { rb[1].index, rb[1].rank, Index(0), Status::UNDECIDED });
                    }
                    if (index == names_size - 2) {
                        if (rb[0].status == Status::UNDECIDED)
                            emit(IndexRankRankStatus { rb[0].index, rb[0].rank, rb[1].rank, Status::UNDECIDED });
                        if (rb[1].status == Status::UNDECIDED)
                            emit(IndexRankRankStatus { rb[1].index, rb[1].rank, Index(0), Status::UNDECIDED });
                    }
                });

        auto new_decided =
            discarded_names.Keep()
            .Filter([](const IndexRankRankStatus& irs) {
                        return irs.status == Status::FULLY_DISCARDED;
                    })
            .Map([](const IndexRankRankStatus& irs) {
                     return IndexRank { irs.index, irs.rank1 };
                 });

        auto partial_discarded =
            discarded_names.Keep()
            .Filter([](const IndexRankRankStatus& irs) {
                        return irs.status == Status::UNIQUE;
                    })
            .Map([](const IndexRankRankStatus& irs) {
                     return IndexRankStatus { irs.index, irs.rank1, Status::UNIQUE };
                 });

        auto undiscarded =
            discarded_names
            .Filter([](const IndexRankRankStatus& irs) {
                        return irs.status == Status::UNDECIDED;
                    })
            .Map([](const IndexRankRankStatus& irs) {
                     return IndexRankRank { irs.index, irs.rank1, irs.rank2 };
                 })
            .Sort();

        fully_discarded.emplace_back(new_decided.Cache());

        size_t duplicates = undiscarded.Keep().Size();

        if (input_dia.context().my_rank() == 0) {
            sLOG1 << "iteration" << iteration - 1
                  << "duplicates" << duplicates;
        }

        if (duplicates == 0) {
            auto sa =
                Union(fully_discarded)
                .Sort([](const IndexRank& a, const IndexRank& b) {
                          return a.rank < b.rank;
                      })
                .Map([](const IndexRank& ir) {
                         return ir.index;
                     });
            return sa.Collapse();
        }

        auto new_ranks =
            undiscarded
            .template FlatWindow<Index3Rank>(
                2,
                [](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) {
                        emit(Index3Rank { rb[0].index, Index(0), Index(0), rb[0].rank1 });
                    }
                    Index i = rb[0].rank1 == rb[1].rank1 ? Index(0) : Index(index + 1);
                    Index r;
                    if (rb[0].rank1 != rb[1].rank1)
                        r = Index(index + 1);
                    else
                        r = (rb[0].rank2 == rb[1].rank2) ? Index(0) : Index(index + 1);
                    emit(Index3Rank { rb[1].index, i, r, rb[1].rank1 });
                },
                [](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0)
                        emit(Index3Rank { rb[0].index, Index(0), Index(0), rb[0].rank1 });
                })
            .PrefixSum([](const Index3Rank& a, const Index3Rank& b) {
                           return Index3Rank {
                               b.index,
                               std::max<Index>(a.rank1, b.rank1),
                               std::max<Index>(a.rank2, b.rank2),
                               b.rank3
                           };
                       })
            .Map([](const Index3Rank& ir) {
                     return IndexRank { ir.index, ir.rank3 + (ir.rank2 - ir.rank1) };
                 });

        names_unique =
            new_ranks
            .template FlatWindow<IndexRankStatus>(
                3,
                [=](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (index == 0) {
                        Status status = rb[0].rank != rb[1].rank ? Status::UNIQUE : Status::UNDECIDED;
                        emit(IndexRankStatus { rb[0].index, rb[0].rank, status });
                    }
                    if (rb[0].rank != rb[1].rank && rb[1].rank != rb[2].rank)
                        emit(IndexRankStatus { rb[1].index, rb[1].rank, Status::UNIQUE });
                    else
                        emit(IndexRankStatus { rb[1].index, rb[1].rank, Status::UNDECIDED });
                    if (index + 3 == duplicates) {
                        Status status = rb[1].rank != rb[2].rank ? Status::UNIQUE : Status::UNDECIDED;
                        emit(IndexRankStatus { rb[2].index, rb[2].rank, status });
                    }
                },
                [](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (index == 0) { // We know that there are exactly 2 names
                        emit(IndexRankStatus { rb[0].index, rb[0].rank, Status::UNIQUE });
                        emit(IndexRankStatus { rb[1].index, rb[1].rank, Status::UNIQUE });
                    }
                });

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
DIA<Index> PrefixDoublingDementiev(const InputDIA& input_dia, size_t input_size, bool packed) {
    LOG1 << "Running PrefixDoublingDementiev";

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;

    size_t input_bit_size = sizeof(Char) << 3;
    size_t k_fitting = sizeof(Index) / sizeof(Char);

    size_t iteration = 1;
    if (packed) {
        iteration = 0;
        size_t tmp = k_fitting;
        while (tmp >>= 1) ++iteration;
    }

    DIA<IndexRank> names;

    if (packed) {
        auto chars_sorted =
            input_dia
            .template FlatWindow<IndexRank>(
                k_fitting,
                [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    size_t result = rb[0];
                    for (size_t i = 1; i < k_fitting; ++i)
                        result = (result << input_bit_size) | rb[i];
                    emit(IndexRank { Index(index), Index(result) });
                    if (index + k_fitting == input_size) {
                        for (size_t i = 1; i < k_fitting; ++i) {
                            result = rb[i];
                            for (size_t j = i + 1; j < k_fitting; ++j)
                                result = (result << input_bit_size) | rb[j];
                            result <<= i * input_bit_size;
                            emit(IndexRank { Index(index + i), Index(result) });
                        }
                    }
                })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                    return a.rank < b.rank;
                });

            if (debug_print)
                chars_sorted.Keep().Print("chars_sorted packed");

        names =
            chars_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (index == 0)
                        emit(IndexRank { rb[0].index, Index(1) });
                    emit(IndexRank {
                             rb[1].index, Index(rb[0].rank == rb[1].rank ? 0 : index + 2)
                         });
                });
    }
    else {
        auto chars_sorted =
            input_dia
            .template FlatWindow<CharCharIndex>(
                2,
                [](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    emit(CharCharIndex {
                             { rb[0], rb[1] }, Index(index)
                         });
                },
                [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    if (index + 1 == input_size) {
                        // emit CharCharIndex for last suffix position
                        emit(CharCharIndex {
                                 { rb[0], std::numeric_limits<Char>::lowest() },
                                 Index(index)
                             });
                    }
                })
            .Sort();

        names =
            chars_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                    if (index == 0)
                        emit(IndexRank { rb[0].index, Index(1) });
                    emit(IndexRank {
                             rb[1].index, Index(rb[0] == rb[1] ? 0 : index + 2)
                         });
                });

    }

    auto number_duplicates =
        names.Keep()
        .Filter([](const IndexRank& ir) {
                return ir.rank == Index(0);
            })
        .Size();

    if (number_duplicates == 0) {
        if (input_dia.context().my_rank() == 0)
            sLOG1 << "Finished before doubling in loop";

        auto sa =
            names
            .Map([](const IndexRank& ir) {
                     return ir.index;
                 });

        return sa.Collapse();
    }

    names =
        names
        .PrefixSum(
            [](const IndexRank& a, const IndexRank& b) {
                return IndexRank { b.index, std::max(a.rank, b.rank) };
            });

    if (debug_print)
        names.Keep().Print("names before loop");

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
                [=](size_t /* index */, const RingBuffer<IndexRank>& rb, auto emit) {
                    emit(IndexRankRank {
                             rb[0].index, rb[0].rank,
                             (rb[0].index + Index(next_index) == rb[1].index)
                             ? rb[1].rank : Index(0)
                         });
                },
                [=](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (index + 1 == input_size)
                        emit(IndexRankRank { rb[0].index, rb[0].rank, Index(0) });
                })
            .Sort();

        names =
            triple_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) emit(IndexRank { rb[0].index, Index(1) });

                    emit(IndexRank {
                             rb[1].index,
                             (rb[0] == rb[1] && rb[0].rank2 != Index(0))
                             ? Index(0) : Index(1)
                         });
                });

        number_duplicates =
            names.Keep()
            .Filter([](const IndexRank& ir) {
                    return ir.rank == Index(0);
                })
            .Size();

        if (input_dia.context().my_rank() == 0) {
            sLOG1 << "iteration" << iteration - 1
                  << "duplicates" << number_duplicates;
        }

        if (number_duplicates == 0) {
            auto sa =
                names
                .Map([](const IndexRank& ir) {
                         return ir.index;
                     });

            return sa.Collapse();
        }

        names =
            names
            .PrefixSum([](const IndexRank& a, const IndexRank& b) {
                           return IndexRank { b.index, a.rank + b.rank };
                       });

        if (debug_print)
            names.Keep().Print("names");
    }
}

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoubling(const InputDIA& input_dia, size_t input_size, bool packed) {
    if (input_dia.ctx().my_rank() == 0)
        LOG1 << "Running PrefixDoubling";

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;

        size_t input_bit_size = sizeof(Char) << 3;
    size_t k_fitting = sizeof(Index) / sizeof(Char);

    size_t iteration = 0;
    if (packed) {
        iteration = 0;
        size_t tmp = k_fitting;
        while (tmp >>= 1) ++iteration;
        --iteration;
    }

    DIA<IndexRank> rebucket;

    if (packed) {
        auto chars_sorted =
            input_dia
            .template FlatWindow<IndexRank>(
                k_fitting,
                [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    size_t result = rb[0];
                    for (size_t i = 1; i < k_fitting; ++i)
                        result = (result << input_bit_size) | rb[i];
                    emit(IndexRank { Index(index), Index(result) });
                    if (index + k_fitting == input_size) {
                        for (size_t i = 1; i < k_fitting; ++i) {
                            result = rb[i];
                            for (size_t j = i + 1; j < k_fitting; ++j)
                                result = (result << input_bit_size) | rb[j];
                            result <<= i * input_bit_size;
                            emit(IndexRank { Index(index + i), Index(result) });
                        }
                    }
                })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                    return a.rank < b.rank;
                });

            if (debug_print)
                chars_sorted.Keep().Print("chars_sorted packed");

        rebucket =
            chars_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (index == 0)
                        emit(IndexRank { rb[0].index, Index(0) });
                    emit(IndexRank { rb[1].index, 
                        Index(rb[0].rank == rb[1].rank ? 0 : index + 1) });
                });
    }
    else {
        auto chars_sorted =
            input_dia
            .template FlatWindow<CharCharIndex>(
                2,
                [](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    emit(CharCharIndex {
                             { rb[0], rb[1] }, Index(index)
                         });
                },
                [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    if (index + 1 == input_size) {
                        // emit CharCharIndex for last suffix position
                        emit(CharCharIndex {
                                 { rb[0], std::numeric_limits<Char>::lowest() },
                                 Index(index)
                             });
                    }
                })
            .Sort();

        if (debug_print)
            chars_sorted.Keep().Print("chars_sorted");

        rebucket =
            chars_sorted.Keep()
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                    if (index == 0) emit(IndexRank { rb[0].index, Index(0) });
                    emit(IndexRank { rb[1].index, 
                        Index(rb[0] == rb[1] ? 0 : index + 1) });
                });
    }

    auto number_duplicates =
        rebucket.Keep()
        .Filter([](const IndexRank& ir) {
                return ir.rank == Index(0);
            })
        .Size();

    // The first rank is always 0 and all other duplicates have "rank" 0
    // before we compute the correct new rank.
    if (number_duplicates == 1) {
        if (input_dia.context().my_rank() == 0)
            sLOG1 << "Finished before doubling in loop.";

        auto sa =
            rebucket
            .Map([](const IndexRank& ir) {
                    return ir.index;
                });
        return sa.Collapse();
    }

    rebucket =
        rebucket
        .PrefixSum([](const IndexRank& a, const IndexRank& b) {
                return IndexRank { b.index, std::max<Index>(a.rank, b.rank) };
            });

    if (debug_print)
        rebucket.Keep().Print("rebucket");

    // size_t iteration = 0;
    while (true) {
        auto isa =
            rebucket
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      return a.index < b.index;
                  });

        if (debug_print)
            isa.Keep().Print("isa");

        size_t shift_by = (1 << iteration++) + 1;

        auto triple_sorted =
            isa
            .template FlatWindow<IndexRankRank>(
                shift_by,
                [](size_t /*index*/, const RingBuffer<IndexRank>& rb, auto emit) {
                    emit(IndexRankRank { rb[0].index, rb.front().rank, rb.back().rank });
                },
                [](size_t /*index*/, const RingBuffer<IndexRank>& rb, auto emit) {
                    emit(IndexRankRank { rb[0].index, rb[0].rank, Index(0) });
                })
            .Sort();

        if (debug_print)
            triple_sorted.Keep().Print("triple_sorted");

        rebucket =
            triple_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) emit(IndexRank { rb[0].index, Index(0) });
                    emit(IndexRank { rb[1].index,
                        Index(rb[0] == rb[1] ? 0 : index + 1) });
                });

        if (debug_print)
            rebucket.Keep().Print("rebucket");

        number_duplicates =
            rebucket.Keep()
            .Filter([](const IndexRank& ir) {
                    return ir.rank == Index(0);
                })
            .Size();

        if (input_dia.context().my_rank() == 0) {
            sLOG1 << "iteration" << iteration
                  << "duplicates" << number_duplicates - 1;
        }

        if (number_duplicates == 1) {
            auto sa =
                rebucket
                .Map([](const IndexRank& ir) {
                        return ir.index;
                    });
            return sa.Collapse();
        }

        rebucket =
            rebucket
            .PrefixSum([](const IndexRank& a, const IndexRank& b) {
                    return IndexRank { b.index, std::max<Index>(a.rank, b.rank) };
                });

        if (debug_print)
            rebucket.Keep().Print("rebucket");
    }
}

template DIA<uint32_t> PrefixDoubling<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<uint64_t> PrefixDoubling<uint64_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<uint32_t> PrefixDoublingDementiev<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<uint64_t> PrefixDoublingDementiev<uint64_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<uint32_t> PrefixDoublingDiscardingDementiev<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<uint64_t> PrefixDoublingDiscardingDementiev<uint64_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);
} // namespace suffix_sorting
} // namespace examples

/******************************************************************************/
