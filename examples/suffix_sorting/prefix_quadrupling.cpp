/*******************************************************************************
 * examples/suffix_sorting/prefix_quadrupling.cpp
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

//! A pair (index, rank)
template <typename Index>
struct IndexRank {
    Index index;
    Index rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& ri) {
        return os << '(' << ri.index << '|' << ri.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

//! A pair (index, rank)
template <typename Index>
struct IndexQuadRank {
    Index index;
    Index rank[4];

    bool operator == (const IndexQuadRank& b) const {
        return std::tie(rank[0], rank[1], rank[2], rank[3])
               == std::tie(b.rank[0], b.rank[1], b.rank[2], b.rank[3]);
    }

    bool operator < (const IndexQuadRank& b) const {
        return rank[0] < b.rank[0] ||
               (rank[0] == b.rank[0] && rank[1] < b.rank[1]) ||
               (rank[0] == b.rank[0] && rank[1] == b.rank[1]
                && rank[2] < b.rank[2]) ||
               (rank[0] == b.rank[0] && rank[1] == b.rank[1]
                && rank[2] == b.rank[2] && rank[3] < b.rank[3]) ||
               (rank[0] == b.rank[0] && rank[1] == b.rank[1]
                && rank[2] == b.rank[2] && rank[3] == b.rank[3]
                && index > b.index);
    }

    friend std::ostream& operator << (std::ostream& os, const IndexQuadRank& ri) {
        return os << '(' << ri.index << '|' << ri.rank[0] << '|' << ri.rank[1]
                  << '|' << ri.rank[2] << '|' << ri.rank[3] << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename AlphabetType, typename Index>
struct QuadCharIndex {
    AlphabetType ch[4];
    Index        index;

    bool operator == (const QuadCharIndex& b) const {
        return std::tie(ch[0], ch[1], ch[2], ch[3])
               == std::tie(b.ch[0], b.ch[1], b.ch[2], b.ch[3]);
    }

    bool operator < (const QuadCharIndex& b) const {
        return std::tie(ch[0], ch[1], ch[2], ch[3])
               < std::tie(b.ch[0], b.ch[1], b.ch[2], b.ch[3]);
    }

    friend std::ostream& operator << (std::ostream& os, const QuadCharIndex& chars) {
        return os << '[' << chars.index << ": " << chars.ch[0] << ',' << chars.ch[1]
                  << ',' << chars.ch[2] << ',' << chars.ch[3] << ']';
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

//! A triple (index, rank [4], status)
template <typename Index>
struct IndexQuadRankStatus {
    Index  index;
    Index  rank[4];
    Status status;

    friend std::ostream& operator << (std::ostream& os, const IndexQuadRankStatus& iqrs) {
        return os << "(i: " << iqrs.index << "| r1: " << iqrs.rank[0] << "| r2: "
                  << iqrs.rank[1] << "| r3: " << iqrs.rank[2] << "| r4: "
                  << iqrs.rank[3] << "| s: " << static_cast<uint8_t>(iqrs.status) << ")";
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

template <typename Index, typename InputDIA>
DIA<Index> PrefixQuadruplingDiscarding(const InputDIA& input_dia, size_t input_size) {
    LOG1 << "Running PrefixQuadruplingDiscarding";

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexQuadRank = suffix_sorting::IndexQuadRank<Index>;
    using QuadCharIndex = suffix_sorting::QuadCharIndex<Char, Index>;
    using IndexQuadRankStatus = suffix_sorting::IndexQuadRankStatus<Index>;
    using IndexRankStatus = suffix_sorting::IndexRankStatus<Index>;
    using Index3Rank = suffix_sorting::Index3Rank<Index>;

    auto chars_sorted =
        input_dia
        .template FlatWindow<QuadCharIndex>(
            4,
            [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                emit(QuadCharIndex {
                         { rb[0], rb[1], rb[2], rb[3] }, Index(index)
                     });
            },
            [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                if (index == input_size - 3) {
                    emit(QuadCharIndex {
                             { rb[0], rb[1], rb[2],
                               std::numeric_limits<Char>::lowest() },
                             Index(index)
                         });
                    emit(QuadCharIndex {
                             { rb[1], rb[2],
                               std::numeric_limits<Char>::lowest(),
                               std::numeric_limits<Char>::lowest() },
                             Index(index + 1)
                         });
                    emit(QuadCharIndex {
                             { rb[2],
                               std::numeric_limits<Char>::lowest(),
                               std::numeric_limits<Char>::lowest(),
                               std::numeric_limits<Char>::lowest() },
                             Index(index + 2)
                         });
                }
            })
        .Sort();

    if (debug_print)
        chars_sorted.Keep().Print("chars_sorted");

    auto names =
        chars_sorted
        .template FlatWindow<IndexRank>(
            2,
            [](size_t index, const RingBuffer<QuadCharIndex>& rb, auto emit) {
                if (index == 0)
                    emit(IndexRank { rb[0].index, Index(1) });
                if (rb[0] == rb[1])
                    emit(IndexRank { rb[1].index, Index(0) });
                else
                    emit(IndexRank { rb[1].index, Index(index + 2) });
            })
        .PrefixSum([](const IndexRank& a, const IndexRank& b) {
                       return IndexRank {
                           b.index,
                           (a.rank > b.rank ? a.rank : b.rank)
                       };
                   });

    if (debug_print)
        names.Keep().Print("names");

    auto names_unique =
        names
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
                if (index == input_size - 3) {
                    Status status = rb[1].rank != rb[2].rank ? Status::UNIQUE : Status::UNDECIDED;
                    emit(IndexRankStatus { rb[2].index, rb[2].rank, status });
                }
            });

    size_t iteration = 1;

    auto names_unique_sorted =
        names_unique.Keep()
        .Sort([iteration](const IndexRankStatus& a, const IndexRankStatus& b) {
                  Index mod_mask = (Index(1) << (iteration << 1)) - 1;
                  Index div_mask = ~mod_mask;

                  if ((a.index & mod_mask) == (b.index & mod_mask))
                      return (a.index & div_mask) < (b.index & div_mask);
                  else
                      return (a.index & mod_mask) < (b.index & mod_mask);
              });

    if (debug_print)
        names_unique_sorted.Keep().Print("Names unique sorted");

    std::vector<DIA<IndexQuadRank> > fully_discarded;

    while (true) {
        size_t next_index = size_t(1) << (iteration << 1);
        size_t names_size = names_unique_sorted.Keep().Size();

        ++iteration;

        auto discarded_names =
            names_unique_sorted.Keep()
            .template FlatWindow<IndexQuadRankStatus>(
                5,
                [=](size_t index, const RingBuffer<IndexRankStatus>& rb, auto emit) {
                    // Discarded names (we need to change the status since we remove it one step later)
                    if (index == 0) {
                        if (rb[0].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[0].index,
                                     { rb[0].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                        if (rb[1].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[1].index,
                                     { rb[1].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                        if (rb[2].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[2].index,
                                     { rb[2].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                        if (rb[3].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[3].index,
                                     { rb[3].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                    }
                    if (rb[4].status == Status::UNIQUE) {
                        if (rb[0].status == Status::UNIQUE || rb[1].status == Status::UNIQUE ||
                            rb[2].status == Status::UNIQUE || rb[3].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[4].index,
                                     { rb[4].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                        else
                            emit(IndexQuadRankStatus {
                                     rb[4].index,
                                     { rb[4].rank, Index(0), Index(0), Index(0) },
                                     Status::UNIQUE
                                 });
                    }
                    if (rb[0].status == Status::UNDECIDED) {
                        Index rank1 = (rb[0].index + Index(next_index) == rb[1].index) ? rb[1].rank : Index(0);
                        Index rank2 = (rb[0].index + 2 * Index(next_index) == rb[2].index) ? rb[2].rank : Index(0);
                        Index rank3 = (rb[0].index + 3 * Index(next_index) == rb[3].index) ? rb[3].rank : Index(0);
                        emit(IndexQuadRankStatus { rb[0].index, { rb[0].rank, rank1, rank2, rank3 }, Status::UNDECIDED });
                    }
                },
                [=](size_t index, const RingBuffer<IndexRankStatus>& rb, auto emit) {
                    if (index == 0) {
                        if (rb[0].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[0].index,
                                     { rb[0].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                        else     //(rb[0].status == Status::UNDECIDED)
                            emit(IndexQuadRankStatus {
                                     rb[0].index,
                                     { rb[0].rank, Index(0), Index(0), Index(0) },
                                     Status::UNDECIDED
                                 });

                        if (rb.size() > 1 && rb[1].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[1].index,
                                     { rb[1].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                        else if (rb.size() > 1)     // && rb[1].status == Status::UNDECIDED)
                            emit(IndexQuadRankStatus {
                                     rb[1].index,
                                     { rb[1].rank, Index(0), Index(0), Index(0) },
                                     Status::UNDECIDED
                                 });

                        if (rb.size() > 2 && rb[2].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[2].index,
                                     { rb[2].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                        else if (rb.size() > 2)     // && rb[2].status == Status::UNDECIDED)
                            emit(IndexQuadRankStatus {
                                     rb[2].index,
                                     { rb[2].rank, Index(0), Index(0), Index(0) },
                                     Status::UNDECIDED
                                 });

                        if (rb.size() > 3 && rb[3].status == Status::UNIQUE)
                            emit(IndexQuadRankStatus {
                                     rb[3].index,
                                     { rb[3].rank, Index(0), Index(0), Index(0) },
                                     Status::FULLY_DISCARDED
                                 });
                        else if (rb.size() > 3)     // && rb[3].status == Status::UNDECIDED)
                            emit(IndexQuadRankStatus {
                                     rb[3].index,
                                     { rb[3].rank, Index(0), Index(0), Index(0) },
                                     Status::UNDECIDED
                                 });
                    }
                    if (index == names_size - 4) {
                        Index rank1;
                        Index rank2;
                        Index rank3;

                        if (rb[0].status == Status::UNDECIDED) {
                            rank1 = (rb[0].index + Index(next_index) == rb[1].index) ? rb[1].rank : Index(0);
                            rank2 = (rb[0].index + 2 * Index(next_index) == rb[2].index) ? rb[2].rank : Index(0);
                            rank3 = (rb[0].index + 3 * Index(next_index) == rb[3].index) ? rb[3].rank : Index(0);
                            emit(IndexQuadRankStatus { rb[0].index, { rb[0].rank, rank1, rank2, rank3 }, Status::UNDECIDED });
                        }
                        if (rb[1].status == Status::UNDECIDED) {
                            rank1 = (rb[1].index + Index(next_index) == rb[2].index) ? rb[2].rank : Index(0);
                            rank2 = (rb[1].index + 2 * Index(next_index) == rb[3].index) ? rb[3].rank : Index(0);
                            emit(IndexQuadRankStatus { rb[1].index, { rb[1].rank, rank1, rank2, Index(0) }, Status::UNDECIDED });
                        }
                        if (rb[2].status == Status::UNDECIDED) {
                            rank1 = (rb[2].index + Index(next_index) == rb[3].index) ? rb[3].rank : Index(0);
                            emit(IndexQuadRankStatus { rb[2].index, { rb[2].rank, rank1, Index(0), Index(0) }, Status::UNDECIDED });
                        }
                        if (rb[3].status == Status::UNDECIDED)
                            emit(IndexQuadRankStatus { rb[3].index, { rb[3].rank, Index(0), Index(0), Index(0) }, Status::UNDECIDED });
                    }
                });

        auto new_decided =
            discarded_names.Keep()
            .Filter([](const IndexQuadRankStatus& iqrs) {
                        return iqrs.status == Status::FULLY_DISCARDED;
                    })
            .Map([](const IndexQuadRankStatus& iqrs) {
                     return IndexQuadRank {
                         iqrs.index,
                         { iqrs.rank[0], iqrs.rank[1], iqrs.rank[2], iqrs.rank[3] }
                     };
                 });

        if (debug_print)
            new_decided.Keep().Print("new_decided");

        auto partial_discarded =
            discarded_names.Keep()
            .Filter([](const IndexQuadRankStatus& iqrs) {
                        return iqrs.status == Status::UNIQUE;
                    })
            .Map([](const IndexQuadRankStatus& iqrs) {
                     return IndexRankStatus {
                         iqrs.index,
                         iqrs.rank[0],
                         Status::UNIQUE
                     };
                 });

        if (debug_print)
            partial_discarded.Keep().Print("partial_discarded");

        auto undiscarded =
            discarded_names
            .Filter([](const IndexQuadRankStatus& iqrs) {
                        return iqrs.status == Status::UNDECIDED;
                    })
            .Map([](const IndexQuadRankStatus& iqrs) {
                     return IndexQuadRank {
                         iqrs.index,
                         { iqrs.rank[0], iqrs.rank[1], iqrs.rank[2], iqrs.rank[3] }
                     };
                 })
            .Sort([](const IndexQuadRank& a, const IndexQuadRank& b) {
                      return a < b;
                  });

        if (debug_print)
            undiscarded.Keep().Print("undiscarded");

        fully_discarded.emplace_back(new_decided.Cache());

        size_t duplicates = undiscarded.Keep().Size();

        if (input_dia.context().my_rank() == 0) {
            sLOG1 << "iteration" << iteration - 1
                  << "duplicates" << duplicates;
        }

        if (duplicates == 0) {
            // auto res = Union(fully_discarded);
            auto sa =
                Union(fully_discarded)
                .Sort()
                .Map([](const IndexQuadRank& iqr) {
                         return iqr.index;
                     });
            return sa.Collapse();
        }

        auto new_ranks =
            undiscarded
            .template FlatWindow<Index3Rank>(
                2,
                [=](size_t index, const RingBuffer<IndexQuadRank>& rb, auto emit) {
                    if (index == 0) {
                        emit(Index3Rank { rb[0].index, Index(0), Index(0), rb[0].rank[0] });
                    }
                    Index i = rb[0].rank[0] == rb[1].rank[0] ? Index(0) : Index(index + 1);
                    Index r;
                    if (rb[0].rank[0] != rb[1].rank[0])
                        r = Index(index + 1);
                    else
                        r = (rb[0].rank[1] == rb[1].rank[1] &&
                             rb[0].rank[2] == rb[1].rank[2] &&
                             rb[0].rank[3] == rb[1].rank[3]) ? Index(0) : Index(index + 1);
                    emit(Index3Rank { rb[1].index, i, r, rb[1].rank[0] });
                },
                [](size_t index, const RingBuffer<IndexQuadRank>& rb, auto emit) {
                    if (index == 0)
                        emit(Index3Rank { rb[0].index, Index(0), Index(0), rb[0].rank[0] });
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

        // new_ranks.Keep().Print("new_ranks");

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
                    if (index == duplicates - 3) {
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

        if (debug_print)
            names_unique.Keep().Print("names_unique");

        names_unique_sorted =
            names_unique
            .Union(partial_discarded)
            .Sort([iteration](const IndexRankStatus& a, const IndexRankStatus& b) {
                      Index mod_mask = (Index(1) << (iteration << 1)) - 1;
                      Index div_mask = ~mod_mask;

                      if ((a.index & mod_mask) == (b.index & mod_mask))
                          return (a.index & div_mask) < (b.index & div_mask);
                      else
                          return (a.index & mod_mask) < (b.index & mod_mask);
                  });

        if (debug_print)
            names_unique_sorted.Keep().Print("names_unique_sorted");
    }
}

template <typename Index, typename InputDIA>
DIA<Index> PrefixQuadrupling(const InputDIA& input_dia, size_t input_size) {
    LOG1 << "Running PrefixQuadrupling";

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexQuadRank = suffix_sorting::IndexQuadRank<Index>;
    using QuadCharIndex = suffix_sorting::QuadCharIndex<Char, Index>;

    auto chars_sorted =
        input_dia
        .template FlatWindow<QuadCharIndex>(
            4,
            [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                emit(QuadCharIndex {
                         { rb[0], rb[1], rb[2], rb[3] }, Index(index)
                     });
            },
            [=](size_t index, const RingBuffer<Char>& rb, auto emit) {
                if (index == input_size - 3) {
                    emit(QuadCharIndex {
                             { rb[0], rb[1], rb[2],
                               std::numeric_limits<Char>::lowest() },
                             Index(index)
                         });
                    emit(QuadCharIndex {
                             { rb[1], rb[2],
                               std::numeric_limits<Char>::lowest(),
                               std::numeric_limits<Char>::lowest() },
                             Index(index + 1)
                         });
                    emit(QuadCharIndex {
                             { rb[2],
                               std::numeric_limits<Char>::lowest(),
                               std::numeric_limits<Char>::lowest(),
                               std::numeric_limits<Char>::lowest() },
                             Index(index + 2)
                         });
                }
            })
        .Sort();

    auto names =
        chars_sorted
        .template FlatWindow<IndexRank>(
            2,
            [](size_t index, const RingBuffer<QuadCharIndex>& rb, auto emit) {
                if (index == 0)
                    emit(IndexRank { rb[0].index, Index(1) });
                if (rb[0] == rb[1])
                    emit(IndexRank { rb[1].index, Index(0) });
                else
                    emit(IndexRank { rb[1].index, Index(index + 2) });
            });

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

    size_t iteration = 1;
    while (true) {
        auto names_sorted =
            names
            .Sort([iteration](const IndexRank& a, const IndexRank& b) {
                      Index mod_mask = (Index(1) << (iteration << 1)) - 1;
                      Index div_mask = ~mod_mask;

                      if ((a.index & mod_mask) == (b.index & mod_mask))
                          return (a.index & div_mask) < (b.index & div_mask);
                      else
                          return (a.index & mod_mask) < (b.index & mod_mask);
                  });

        size_t next_index = size_t(1) << (iteration << 1);
        ++iteration;

        auto triple_sorted =
            names_sorted
            .template FlatWindow<IndexQuadRank>(
                4,
                [=](size_t /*index*/, const RingBuffer<IndexRank>& rb, auto emit) {
                    Index rank1 = (rb[0].index + Index(next_index) == rb[1].index) ? rb[1].rank : Index(0);
                    Index rank2 = (rb[0].index + 2 * Index(next_index) == rb[2].index) ? rb[2].rank : Index(0);
                    Index rank3 = (rb[0].index + 3 * Index(next_index) == rb[3].index) ? rb[3].rank : Index(0);
                    emit(IndexQuadRank { rb[0].index, { rb[0].rank, rank1, rank2, rank3 }
                         });
                },
                [=](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (index == input_size - 3) {
                        Index rank1 = (rb[0].index + Index(next_index) == rb[1].index) ? rb[1].rank : Index(0);
                        Index rank2 = (rb[0].index + 2 * Index(next_index) == rb[2].index) ? rb[2].rank : Index(0);
                        emit(IndexQuadRank { rb[0].index, { rb[0].rank, rank1, rank2, Index(0) }
                             });

                        rank1 = (rb[1].index + Index(next_index) == rb[2].index) ? rb[2].rank : Index(0);
                        emit(IndexQuadRank { rb[1].index, { rb[1].rank, rank1, Index(0), Index(0) }
                             });
                        emit(IndexQuadRank { rb[2].index, { rb[2].rank, Index(0), Index(0), Index(0) }
                             });
                    }
                })
            .Sort();

        names =
            triple_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<IndexQuadRank>& rb, auto emit) {
                    if (index == 0) emit(IndexRank { rb[0].index, Index(1) });

                    if (rb[0] == rb[1] && (rb[0].rank[1] != Index(0) || rb[0].rank[2] != Index(0) || rb[0].rank[3] != Index(0)))
                        emit(IndexRank { rb[1].index, Index(0) });
                    else
                        emit(IndexRank { rb[1].index, Index(1) });
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
                           return IndexRank { b.index, std::max(a.rank, b.rank) };
                       });

        if (debug_print)
            names.Keep().Print("names");
    }
}

template DIA<uint32_t> PrefixQuadrupling<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

template DIA<uint64_t> PrefixQuadrupling<uint64_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

template DIA<uint32_t> PrefixQuadruplingDiscarding<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

template DIA<uint64_t> PrefixQuadruplingDiscarding<uint64_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

} // namespace suffix_sorting
} // namespace examples

/******************************************************************************/
