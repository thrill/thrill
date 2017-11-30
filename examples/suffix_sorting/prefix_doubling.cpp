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
        return os << "[i=" << iom.index << ",c=" << iom.chars << ']';
    }
} TLX_ATTRIBUTE_PACKED;

//! A pair (index, rank)
template <typename Index>
struct IndexRank {
    Index index;
    Index rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& ri) {
        return os << "(i=" << ri.index << ",r=" << ri.rank << ')';
    }
} TLX_ATTRIBUTE_PACKED;

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
        return std::tie(rank1, rank2, b.index) < std::tie(b.rank1, b.rank2, index);
    }

    friend std::ostream& operator << (std::ostream& os, const IndexRankRank& rri) {
        return os << "(i=" << rri.index << ",r1=" << rri.rank1 << ",r2=" << rri.rank2 << ")";
    }
} TLX_ATTRIBUTE_PACKED;

//! A triple with index (index, rank_1, rank_2, rank_3)
template <typename Index>
struct Index3Rank {
    Index index;
    Index rank1;
    Index rank2;
    Index rank3;

    friend std::ostream& operator << (std::ostream& os, const Index3Rank& irrr) {
        return os << "(i=" << irrr.index << ",r1=" << irrr.rank1
                  << ",r2=" << irrr.rank2 << ",r3=" << irrr.rank3 << ")";
    }
} TLX_ATTRIBUTE_PACKED;

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
        return os << "[ch0=" << cci.ch[0] << ",ch1=" << cci.ch[1]
                  << ",index=" << cci.index << ']';
    }
} TLX_ATTRIBUTE_PACKED;

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
        return os << "(index=" << irs.index << ",rank=" << irs.rank << ",status="
                  << static_cast<uint8_t>(irs.status) << ")";
    }
} TLX_ATTRIBUTE_PACKED;

//! A triple (index, rank, status)
template <typename Index>
struct IndexRankRankStatus {
    Index  index;
    Index  rank1;
    Index  rank2;
    Status status;

    friend std::ostream& operator << (std::ostream& os, const IndexRankRankStatus& irrs) {
        return os << "(index=" << irrs.index
                  << ",rank1=" << irrs.rank1 << ",rank2=" << irrs.rank2
                  << ",status=" << static_cast<uint8_t>(irrs.status) << ")";
    }
} TLX_ATTRIBUTE_PACKED;

//! take input and pack it into an array of Index characters
template <typename Index, typename InputDIA>
DIA<IndexRank<Index> > PrefixDoublingPack(
    const InputDIA& input_dia, size_t input_size, bool packed, size_t& iteration) {

    using Char = typename InputDIA::ValueType;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;

    if (packed && sizeof(Char) == 1) {

        // make histogram of characters
        std::vector<size_t> alpha_map(256);

        input_dia.Keep()
        .Map([&alpha_map](const Char& c) { alpha_map[c]++; return c; })
        .Size();

        alpha_map = input_dia.ctx().net.AllReduce(
            alpha_map, common::ComponentSum<std::vector<size_t> >());

        // determine alphabet size and map to names, keeping zero reserved
        size_t alphabet_size = 1;
        for (size_t i = 0; i < 256; ++i) {
            if (alpha_map[i] != 0) {
                alpha_map[i] = alphabet_size;
                alphabet_size++;
            }
        }

        // calculate number of characters fit into the bits of an Index, and the
        // next iteration
        size_t input_bit_size = tlx::integer_log2_ceil(alphabet_size);
        size_t k_fitting = 8 * sizeof(Index) / input_bit_size;
        iteration = tlx::integer_log2_floor(k_fitting);

        if (input_dia.ctx().my_rank() == 0) {
            LOG1 << "Packing:"
                 << " alphabet_size=" << alphabet_size - 1
                 << " input_bit_size=" << input_bit_size
                 << " k_fitting=" << k_fitting
                 << " next_iteration=" << iteration;
        }

        // pack and sort character groups
        auto chars_sorted =
            input_dia
            .template FlatWindow<IndexRank>(
                k_fitting,
                [=, alpha_map = std::move(alpha_map)](
                    size_t index, const RingBuffer<Char>& rb, auto emit) {

                    size_t result = alpha_map[rb[0]];
                    for (size_t i = 1; i < k_fitting; ++i)
                        result = (result << input_bit_size) | alpha_map[rb[i]];
                    emit(IndexRank { Index(index), Index(result) });

                    if (index + k_fitting == input_size) {
                        for (size_t i = 1; i < k_fitting; ++i) {
                            result = alpha_map[rb[i]];
                            for (size_t j = i + 1; j < k_fitting; ++j)
                                result = (result << input_bit_size) | alpha_map[rb[j]];
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

        return chars_sorted.template FlatWindow<IndexRank>(
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
        iteration = 1;

        // sorts pairs of characters to generate first iteration of lexnames

        auto chars_sorted =
            input_dia
            .template FlatWindow<CharCharIndex>(
                2,
                [](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    // emit CharCharIndex for each character pair
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
            // sort character pairs
            .Sort();

        if (debug_print)
            chars_sorted.Keep().Print("chars_sorted");

        return chars_sorted.template FlatWindow<IndexRank>(
            2,
            [](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                if (index == 0) {
                        // emit rank 1 for smallest character pair
                    emit(IndexRank { rb[0].index, Index(1) });
                }
                // emit next rank if character pair is unequal, else 0 which
                // will become the previous rank in the subsequent max().
                emit(IndexRank {
                         rb[1].index, Index(rb[0] == rb[1] ? 0 : index + 2)
                     });
            });
    }
}

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoublingSorting(
    const InputDIA& input_dia, size_t input_size, bool packed) {

    if (input_dia.ctx().my_rank() == 0)
        LOG1 << "Running PrefixDoublingSorting";

    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;

    size_t iteration;

    DIA<IndexRank> names = PrefixDoublingPack<Index>(
        input_dia, input_size, packed, iteration);

    if (debug_print)
        names.Keep().Print("names");

    // count number of duplicate character pairs, these are 0 indicators
    auto number_duplicates =
        names.Keep()
        .Filter([](const IndexRank& ir) {
                    return ir.rank == Index(0);
                })
        .Size();

    if (number_duplicates == 0) {
        if (input_dia.context().my_rank() == 0)
            sLOG1 << "Finished before doubling in loop";

        // suffix array already known, as character pairs are unique
        auto sa =
            names
            .Map([](const IndexRank& ir) {
                     return ir.index;
                 });

        return sa.Collapse();
    }

    // construct lexnames array by maxing ranks = filling in zeros with names.
    names =
        names
        .PrefixSum(
            [](const IndexRank& a, const IndexRank& b) {
                return IndexRank { b.index, std::max(a.rank, b.rank) };
            });

    if (debug_print)
        names.Keep().Print("names before loop");

    auto last_number_duplicates = number_duplicates;

    while (true) {
        // reorder names such that 2^k+i and 2^(k+1)+i are adjacent
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

        if (input_dia.context().my_rank() == 0) {
            sLOG1 << "next_index" << next_index;
        }

        auto triple =
            names_sorted
            .template FlatWindow<IndexRankRank>(
                2,
                [=](size_t /* index */, const RingBuffer<IndexRank>& rb, auto emit) {
                    emit(IndexRankRank {
                             rb[0].index, rb[0].rank,
                             // check if at boundary between 2^k+i range, emit 0
                             // if crossing boundary
                             (rb[0].index + Index(next_index) == rb[1].index)
                             ? rb[1].rank : Index(0)
                         });
                },
                [=](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (index + 1 == input_size)
                        emit(IndexRankRank { rb[0].index, rb[0].rank, Index(0) });
                });

        if (debug_print)
            triple.Keep().Print("triple");

        auto triple_sorted = triple.Sort();

        if (debug_print)
            triple_sorted.Keep().Print("triple_sorted");

        names =
            triple_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0)
                        emit(IndexRank { rb[0].index, Index(1) });

                    emit(IndexRank {
                             rb[1].index,
                             (rb[0] == rb[1] && rb[0].rank2 != Index(0))
                             ? Index(0) : Index(index + 2)
                         });
                });

        if (debug_print)
            names.Keep().Print("names indicator");

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

        if (number_duplicates > last_number_duplicates) {
            sLOG1 << "number_duplicates" << number_duplicates
                  << "last_number_duplicates" << last_number_duplicates;

            auto sa =
                names
                .Map([](const IndexRank& ir) {
                         return ir.index;
                     });

            return sa.Collapse();
        }

        last_number_duplicates = number_duplicates;

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

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoublingWindow(
    const InputDIA& input_dia, size_t input_size, bool packed) {

    if (input_dia.ctx().my_rank() == 0)
        LOG1 << "Running PrefixDoublingWindow";

    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;

    size_t iteration;

    DIA<IndexRank> names = PrefixDoublingPack<Index>(
        input_dia, input_size, packed, iteration);

    auto number_duplicates =
        names.Keep()
        .Filter([](const IndexRank& ir) {
                    return ir.rank == Index(0);
                })
        .Size();

    // The first rank is always 0 and all other duplicates have "rank" 0
    // before we compute the correct new rank.
    if (number_duplicates == 0) {
        if (input_dia.context().my_rank() == 0)
            sLOG1 << "Finished before doubling in loop.";

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

    while (true) {
        auto isa =
            names
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      return a.index < b.index;
                  });

        if (debug_print)
            isa.Keep().Print("isa");

        size_t shift_by = (size_t(1) << (iteration - 1)) + 1;

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

        names =
            triple_sorted
            .template FlatWindow<IndexRank>(
                2,
                [](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) emit(IndexRank { rb[0].index, Index(1) });
                    emit(IndexRank { rb[1].index,
                                     Index(rb[0] == rb[1] ? 0 : index + 2) });
                });

        if (debug_print)
            names.Keep().Print("names");

        number_duplicates =
            names.Keep()
            .Filter([](const IndexRank& ir) {
                        return ir.rank == Index(0);
                    })
            .Size();

        if (input_dia.context().my_rank() == 0) {
            sLOG1 << "iteration" << iteration
                  << "shift_by" << shift_by
                  << "duplicates" << number_duplicates;
        }
        ++iteration;

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

template <typename Index, typename InputDIA>
DIA<Index> PrefixDoublingDiscarding(
    const InputDIA& input_dia, size_t input_size, bool packed) {

    if (input_dia.ctx().my_rank() == 0)
        LOG1 << "Running PrefixDoublingDiscarding";

    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankStatus = suffix_sorting::IndexRankStatus<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;
    using Index3Rank = suffix_sorting::Index3Rank<Index>;
    using IndexRankRankStatus = suffix_sorting::IndexRankRankStatus<Index>;

    size_t iteration;

    DIA<IndexRank> names = PrefixDoublingPack<Index>(
        input_dia, input_size, packed, iteration);

    names =
        names.PrefixSum([](const IndexRank a, const IndexRank b) {
                            return IndexRank { b.index, std::max(a.rank, b.rank) };
                        });

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
                        if (rb[0].index + Index(1llu << (iteration - 1)) == rb[1].index)
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

template DIA<uint32_t> PrefixDoublingWindow<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<common::uint40> PrefixDoublingWindow<common::uint40>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<uint32_t> PrefixDoublingSorting<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<common::uint40> PrefixDoublingSorting<common::uint40>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<uint32_t> PrefixDoublingDiscarding<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

template DIA<common::uint40> PrefixDoublingDiscarding<common::uint40>(
    const DIA<uint8_t>& input_dia, size_t input_size, bool packed);

} // namespace suffix_sorting
} // namespace examples

/******************************************************************************/
