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
#include <thrill/api/equal_to_dia.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

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

//! A pair (rank, index)
template <typename Index>
struct IndexRank {
    Index index;
    Index rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& ri) {
        return os << '(' << ri.index << '|' << ri.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

//! A triple (rank_1, rank_2, index)
template <typename Index>
struct IndexRankRank {
    Index index;
    Index rank1;
    Index rank2;

    //! Two IndexRankRanks are equal iff their ranks are equal.
    bool operator == (const IndexRankRank& b) const {
        return rank1 == b.rank1 && rank2 == b.rank2;
    }

    //! A IndexRankRank is smaller than another iff either its fist rank is smaller
    //! or if the first ranks are equal if its second rank is smaller.
    bool operator < (const IndexRankRank& b) const {
        return rank1 < b.rank1 || (rank1 == b.rank1 && rank2 < b.rank2);
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

template <typename InputDIA>
DIA<size_t> PrefixDoublinDiscardingDementiev(const InputDIA& input_dia, size_t input_size) {
    using Char = typename InputDIA::ValueType;
    using Index = uint64_t;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;

    auto chars_sorted =
        input_dia
        .template FlatWindow<CharCharIndex>(
            2,
            [&](size_t index, const RingBuffer<Char>& rb, auto emit) {
                emit(CharCharIndex { rb[0], rb[1], index });
                if (index == input_size - 2)
                    emit(CharCharIndex { rb[1], std::numeric_limits<Char>::lowest(), index + 1 });
            })
        .Sort([](const CharCharIndex& a, const CharCharIndex& b) {
                  return a < b;
              });

    DIA<size_t> renamed_ranks =
        chars_sorted
        .template FlatWindow<size_t>(
            2,
            [&](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                if (index == 0) emit(1);
                if (rb[0] == rb[1]) emit(0);
                else emit(index + 2);
                if (index == input_size - 2) {
                    if (rb[0] == rb[1]) emit(0);
                    else emit(index + 3);
                }
            })
        .PrefixSum([](const size_t a, const size_t b) {
                       return a > b ? a : b;
                   });

    DIA<IndexRank> names =
        chars_sorted
        .Zip(
            renamed_ranks,
            [](const CharCharIndex& cci, const size_t r) {
                return IndexRank { cci.index, r };
            });

    while (true) { }
}

template <typename InputDIA>
DIA<uint64_t> PrefixDoublingDementiev(const InputDIA& input_dia, size_t input_size) {
    // enable online consume of DIA contents if not debugging.
    input_dia.context().enable_consume(!debug_print);

    LOG1 << "Running PrefixDoublingDementiev";

    using Char = typename InputDIA::ValueType;
    using Index = uint64_t;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using IndexRankRank = suffix_sorting::IndexRankRank<Index>;
    using CharCharIndex = suffix_sorting::CharCharIndex<Char, Index>;

    auto chars_sorted =
        input_dia
        .template FlatWindow<CharCharIndex>(
            2,
            [&](size_t index, const RingBuffer<Char>& rb, auto emit) {
                emit(CharCharIndex {
                         { rb[0], rb[1] }, index
                     });
                if (index == input_size - 2) {
                    emit(CharCharIndex {
                             { rb[1], std::numeric_limits<Char>::lowest() },
                             index + 1
                         });
                }
            })
        .Sort();

    auto renamed_ranks =
        chars_sorted.Keep()
        .template FlatWindow<Index>(
            2,
            [&](size_t index, const RingBuffer<CharCharIndex>& rb, auto emit) {
                if (index == 0) emit(1);
                if (rb[0] == rb[1]) emit(0);
                else emit(index + 2);
                if (index == input_size - 2) {
                    if (rb[0] == rb[1]) emit(0);
                    else emit(index + 3);
                }
            })
        .PrefixSum(common::maximum<Index>());

    Index non_singletons =
        renamed_ranks.Keep()
        .Window(
            2,
            [](size_t /* index */, const RingBuffer<Index>& rb) -> Index {
                return rb[0] == rb[1];
            })
        .Sum();

    if (non_singletons == 0) {
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

    Index shift_by = 1;
    while (true) {
        auto names_sorted =
            names
            .Sort([shift_by](const IndexRank& a, const IndexRank& b) {
                      Index mod_mask = (1 << shift_by) - 1;
                      Index div_mask = ~mod_mask;

                      if ((a.index & mod_mask) == (b.index & mod_mask))
                          return (a.index & div_mask) < (b.index & div_mask);
                      else
                          return (a.index & mod_mask) < (b.index & mod_mask);
                  });

        if (debug_print)  // If we have debug_print = true everything works fine.
            names_sorted.Print("names_sorted");

        size_t next_index = size_t(1) << shift_by++;

        auto triple_sorted =
            names_sorted
            .template FlatWindow<IndexRankRank>(
                2,
                [&](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    if (rb[0].index + next_index == rb[1].index)
                        emit(IndexRankRank { rb[0].index, rb[0].rank, rb[1].rank });
                    else
                        emit(IndexRankRank { rb[0].index, rb[0].rank, Index(0) });

                    if (index == input_size - 2)
                        emit(IndexRankRank { rb[1].index, rb[1].rank, Index(0) });
                })
            .Sort();

        renamed_ranks =
            triple_sorted.Keep()
            .template FlatWindow<Index>(
                2,
                [&](Index index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) emit(1);
                    if (rb[0] == rb[1]) emit(0);
                    else emit(index + 2);
                    if (index == input_size - 2) {
                        if (rb[0] == rb[1]) emit(0);
                        else emit(index + 3);
                    }
                })
            .PrefixSum(common::maximum<Index>());

        non_singletons =
            renamed_ranks.Keep()
            .Window(
                2,
                [](size_t /* index */, const RingBuffer<Index>& rb) -> Index {
                    return rb[0] == rb[1];
                })
            .Sum();

        LOG << "Non singletons " << non_singletons;

        if (non_singletons == 0) {
            auto sa =
                triple_sorted
                .Map([](const IndexRankRank& irr) {
                         return irr.index;
                     });

            if (debug_print)
                sa.Print("sa");

            return sa.Collapse();
        }

        names =
            triple_sorted
            .Zip(
                renamed_ranks,
                [](const IndexRankRank& irr, const Index r) {
                    return IndexRank { irr.index, r };
                });
    }
}

template <typename InputDIA>
auto PrefixDoubling(const InputDIA &input_dia, size_t input_size) {
    // enable online consume of DIA contents if not debugging.
    input_dia.context().enable_consume(!debug_print);

    using Char = typename InputDIA::ValueType;
    using Index = uint64_t;
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
                Index result = rb[0];
                for (size_t i = 1; i < k_fitting; ++i)
                    result = (result << input_bit_size) | rb[i];
                emit(IndexKMer { index, result });
                if (index == input_size - k_fitting) {
                    for (size_t i = 1; i < k_fitting; ++i) {
                        result = rb[i];
                        for (size_t j = i + 1; j < k_fitting; ++j)
                            result = (result << input_bit_size) | rb[j];
                        result <<= i * input_bit_size;
                        emit(IndexKMer { index + i, result });
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
            [&](size_t index, const RingBuffer<IndexKMer>& rb, auto emit) {
                if (index == 0) emit(Index(0));
                if (rb[0] == rb[1]) emit(Index(0));
                else emit(Index(index + 1));
                if (index == input_size - 2) {
                    if (rb[0] == rb[1]) emit(Index(0));
                    else emit(Index(index + 2));
                }
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

    uint8_t shifted_exp = 0;
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

        size_t shift_by = (1 << shifted_exp++) + 1;
        LOG << "Shift the ISA by " << shift_by - 1
            << " positions. Hence the window has size " << shift_by;

        DIA<IndexRankRank> triple_sorted =
            isa
            .template FlatWindow<IndexRankRank>(
                shift_by,
                [&](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    emit(IndexRankRank { rb[0].rank, rb[0].index, rb[shift_by - 1].index });
                    if (index == input_size - shift_by)
                        for (size_t i = 1; i < input_size - index; ++i)
                            emit(IndexRankRank { rb[i].rank, rb[i].index, 0 });
                }
                )
            .Sort([](const IndexRankRank& a, const IndexRankRank& b) {
                      return a < b;
                  });

        if (debug_print)
            triple_sorted.Print("triple_sorted");

        // If we don't care about the number of singletons, it's sufficient to
        // test two.
        Index non_singletons =
            triple_sorted.Keep()
            .Window(
                2,
                [&](size_t /* index */, const RingBuffer<IndexRankRank>& rb) -> Index {
                    return rb[0] == rb[1];
                })
            .Sum();

        sa =
            triple_sorted
            .Map([](const IndexRankRank& rri) { return rri.index; })
            .Collapse();

        if (debug_print)
            sa.Print("sa");

        // If each suffix is unique regarding their 2h-prefix, we have computed
        // the suffix array and can return it.
        if (non_singletons == 0) {
            return sa;
        }

        rebucket =
            triple_sorted
            .template FlatWindow<Index>(
                2,
                [&](Index index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) emit(0);
                    if (rb[0] == rb[1]) emit(0);
                    else { emit(index + 1);
                    }
                    if (index == input_size - 2) {
                        if (rb[0] == rb[1]) emit(0);
                        else emit(index + 2);
                    }
                })
            .PrefixSum(common::maximum<Index>());

        if (debug_print)
            rebucket.Print("rebucket");
    }
}

/*!
 * Class to encapsulate all
 */
class StartPrefixDoubling
{
public:
    StartPrefixDoubling(
        Context& ctx,
        const std::string& input_path, const std::string& output_path,
        const std::string& pd_algorithm,
        bool text_output_flag,
        bool check_flag,
        bool input_verbatim)
        : ctx_(ctx),
          input_path_(input_path), output_path_(output_path),
          pd_algorithm_(pd_algorithm),
          text_output_flag_(text_output_flag),
          check_flag_(check_flag),
          input_verbatim_(input_verbatim) { }

    void Run() {
        if (input_verbatim_) {
            // take path as verbatim text
            std::vector<uint8_t> input_vec(input_path_.begin(), input_path_.end());
            auto input_dia = EqualToDIA(ctx_, input_vec);
            StartPrefixDoublingInput(input_dia, input_vec.size());
        }
        else {
            auto input_dia = ReadBinary<uint8_t>(ctx_, input_path_);
            size_t input_size = input_dia.Size();
            StartPrefixDoublingInput(input_dia, input_size);
        }
    }

    template <typename InputDIA>
    void StartPrefixDoublingInput(
        const InputDIA& input_dia, uint64_t input_size) {

        DIA<uint64_t> suffix_array;
        if (pd_algorithm_ == "de") {
            suffix_array = PrefixDoublingDementiev(input_dia, input_size);
        }
        else {
            suffix_array = PrefixDoubling(input_dia, input_size);
        }

        if (check_flag_) {
            LOG1 << "checking suffix array...";
            die_unless(CheckSA(input_dia, suffix_array));
        }

        if (text_output_flag_) {
            suffix_array.Print("suffix_array");
        }

        if (output_path_.size()) {
            LOG1 << "writing suffix array to " << output_path_;
            suffix_array.WriteBinary(output_path_);
        }
    }

protected:
    Context& ctx_;

    std::string input_path_;
    std::string output_path_;
    std::string pd_algorithm_;

    bool text_output_flag_;
    bool check_flag_;
    bool input_verbatim_;
};

} // namespace suffix_sorting
} // namespace examples

int main(int argc, char* argv[]) {

    using namespace thrill; // NOLINT

    common::CmdlineParser cp;

    cp.SetDescription("A collection of prefix doubling suffix array construction algorithms.");
    cp.SetAuthor("Florian Kurpicz <florian.kurpicz@tu-dortmund.de>");

    std::string input_path, output_path;
    std::string pd_algorithm;
    bool text_output_flag = false;
    bool check_flag = false;
    bool input_verbatim = false;

    cp.AddParamString("input", input_path,
                      "Path to input file (or verbatim text).\n"
                      "  The special inputs 'random' and 'unary' generate "
                      "such text on-the-fly.");
    cp.AddFlag('c', "check", check_flag,
               "Check suffix array for correctness.");
    cp.AddFlag('t', "text", text_output_flag,
               "Print out suffix array in readable text.");
    cp.AddString('o', "output", output_path,
                 "Output suffix array to given path.");
    cp.AddFlag('v', "verbatim", input_verbatim,
               "Consider \"input\" as verbatim text to construct "
               "suffix array on.");
    cp.AddFlag('d', "debug", examples::suffix_sorting::debug_print,
               "Print debug info.");
    cp.AddString('a', "algorithm", pd_algorithm,
                 "The prefix doubling algorithm which is used to construct the "
                 "suffix array. [fl]ick (default) and [de]mentiev are "
                 "available.");
    // debug = debug_print;
    // process command line
    if (!cp.Process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            return examples::suffix_sorting::StartPrefixDoubling(
                ctx,
                input_path, output_path,
                pd_algorithm,
                text_output_flag,
                check_flag,
                input_verbatim).Run();
        });
}

/******************************************************************************/
