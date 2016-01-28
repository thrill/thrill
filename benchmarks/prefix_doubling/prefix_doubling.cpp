/*******************************************************************************
 * benchmarks/prefix_doubling/prefix_doubling.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/allgather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/distribute_from.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/groupby.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/multiway_merge.hpp>

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

bool debug_print = false;
bool debug = true;

using namespace thrill; // NOLINT
using thrill::common::RingBuffer;

//! A pair (index, t=T[index]).
template <typename AlphabetType>  
struct IndexOneMer {
    size_t index;
    AlphabetType chars;

    bool operator == (const IndexOneMer& b) const {
        return chars == b.chars;
    }

    bool operator < (const IndexOneMer& b) const {
        return chars < b.chars;
    }

    friend std::ostream& operator << (std::ostream& os, const IndexOneMer& iom) {
        return os << '[' << iom.index << ',' << iom.chars << ']';
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename CharsType> 
struct IndexKMer {
    size_t    index;
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
struct IndexRank {
    size_t index;
    size_t rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& ri) {
        return os << '(' << ri.index << '|' << ri.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

//! A triple (rank_1, rank_2, index)
struct IndexRankRank {
    size_t index;
    size_t rank1;
    size_t rank2;

    //! Two IndexRankRanks are equal iff their ranks are equal.
    bool operator == (const IndexRankRank& b) const {
        return rank1 == b.rank1 and rank2 == b.rank2;
    }

    //! A IndexRankRank is smaller than another iff either its fist rank is smaller
    //! or if the first ranks are equal if its second rank is smaller.
    bool operator < (const IndexRankRank& b) const {
        return rank1 < b.rank1 or (rank1 == b.rank1 and rank2 < b.rank2);
    }

    friend std::ostream& operator << (std::ostream& os, const IndexRankRank& rri) {
        return os << "( i: " << rri.index << "| r1: " << rri.rank1 << "| r2: " << rri.rank2 << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Char>
struct Index3 {
    size_t index;
    size_t next;
    Char   ch;

    friend std::ostream& operator << (std::ostream& os, const Index3& i) {
        return os << "(index=" << i.index << " next=" << i.next << " ch=" << i.ch << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename InputDIA, typename SuffixArrayDIA>
bool CheckSA(const InputDIA& input, const SuffixArrayDIA& suffix_array) {

    Context& ctx = input.ctx();

    using Char = typename InputDIA::ValueType;
    using Index3 = ::Index3<Char>;

    size_t input_size = input.Size();

    auto isa_pair =
        suffix_array
        // build tuples with index: (SA[i]) -> (i, SA[i]),
        .Zip(Generate(ctx, input_size),
             [](size_t sa, size_t i) {
                 return IndexRank { sa, i };
             })
        // take (i, SA[i]) and sort to (ISA[i], i)
        .Sort([](const IndexRank& a, const IndexRank& b) {
                  return a.index < b.index;
              });

    // Zip (ISA[i], i) with [0,n) and check that the second component was a
    // permutation of [0,n)
    size_t perm_check =
        isa_pair
        .Zip(Generate(ctx, input_size),
             [](const IndexRank& ir, size_t index) -> size_t {
                 return ir.index == index ? 0 : 1;
             })
        // sum over all boolean values.
        .Sum();

    if (perm_check != 0) {
        LOG1 << "Error: suffix array is not a permutation of 0..n-1.";
        return false;
    }

    using IndexPair = std::pair<size_t, size_t>;

    auto order_check =
        isa_pair
        // extract ISA[i]
        .Map([](const IndexRank& ir) { return ir.rank; })
        // build (ISA[i], ISA[i+1], T[i])
        .template FlatWindow<IndexPair>(
            2, [input_size](size_t index, const RingBuffer<size_t>& rb, auto emit) {
                emit(IndexPair { rb[0], rb[1] });
                if (index == input_size - 2) {
                    // emit sentinel at end
                    emit(IndexPair { rb[1], input_size });
                }
            })
        .Zip(input,
             [](const std::pair<size_t, size_t>& isa_pair, const Char& ch) {
                 return Index3 { isa_pair.first, isa_pair.second, ch };
             })
        // and sort to (i, ISA[SA[i]+1], T[SA[i]])
        .Sort([](const Index3& a, const Index3& b) {
                  return a.index < b.index;
              });

    // order_check.Print("order_check");

    size_t order_check_sum =
        order_check
        // check that no pair violates the order
        .Window(2, [input_size](size_t index, const RingBuffer<Index3>& rb) -> size_t {

                    if (rb[0].ch > rb[1].ch) {
                        // simple check of first character of suffix failed.
                        LOG1 << "Error: suffix array position "
                             << index << " ordered incorrectly.";
                        return 1;
                    }
                    else if (rb[0].ch == rb[1].ch) {
                        if (rb[1].next == input_size) {
                            // last suffix of string must be first among those with
                            // same first character
                            LOG1 << "Error: suffix array position "
                                 << index << " ordered incorrectly.";
                            return 1;
                        }
                        if (rb[0].next != input_size && rb[0].next > rb[1].next) {
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
        .Sum();

    return (order_check_sum == 0);
}

// rebucket_sa = (index=rebucket, rank=sa)
// DIA<size_t> PartPrefixDoubling(DIA<IndexRankRank>& triple_sorted, size_t input_size, size_t shifted_exp) {
    // while(true) {
    //     size_t non_singletons =
    //         triple_sorted
    //         .template FlatWindow<uint8_t>(
    //             2,
    //             [&](size_t /*index*/, const RingBuffer<IndexRankRank>& rb, auto emit) {
    //                 if (rb[0] == rb[1]) emit(1);
    //             }
    //         ).Sum();

    //     DIA<size_t> sa =
    //         triple_sorted
    //         .Map([](const IndexRankRank& rri) {
    //             return rri.index;
    //     }).Cache();

    //     if (debug_print)
    //         sa.Print("sa");

    //     // If each suffix is unique regarding their 2h-prefix, we have computed
    //     // the suffix array and can return it. 
    //     if (non_singletons == 0) {
    //         die_unless(CheckSA(input_dia, sa));
    //         return sa.Collapse();
    //     }

    //     DIA<size_t> rebucket =
    //         triple_sorted
    //         .template FlatWindow<size_t>(
    //             2,
    //             [input_size](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
    //                 if (index == 0) emit(0);
    //                 if (rb[0] == rb[1]) emit(0); 
    //                 else { emit(index + 1); }
    //                 if (index == input_size - 2) {
    //                     if (rb[0] == rb[1]) emit(0);
    //                     else emit(index + 2);
    //                 }
    //         })
    //         .PrefixSum([](const size_t a, const size_t b) {
    //             return a > b ? a : b;
    //         });

    //     DIA<IndexRank> isa =
    //         sa
    //         .Zip(
    //             rebucket,
    //             [](size_t s, size_t r) {
    //                 return IndexRank {r, s};
    //         })
    //         .Sort([](const IndexRank& a, const IndexRank& b) {
    //             return a.rank < b.rank;   
    //         });

    //     // We need to increment the size in advance because we get the size of the
    //     // already computed prefixes. 
    //     size_t shift_by = (1 << ++shifted_exp) + 1;
    //         LOG << "Shift the ISA by " << shift_by - 1 << " positions. Hence the window has size " << shift_by;

    //     auto triples =
    //         isa
    //         .template FlatWindow<IndexRankRank>(
    //             shift_by,
    //             [&](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
    //                 emit(IndexRankRank {rb[0].rank, rb[0].index, rb[shift_by - 1].index});
    //                 if(index == input_size - shift_by)
    //                     for(size_t i = 1; i < input_size - index; ++i)
    //                         emit(IndexRankRank {rb[i].rank, rb[i].index, 0});
    //         })
    //         .Keep();

    //     auto triples_sorted =
    //         triples
    //         .template FlatWindow<IndexRankRank>(
    //             3,
    //             [&](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
    //                 if (index == 0 and rb[0] == rb[1]) emit(rb[0]);
    //                 if (rb[0] == rb[1] or rb[1] == rb[2]) emit(rb[1]);
    //                 if (index == input_size - 3 and rb[1] == rb[2]) emit(rb[2]);
    //         })
    //         .Sort(
    //             [](const IndexRankRank& a, const IndexRankRank& b) {
    //                 return a < b;
    //         });

    //     auto triples_merged =
    //         triples
    //         .template FlatWindow<IndexRankRank>(
    //             3,
    //             [&](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
    //                 if (index == 0 and rb[0] != rb[1]) emit(rb[0]);
    //                 if (rb[0] != rb[1] and rb[1] != rb[2]) emit(rb[1]);
    //                 if (index == input_size - 3 and rb[1] != rb[2]) emit(rb[2]);
    //         })
    //         .Merge(
    //             triples_sorted,
    //             [](const IndexRankRank& a, const IndexRankRank& b) {

    //         });
    // }
//}

template <typename InputDIA>
DIA<size_t> PrefixDoubling(Context& /*ctx*/, const InputDIA& input_dia, size_t input_size, size_t /*computation_rounds*/) {
    
    using Char = typename InputDIA::ValueType;
    using IndexKMer = ::IndexKMer<size_t>;

    size_t input_bit_size = sizeof(Char) << 3;
    size_t k_fitting = (sizeof(size_t) << 3) / input_bit_size;

    auto one_mers_sorted = 
        input_dia
        .template FlatWindow<IndexKMer>(
            k_fitting,
            [&](size_t index, const RingBuffer<Char>& rb, auto emit) {
                size_t result = rb[0];
                for (size_t i = 1; i < k_fitting; ++i)
                    result = (result << input_bit_size) | rb[i];
                emit(IndexKMer {index, result});
                if (index == input_size - k_fitting) {
                    for (size_t i = 1; i < k_fitting; ++i) {
                        result = rb[i];
                        for (size_t j = i + 1; j < k_fitting; ++j)
                            result = (result << input_bit_size) | rb[j];
                        result <<= i * input_bit_size;
                        emit(IndexKMer {index + i, result});
                    }
                }
        })
        .Sort([](const IndexKMer& a, const IndexKMer& b) {
            return a < b;
        });

    if(debug_print)
        one_mers_sorted.Print("one_mers_sorted");

    DIA<size_t> rebucket =
        one_mers_sorted
        .template FlatWindow<size_t>(
            2,
            [&](size_t index, const RingBuffer<IndexKMer>& rb, auto emit) {
                if (index == 0) emit(0);
                if (rb[0] == rb[1]) emit(0);
                else emit(index + 1);
                if (index == input_size - 2) {
                    if (rb[0] == rb[1]) emit(0);
                    else emit(index + 2);
                }
        })
        .PrefixSum([](const size_t a, const size_t b) {
            return a > b ? a : b;
        });

    if (debug_print)
        rebucket.Print("rebucket");

    DIA<size_t> sa =
        one_mers_sorted
        .Map([](const IndexKMer& iom) {
            return iom.index;
        }).Cache();

    if (debug_print)
        sa.Print("sa");

    uint8_t shifted_exp = 0;
    while(true) {
        DIA<IndexRank> isa =
            sa
            .Zip(
                rebucket,
                [](size_t s, size_t r) {
                    return IndexRank {r, s};
            })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                return a.rank < b.rank;   
            });

        if (debug_print)
            isa.Print("isa");

        size_t shift_by = (1 << shifted_exp++) + 1;
        LOG << "Shift the ISA by " << shift_by - 1 << " positions. Hence the window has size " << shift_by;

        DIA<IndexRankRank> triple_sorted =
            isa
            .template FlatWindow<IndexRankRank>(
                shift_by,
                [&](size_t index, const RingBuffer<IndexRank>& rb, auto emit) {
                    emit(IndexRankRank {rb[0].rank, rb[0].index, rb[shift_by - 1].index});
                    if(index == input_size - shift_by)
                        for(size_t i = 1; i < input_size - index; ++i)
                            emit(IndexRankRank {rb[i].rank, rb[i].index, 0});
                }
            )
            .Sort([](const IndexRankRank& a, const IndexRankRank& b) {
                return a < b;
            });

        if (debug_print)
            triple_sorted.Print("triple_sorted");

        // If we don't care about the number of singletons, it's sufficient to test two.
        size_t non_singletons =
            triple_sorted
            .template FlatWindow<uint8_t>(
                2,
                [&](size_t /*index*/, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (rb[0] == rb[1]) emit(1);
                }
            ).Size();

        sa =
            triple_sorted
            .Map([](const IndexRankRank& rri) {
                return rri.index;
        }).Cache();

        if (debug_print)
            sa.Print("sa");

        // If each suffix is unique regarding their 2h-prefix, we have computed
        // the suffix array and can return it. 
        if (non_singletons == 0) {
            die_unless(CheckSA(input_dia, sa));
            return sa;
        }

        rebucket =
            triple_sorted
            .template FlatWindow<size_t>(
                2,
                [&](size_t index, const RingBuffer<IndexRankRank>& rb, auto emit) {
                    if (index == 0) emit(0);
                    if (rb[0] == rb[1]) emit(0); 
                    else { emit(index + 1); }
                    if (index == input_size - 2) {
                        if (rb[0] == rb[1]) emit(0);
                        else emit(index + 2);
                    }
            })
            .PrefixSum([](const size_t a, const size_t b) {
                return a > b ? a : b;
            });

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
        bool text_output_flag,
        bool check_flag,
        bool input_verbatim)
        : ctx_(ctx),
          input_path_(input_path), output_path_(output_path),
          text_output_flag_(text_output_flag),
          check_flag_(check_flag),
          input_verbatim_(input_verbatim) { }

    void Run() {
        if (input_verbatim_) {
            // take path as verbatim text
            std::vector<uint8_t> input_vec(input_path_.begin(), input_path_.end());
            auto input_dia = Distribute<uint8_t>(ctx_, input_vec);
            StartPrefixDoublingInput(input_dia, input_vec.size(), input_vec.size());
        } 
        else {
            auto input_dia = ReadBinary<uint8_t>(ctx_, input_path_);
            size_t input_size = input_dia.Size();
            StartPrefixDoublingInput(input_dia, input_size, input_size);
        }
    }

    template <typename InputDIA>
    void StartPrefixDoublingInput(const InputDIA& input_dia, uint64_t input_size, size_t computation_rounds) {

        auto suffix_array = PrefixDoubling(ctx_, input_dia, input_size, computation_rounds);
        if (output_path_.size()) {
            suffix_array.WriteBinary(output_path_);
        }

        if (check_flag_) {
            LOG1 << "checking suffix array...";

            //if (!CheckSA(input_dia, suffix_array)) {
            //    throw std::runtime_error("Suffix array is invalid!");
            //}
            //else {
            //    LOG1 << "okay.";
            //}
        }
    }

protected:
    Context& ctx_;

    std::string input_path_;
    std::string output_path_;

    bool text_output_flag_;
    bool check_flag_;
    bool input_verbatim_;
    
};

int main(int argc, char* argv[]) {
    common::CmdlineParser cp;

    cp.SetDescription("A prefix doubling suffix array construction algorithm.");
    cp.SetAuthor("Florian Kurpicz <florian.kurpicz@tu-dortmund.de>");

    std::string input_path, output_path;
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
    cp.AddFlag('d', "debug", debug_print,
               "Print debug info.");
    debug = debug_print;
    // process command line
    if (!cp.Process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            return StartPrefixDoubling(ctx,
                            input_path, output_path,
                            text_output_flag,
                            check_flag,
                            input_verbatim).Run();
        });
}
/******************************************************************************/
