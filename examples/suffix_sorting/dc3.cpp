/*******************************************************************************
 * examples/suffix_sorting/dc3.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/allgather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/distribute_from.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
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

#include <algorithm>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

bool debug_print = false;

using namespace thrill; // NOLINT
using thrill::common::RingBuffer;

//! A triple with index (i,t_i,t_{i+1},t_{i+2}).
template <typename AlphabetType>
struct Chars {
    AlphabetType ch[3];

    bool operator == (const Chars& b) const {
        return std::equal(ch + 0, ch + 3, b.ch + 0);
    }

    bool operator < (const Chars& b) const {
        return std::lexicographical_compare(
            ch + 0, ch + 3, b.ch + 0, b.ch + 3);
    }

    friend std::ostream& operator << (std::ostream& os, const Chars& chars) {
        return os << '[' << chars.ch[0] << ',' << chars.ch[1]
                  << ',' << chars.ch[2] << ']';
    }

    static Chars EndSentinel() {
        return Chars {
                   {
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest()
                   }
        };
    }
} THRILL_ATTRIBUTE_PACKED;

//! A triple with index (i,t_i,t_{i+1},t_{i+2}).
template <typename AlphabetType>
struct IndexChars {
    size_t              index;
    Chars<AlphabetType> chars;

    friend std::ostream& operator << (std::ostream& os, const IndexChars& tc) {
        return os << '[' << tc.index << '|' << tc.chars << ']';
    }
} THRILL_ATTRIBUTE_PACKED;

//! A pair (index, rank)
struct IndexRank {
    size_t index;
    size_t rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& tr) {
        return os << '(' << tr.index << '|' << tr.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 0 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod0
{
    size_t       index;
    AlphabetType t0, t1;
    size_t       r1, r2;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod0& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1
                  << " r1=" << sf.r1 << " r2=" << sf.r2;
    }
} THRILL_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 1 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod1
{
    size_t       index;
    AlphabetType t0;
    size_t       r0, r1;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod1& sf) {
        return os << "i=" << sf.index
                  << " r0=" << sf.r0 << " t0=" << sf.t0 << " r1=" << sf.r1;
    }
} THRILL_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 2 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod2
{
    size_t       index;
    AlphabetType t0, t1;
    size_t       r0, r2;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod2& sf) {
        return os << "i=" << sf.index
                  << " r0=" << sf.r0 << " t0=" << sf.t0
                  << " t1=" << sf.t1 << " r2=" << sf.r2;
    }
} THRILL_ATTRIBUTE_PACKED;

//! Union of String Fragments with Index
template <typename AlphabetType>
struct StringFragment
{
    union {
        size_t                           index;
        StringFragmentMod0<AlphabetType> mod0;
        StringFragmentMod1<AlphabetType> mod1;
        StringFragmentMod2<AlphabetType> mod2;
    };

    StringFragment() = default;

    // conversion from StringFragmentMod0
    explicit StringFragment(const StringFragmentMod0<AlphabetType>& _mod0)
        : mod0(_mod0) { }

    // conversion from StringFragmentMod1
    explicit StringFragment(const StringFragmentMod1<AlphabetType>& _mod1)
        : mod1(_mod1) { }

    // conversion from StringFragmentMod2
    explicit StringFragment(const StringFragmentMod2<AlphabetType>& _mod2)
        : mod2(_mod2) { }

    friend std::ostream& operator << (std::ostream& os, const StringFragment& tc) {
        os << '[' << std::to_string(tc.index) << '|';
        if (tc.index % 3 == 0)
            return os << "0|" << tc.mod0 << ']';
        else if (tc.index % 3 == 1)
            return os << "1|" << tc.mod1 << ']';
        else if (tc.index % 3 == 2)
            return os << "2|" << tc.mod2 << ']';
        abort();
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

template <typename Char>
struct CharsRanks12 {
    Chars<Char> chars;
    size_t      rank1;
    size_t      rank2;

    friend std::ostream& operator << (std::ostream& os, const CharsRanks12& c) {
        return os << "(ch=" << c.chars << " r1=" << c.rank1 << " r2=" << c.rank2 << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Char>
struct IndexCR12Pair {
    size_t             index;
    CharsRanks12<Char> cr0;
    CharsRanks12<Char> cr1;
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

template <typename InputDIA>
DIA<size_t> DC3(Context& ctx, const InputDIA& input_dia, size_t input_size) {

    using Char = typename InputDIA::ValueType;
    using IndexChars = ::IndexChars<Char>;
    using Chars = ::Chars<Char>;

    auto triple_sorted =
        input_dia
        // map (t_i) -> (i,t_i,t_{i+1},t_{i+2}) where i neq 0 mod 3
        .template FlatWindow<IndexChars>(
            3, [input_size](size_t index, const RingBuffer<Char>& rb, auto emit) {
                if (index % 3 != 0)
                    emit(IndexChars { index, {
                                          { rb[0], rb[1], rb[2] }
                                      }
                         });

                if (index == input_size - 3) {
                    // emit last sentinel items.
                    if ((index + 1) % 3 != 0)
                        emit(IndexChars { index + 1, {
                                              { rb[1], rb[2], Char() }
                                          }
                             });
                    if ((index + 2) % 3 != 0)
                        emit(IndexChars { index + 2, {
                                              { rb[2], Char(), Char() }
                                          }
                             });

                    if (input_size % 3 == 1) {
                        // emit a sentinel tuple for inputs n % 3 == 1 to
                        // separate mod1 and mod2 strings in recursive
                        // subproblem. example which needs this: aaaaaaaaaa.
                        emit(IndexChars { index + 3, {
                                              { Char(), Char(), Char() }
                                          }
                             });
                    }
                }
            })
        // sort triples by contained letters
        .Sort([](const IndexChars& a, const IndexChars& b) {
                  return a.chars < b.chars;
              });

    if (debug_print)
        triple_sorted.Print("triple_sorted");

    // save triple's indexes (sorted by triple content) -> less storage
    auto triple_index_sorted =
        triple_sorted
        .Map([](const IndexChars& tc) { return tc.index; });

    auto triple_prerank_sums =
        triple_sorted
        .template FlatWindow<size_t>(
            2, [](size_t index, const RingBuffer<IndexChars>& rb, auto emit) {
                assert(rb.size() == 2);

                // emit one sentinel for index 0.
                if (index == 0) emit(0);

                // emit 0 or 1 depending on whether previous triple is equal
                emit(rb[0].chars == rb[1].chars ? 0 : 1);
            })
        .PrefixSum();

    if (debug_print)
        triple_prerank_sums.Print("triple_prerank_sums");

    // get the last element via an associative reduce.
    size_t max_lexname =
        triple_prerank_sums
        .Sum([](const size_t&, const size_t& b) { return b; });

    // compute the size of the 2/3 subproblem.
    size_t size_subp = (input_size / 3) * 2 + (input_size % 3 != 0);

    // size of the mod1 part of the recursive subproblem
    size_t size_mod1 = input_size / 3 + (input_size % 3 != 0);

    if (debug_print) {
        sLOG1 << "max_lexname=" << max_lexname
              << " size_subp=" << size_subp
              << " size_mod1=" << size_mod1;
    }

    DIA<IndexRank> ranks_rec;

    if (max_lexname + 1 != size_subp) {

        // some lexical name is not unique -> perform recursion on two
        // substrings (mod 1 and mod 2)

        // zip triples and ranks.
        auto triple_ranks =
            triple_index_sorted
            .Zip(triple_prerank_sums,
                 [](const size_t& triple_index, size_t rank) {
                     return IndexRank { triple_index, rank };
                 });

        if (debug_print)
            triple_ranks.Print("triple_ranks");

        // construct recursion string with all ranks at mod 1 indices followed
        // by all ranks at mod 2 indices.
        DIA<size_t> string_mod12 =
            triple_ranks
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      if (a.index % 3 == b.index % 3)
                          return a.index < b.index;
                      else
                          return a.index % 3 < b.index % 3;
                  })
            .Map([](const IndexRank& tr) {
                     return tr.rank;
                 })
            .Collapse();

        if (debug_print)
            string_mod12.Print("string_mod12");

        // auto suffix_array_rec = Recursion(string_mod12);
        auto suffix_array_rec = DC3(ctx, string_mod12, size_subp);

        // reverse suffix array of recursion strings to find ranks for mod 1
        // and mod 2 positions.

        if (debug_print)
            suffix_array_rec.Print("suffix_array_rec");

        assert(suffix_array_rec.Size() == size_subp);

        ranks_rec =
            suffix_array_rec
            .Zip(Generate(ctx, size_subp),
                 [](size_t sa, size_t i) {
                     return IndexRank { sa, i };
                 })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      // TODO(tb): change sort order for better locality later.
                      return a.index < b.index;
                  });

        if (debug_print)
            ranks_rec.Print("ranks_rec");
    }
    else {
        if (debug_print)
            triple_index_sorted.Print("triple_index_sorted");

        ranks_rec =
            triple_index_sorted
            .Zip(Generate(ctx, size_subp),
                 [](size_t sa, size_t i) {
                     return IndexRank { sa, i };
                 })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      // TODO(tb): change sort order for better locality later.
                      if (a.index % 3 == b.index % 3)
                          return a.index < b.index;
                      else
                          return a.index % 3 < b.index % 3;
                  })
            .Map([size_mod1](const IndexRank& a) {
                     return IndexRank { a.index % 3 == 1 ? 0 : size_mod1, a.rank };
                 })
            .Collapse();

        if (debug_print)
            ranks_rec.Print("ranks_rec");
    }

    // *** construct StringFragments ***

    auto triple_chars =
        input_dia
        // map (t_i) -> (i,t_i,t_{i+1},t_{i+2}) where i neq 0 mod 3
        .template FlatWindow<Chars>(
            3, [input_size](size_t index, const RingBuffer<Char>& rb, auto emit) {
                if (index % 3 == 0)
                    emit(Chars {
                             { rb[0], rb[1], rb[2] }
                         });

                if (index == input_size - 3) {
                    // emit sentinel
                    if ((index + 1) % 3 == 0)
                        emit(Chars {
                                 { rb[1], rb[2], Char() }
                             });
                    if ((index + 2) % 3 == 0)
                        emit(Chars {
                                 { rb[2], Char(), Char() }
                             });
                }
            });

    auto ranks_mod1 =
        ranks_rec
        .Filter([&](const IndexRank& a) {
                    return a.index < size_mod1;
                })
        .Map([](const IndexRank& a) {
                 // add one to ranks such that zero can be used as sentinel
                 // for suffixes beyond the end of the string.
                 return a.rank + 1;
             });

    auto ranks_mod2 =
        ranks_rec
        .Filter([&](const IndexRank& a) {
                    return a.index >= size_mod1;
                })
        .Map([](const IndexRank& a) {
                 return a.rank + 1;
             });

    if (debug_print) {
        triple_chars.Print("triple_chars");
        ranks_mod1.Print("ranks_mod1");
        ranks_mod2.Print("ranks_mod2");
    }

    assert(triple_chars.Size() == size_mod1);
    die_unless(ranks_mod1.Size() == size_mod1);
    assert(ranks_mod2.Size() == size_mod1 - (input_size % 3 ? 1 : 0));

    size_t zip_size = size_mod1;
    if (debug_print)
        sLOG1 << "zip_size" << zip_size;

    // Zip together the three arrays, create pairs, and extract needed
    // tuples into string fragments.

    using StringFragmentMod0 = ::StringFragmentMod0<Char>;
    using StringFragmentMod1 = ::StringFragmentMod1<Char>;
    using StringFragmentMod2 = ::StringFragmentMod2<Char>;

    using CharsRanks12 = ::CharsRanks12<Char>;
    using IndexCR12Pair = ::IndexCR12Pair<Char>;

    auto zip_triple_pairs1 =
        ZipPadding(
            [](const Chars& ch, const size_t& mod1, const size_t& mod2) {
                return CharsRanks12 { ch, mod1, mod2 };
            },
            std::make_tuple(Chars::EndSentinel(), 0, 0),
            triple_chars, ranks_mod1, ranks_mod2);

    if (debug_print)
        zip_triple_pairs1.Print("zip_triple_pairs1");

    auto zip_triple_pairs =
        zip_triple_pairs1
        .template FlatWindow<IndexCR12Pair>(
            2, [zip_size](size_t index, const RingBuffer<CharsRanks12>& rb, auto emit) {
                emit(IndexCR12Pair { 3 * index, rb[0], rb[1] });
                if (index == zip_size - 2) {
                    // emit last sentinel
                    emit(IndexCR12Pair { 3 * (index + 1), rb[1],
                                         CharsRanks12 { Chars::EndSentinel(), 0, 0 }
                         });
                }
            });

    auto fragments_mod0 =
        zip_triple_pairs
        .Map([](const IndexCR12Pair& ip) {
                 return StringFragmentMod0 {
                     ip.index,
                     ip.cr0.chars.ch[0], ip.cr0.chars.ch[1],
                     ip.cr0.rank1, ip.cr0.rank2
                 };
             })
        .Filter([input_size](const StringFragmentMod0& mod0) {
                    return mod0.index < input_size;
                });

    auto fragments_mod1 =
        zip_triple_pairs
        .Map([](const IndexCR12Pair& ip) {
                 return StringFragmentMod1 {
                     ip.index + 1,
                     ip.cr0.chars.ch[1],
                     ip.cr0.rank1, ip.cr0.rank2
                 };
             })
        .Filter([input_size](const StringFragmentMod1& mod1) {
                    return mod1.index < input_size;
                });

    auto fragments_mod2 =
        zip_triple_pairs
        .Map([](const IndexCR12Pair& ip) {
                 return StringFragmentMod2 {
                     ip.index + 2,
                     ip.cr0.chars.ch[2], ip.cr1.chars.ch[0],
                     ip.cr0.rank2, ip.cr1.rank1
                 };
             })
        .Filter([input_size](const StringFragmentMod2& mod2) {
                    return mod2.index < input_size;
                });

    if (debug_print) {
        fragments_mod0.Print("fragments_mod0");
        fragments_mod1.Print("fragments_mod1");
        fragments_mod2.Print("fragments_mod2");
    }

    // Sort the three string fragment sets

    auto sorted_fragments_mod0 =
        fragments_mod0
        .Sort([](const StringFragmentMod0& a, const StringFragmentMod0& b) {
                  return a.t0 == b.t0 ? a.r1 < b.r1 : a.t0 < b.t0;
              });

    auto sorted_fragments_mod1 =
        fragments_mod1
        .Sort([](const StringFragmentMod1& a, const StringFragmentMod1& b) {
                  return a.r0 < b.r0;
              });

    auto sorted_fragments_mod2 =
        fragments_mod2
        .Sort([](const StringFragmentMod2& a, const StringFragmentMod2& b) {
                  return a.r0 < b.r0;
              });

    if (debug_print) {
        sorted_fragments_mod0.Print("sorted_fragments_mod0");
        sorted_fragments_mod1.Print("sorted_fragments_mod1");
        sorted_fragments_mod2.Print("sorted_fragments_mod2");
    }

    using StringFragment = ::StringFragment<Char>;

    auto string_fragments_mod0 =
        sorted_fragments_mod0
        .Map([](const StringFragmentMod0& mod0)
             { return StringFragment(mod0); });

    auto string_fragments_mod1 =
        sorted_fragments_mod1
        .Map([](const StringFragmentMod1& mod1)
             { return StringFragment(mod1); });

    auto string_fragments_mod2 =
        sorted_fragments_mod2
        .Map([](const StringFragmentMod2& mod2)
             { return StringFragment(mod2); });

    auto fragment_comparator =
        [](const StringFragment& a, const StringFragment& b)
        {
            unsigned ai = a.index % 3, bi = b.index % 3;

            if (ai == 0 && bi == 0)
                return a.mod0.t0 == b.mod0.t0 ?
                       a.mod0.r1 < b.mod0.r1 :
                       a.mod0.t0 < b.mod0.t0;

            else if (ai == 0 && bi == 1)
                return a.mod0.t0 == b.mod1.t0 ?
                       a.mod0.r1 < b.mod1.r1 :
                       a.mod0.t0 < b.mod1.t0;

            else if (ai == 0 && bi == 2)
                return a.mod0.t0 == b.mod2.t0 ? (
                    a.mod0.t1 == b.mod2.t1 ?
                    a.mod0.r2 < b.mod2.r2 :
                    a.mod0.t1 < b.mod2.t1)
                       : a.mod0.t0 < b.mod2.t0;

            else if (ai == 1 && bi == 0)
                return a.mod1.t0 == b.mod0.t0 ?
                       a.mod1.r1 < b.mod0.r1 :
                       a.mod1.t0 < b.mod0.t0;

            else if (ai == 1 && bi == 1)
                return a.mod1.r0 < b.mod1.r0;

            else if (ai == 1 && bi == 2)
                return a.mod1.r0 < b.mod2.r0;

            else if (ai == 2 && bi == 0)
                return a.mod2.t0 == b.mod0.t0 ? (
                    a.mod2.t1 == b.mod0.t1 ?
                    a.mod2.r2 < b.mod0.r2 :
                    a.mod2.t1 < b.mod0.t1)
                       : a.mod2.t0 < b.mod0.t0;

            else if (ai == 2 && bi == 1)
                return a.mod2.r0 < b.mod1.r0;

            else if (ai == 2 && bi == 2)
                return a.mod2.r0 < b.mod2.r0;

            abort();
        };

    // merge and map to only suffix array

    auto suffix_array =
        Merge(fragment_comparator,
              string_fragments_mod0,
              string_fragments_mod1,
              string_fragments_mod2)
        .Map([](const StringFragment& a) { return a.index; });

    // debug output

    if (debug_print) {
        std::vector<Char> input_vec = input_dia.AllGather();
        std::vector<size_t> vec = suffix_array.AllGather();

        if (ctx.my_rank() == 0) {
            for (const size_t& index : vec)
            {
                std::cout << std::setw(5) << index << " =";
                for (size_t i = index; i < index + 64 && i < input_size; ++i) {
                    std::cout << ' ' << input_vec[i];
                }
                std::cout << '\n';
            }
        }
    }

    // check result

    die_unless(CheckSA(input_dia, suffix_array));

    return suffix_array.Collapse();
}

/*!
 * Class to encapsulate all
 */
class StartDC3
{
public:
    StartDC3(
        Context& ctx,
        const std::string& input_path, const std::string& output_path,
        uint64_t sizelimit,
        bool text_output_flag,
        bool check_flag,
        bool input_verbatim)
        : ctx_(ctx),
          input_path_(input_path), output_path_(output_path),
          sizelimit_(sizelimit),
          text_output_flag_(text_output_flag),
          check_flag_(check_flag),
          input_verbatim_(input_verbatim) { }

    void Run() {
        if (input_verbatim_) {
            // take path as verbatim text
            std::vector<uint8_t> input_vec(input_path_.begin(), input_path_.end());
            DIA<uint8_t> input_dia = Distribute<uint8_t>(ctx_, input_vec);
            StartDC3Input(input_dia, input_vec.size());
        }
        else if (input_path_ == "unary") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            DIA<uint8_t> input_dia = Generate(
                ctx_, [](size_t /* i */) { return uint8_t('a'); }, sizelimit_);
            StartDC3Input(input_dia, sizelimit_);
        }
        else if (input_path_ == "random") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            // share prng in Generate (just random numbers anyway)
            std::default_random_engine prng(std::random_device { } ());

            DIA<uint8_t> input_dia =
                Generate(
                    ctx_,
                    [&prng](size_t /* i */) {
                        return static_cast<uint8_t>(prng());
                    },
                    sizelimit_)
                // the random input _must_ be cached, otherwise it will be
                // regenerated ... and contain new numbers.
                .Cache();
            StartDC3Input(input_dia, sizelimit_);
        }
        else {
            DIA<uint8_t> input_dia = ReadBinary<uint8_t>(ctx_, input_path_);
            size_t input_size = input_dia.Size();
            StartDC3Input(input_dia, input_size);
        }
    }

    template <typename InputDIA>
    void StartDC3Input(const InputDIA& input_dia, uint64_t input_size) {

        // run DC3
        auto suffix_array = DC3(ctx_, input_dia, input_size);

        if (output_path_.size()) {
            suffix_array.WriteBinary(output_path_);
        }

        if (check_flag_) {
            LOG1 << "checking suffix array...";

            if (!CheckSA(input_dia, suffix_array)) {
                throw std::runtime_error("Suffix array is invalid!");
            }
            else {
                LOG1 << "okay.";
            }
        }
    }

protected:
    Context& ctx_;

    std::string input_path_;
    std::string output_path_;

    uint64_t sizelimit_;
    bool text_output_flag_;
    bool check_flag_;
    bool input_verbatim_;
};

int main(int argc, char* argv[]) {

    common::CmdlineParser cp;

    cp.SetDescription("DC3 aka skew3 algorithm for suffix array construction.");
    cp.SetAuthor("Timo Bingmann <tb@panthema.net>");

    std::string input_path, output_path;
    uint64_t sizelimit = std::numeric_limits<uint64_t>::max();
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
    cp.AddBytes('s', "size", sizelimit,
                "Cut input text to given size, e.g. 2 GiB. (TODO: not working)");
    cp.AddFlag('d', "debug", debug_print,
               "Print debug info.");

    // process command line
    if (!cp.Process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            return StartDC3(ctx,
                            input_path, output_path,
                            sizelimit,
                            text_output_flag,
                            check_flag,
                            input_verbatim).Run();
        });
}

/******************************************************************************/
