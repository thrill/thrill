/*******************************************************************************
 * examples/suffix_sorting/dc3.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/suffix_sorting/check_sa.hpp>
#include <examples/suffix_sorting/suffix_sorting.hpp>

#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/common/uint_types.hpp>

#include <algorithm>
#include <iomanip>
#include <limits>
#include <random>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace examples {
namespace suffix_sorting {
namespace dc3_local {

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
template <typename Index, typename AlphabetType>
struct IndexChars {
    Index               index;
    Chars<AlphabetType> chars;

    friend std::ostream& operator << (std::ostream& os, const IndexChars& tc) {
        return os << '[' << tc.index << '|' << tc.chars << ']';
    }
} THRILL_ATTRIBUTE_PACKED;

//! A pair (index, rank)
template <typename Index>
struct IndexRank {
    Index index;
    Index rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& tr) {
        return os << '(' << tr.index << '|' << tr.rank << ')';
    }
} THRILL_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 0 Mod 3.
template <typename Index, typename AlphabetType>
struct StringFragmentMod0 {
    Index        index;
    AlphabetType t0, t1;
    Index        r1, r2;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod0& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1
                  << " r1=" << sf.r1 << " r2=" << sf.r2;
    }
} THRILL_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 1 Mod 3.
template <typename Index, typename AlphabetType>
struct StringFragmentMod1 {
    Index        index;
    AlphabetType t0;
    Index        r0, r1;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod1& sf) {
        return os << "i=" << sf.index
                  << " r0=" << sf.r0 << " t0=" << sf.t0 << " r1=" << sf.r1;
    }
} THRILL_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 2 Mod 3.
template <typename Index, typename AlphabetType>
struct StringFragmentMod2 {
    Index        index;
    AlphabetType t0, t1;
    Index        r0, r2;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod2& sf) {
        return os << "i=" << sf.index
                  << " r0=" << sf.r0 << " t0=" << sf.t0
                  << " t1=" << sf.t1 << " r2=" << sf.r2;
    }
} THRILL_ATTRIBUTE_PACKED;

//! Union of String Fragments with Index
template <typename Index, typename AlphabetType>
struct StringFragment {
    union {
        Index                                   index;
        StringFragmentMod0<Index, AlphabetType> mod0;
        StringFragmentMod1<Index, AlphabetType> mod1;
        StringFragmentMod2<Index, AlphabetType> mod2;
    } THRILL_ATTRIBUTE_PACKED;

    StringFragment() = default;

    // conversion from StringFragmentMod0
    explicit StringFragment(const StringFragmentMod0<Index, AlphabetType>& _mod0)
        : mod0(_mod0) { }

    // conversion from StringFragmentMod1
    explicit StringFragment(const StringFragmentMod1<Index, AlphabetType>& _mod1)
        : mod1(_mod1) { }

    // conversion from StringFragmentMod2
    explicit StringFragment(const StringFragmentMod2<Index, AlphabetType>& _mod2)
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

template <typename StringFragment>
struct FragmentComparator {
    bool operator () (const StringFragment& a, const StringFragment& b) const {
        unsigned ai = a.index % 3, bi = b.index % 3;

        if (ai == 0 && bi == 0)
            return std::tie(a.mod0.t0, a.mod0.r1)
                   < std::tie(b.mod0.t0, b.mod0.r1);

        else if (ai == 0 && bi == 1)
            return std::tie(a.mod0.t0, a.mod0.r1)
                   < std::tie(b.mod1.t0, b.mod1.r1);

        else if (ai == 0 && bi == 2)
            return std::tie(a.mod0.t0, a.mod0.t1, a.mod0.r2)
                   < std::tie(b.mod2.t0, b.mod2.t1, b.mod2.r2);

        else if (ai == 1 && bi == 0)
            return std::tie(a.mod1.t0, a.mod1.r1)
                   < std::tie(b.mod0.t0, b.mod0.r1);

        else if (ai == 1 && bi == 1)
            return a.mod1.r0 < b.mod1.r0;

        else if (ai == 1 && bi == 2)
            return a.mod1.r0 < b.mod2.r0;

        else if (ai == 2 && bi == 0)
            return std::tie(a.mod2.t0, a.mod2.t1, a.mod2.r2)
                   < std::tie(b.mod0.t0, b.mod0.t1, b.mod0.r2);

        else if (ai == 2 && bi == 1)
            return a.mod2.r0 < b.mod1.r0;

        else if (ai == 2 && bi == 2)
            return a.mod2.r0 < b.mod2.r0;

        abort();
    }
};

template <typename Index, typename Char>
struct CharsRanks12 {
    Chars<Char> chars;
    Index       rank1;
    Index       rank2;

    friend std::ostream& operator << (std::ostream& os, const CharsRanks12& c) {
        return os << "(ch=" << c.chars << " r1=" << c.rank1 << " r2=" << c.rank2 << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Index, typename Char>
struct IndexCR12Pair {
    Index                     index;
    CharsRanks12<Index, Char> cr0;
    CharsRanks12<Index, Char> cr1;
} THRILL_ATTRIBUTE_PACKED;

} // namespace dc3_local

using namespace thrill; // NOLINT
using thrill::common::RingBuffer;

template <typename Index, typename InputDIA>
DIA<Index> DC3(const InputDIA& input_dia, size_t input_size) {

    using Char = typename InputDIA::ValueType;
    using IndexChars = dc3_local::IndexChars<Index, Char>;
    using IndexRank = dc3_local::IndexRank<Index>;
    using Chars = dc3_local::Chars<Char>;

    Context& ctx = input_dia.context();

    auto triple_sorted =
        input_dia.Keep()
        // map (t_i) -> (i,t_i,t_{i+1},t_{i+2}) where i neq 0 mod 3
        .template FlatWindow<IndexChars>(
            3, [input_size](size_t index, const RingBuffer<Char>& rb, auto emit) {
                if (index % 3 != 0)
                    emit(IndexChars { Index(index), {
                                          { rb[0], rb[1], rb[2] }
                                      }
                         });

                if (index + 3 == input_size) {
                    // emit last sentinel items.
                    if ((index + 1) % 3 != 0)
                        emit(IndexChars { Index(index + 1), {
                                              { rb[1], rb[2], Char() }
                                          }
                             });
                    if ((index + 2) % 3 != 0)
                        emit(IndexChars { Index(index + 2), {
                                              { rb[2], Char(), Char() }
                                          }
                             });

                    if (input_size % 3 == 1) {
                        // emit a sentinel tuple for inputs n % 3 == 1 to
                        // separate mod1 and mod2 strings in recursive
                        // subproblem. example which needs this: aaaaaaaaaa.
                        emit(IndexChars { Index(index + 3), Chars::EndSentinel() });
                    }
                }
            })
        // sort triples by contained letters
        .Sort([](const IndexChars& a, const IndexChars& b) {
                  return a.chars < b.chars;
              });

    if (debug_print)
        triple_sorted.Keep().Print("triple_sorted");

    // save triple's indexes (sorted by triple content) -> less storage
    auto triple_index_sorted =
        triple_sorted
        .Map([](const IndexChars& tc) { return tc.index; })
        .Cache();

    auto triple_prerank_sums =
        triple_sorted
        .template FlatWindow<Index>(
            2, [](size_t index, const RingBuffer<IndexChars>& rb, auto emit) {
                assert(rb.size() == 2);

                // emit one sentinel for index 0.
                if (index == 0) emit(0);

                // emit 0 or 1 depending on whether previous triple is equal
                emit(rb[0].chars == rb[1].chars ? 0 : 1);
            })
        .PrefixSum();

    if (debug_print)
        triple_prerank_sums.Keep().Print("triple_prerank_sums");

    // get the last element via an associative reduce.
    Index max_lexname = triple_prerank_sums.Keep().Max();

    // compute the size of the 2/3 subproblem.
    const Index size_subp = (input_size / 3) * 2 + (input_size % 3 != 0);

    // size of the mod1 part of the recursive subproblem
    const Index size_mod1 = input_size / 3 + (input_size % 3 != 0);

    if (debug_print) {
        sLOG1 << "max_lexname=" << max_lexname
              << " size_subp=" << size_subp
              << " size_mod1=" << size_mod1;
    }

    DIA<IndexRank> ranks_rec;

    if (max_lexname + Index(1) != size_subp) {

        // some lexical name is not unique -> perform recursion on two
        // substrings (mod 1 and mod 2)

        // zip triples and ranks.
        auto triple_ranks =
            triple_index_sorted
            .Zip(triple_prerank_sums,
                 [](const Index& triple_index, const Index& rank) {
                     return IndexRank { triple_index, rank };
                 });

        if (debug_print)
            triple_ranks.Keep().Print("triple_ranks");

        // construct recursion string with all ranks at mod 1 indices followed
        // by all ranks at mod 2 indices.
        DIA<Index> string_mod12 =
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
            .Cache();

        if (debug_print)
            string_mod12.Keep().Print("string_mod12");

        auto suffix_array_rec = DC3<Index>(string_mod12, size_subp);

        // reverse suffix array of recursion strings to find ranks for mod 1
        // and mod 2 positions.

        if (debug_print)
            suffix_array_rec.Keep().Print("suffix_array_rec");

        assert(suffix_array_rec.Keep().Size() == size_subp);

        ranks_rec =
            suffix_array_rec
            .Zip(Generate(ctx, size_subp),
                 [](const Index& sa, const Index& i) {
                     return IndexRank { sa, i };
                 })
            .Sort([size_mod1](const IndexRank& a, const IndexRank& b) {
                      // DONE(tb): changed sort order for better locality
                      // later. ... but slower?

                      // return a.index < b.index;
                      return a.index / size_mod1 < b.index / size_mod1 || (
                          a.index / size_mod1 == b.index / size_mod1 &&
                          a.index < b.index);
                  });

        if (debug_print)
            ranks_rec.Keep().Print("ranks_rec");
    }
    else {
        if (debug_print)
            triple_index_sorted.Keep().Print("triple_index_sorted");

        ranks_rec =
            triple_index_sorted
            .Zip(Generate(ctx, size_subp + Index(1)),
                 [](const Index& sa, const Index& i) {
                     return IndexRank { sa, i };
                 })
            .Sort([size_mod1](const IndexRank& a, const IndexRank& b) {
                      if (a.index % 3 == b.index % 3) {
                          // DONE(tb): changed sort order for better locality
                          // later. ... but slower?

                          // return a.index < b.index;
                          return a.index / size_mod1 < b.index / size_mod1 || (
                              a.index / size_mod1 == b.index / size_mod1 &&
                              a.index < b.index);
                      }
                      else
                          return a.index % 3 < b.index % 3;
                  })
            .Map([size_mod1](const IndexRank& a) {
                     return IndexRank {
                         a.index % 3 == 1 ? Index(0) : size_mod1, a.rank
                     };
                 })
            .Collapse();

        if (debug_print)
            ranks_rec.Keep().Print("ranks_rec");
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

                if (index + 3 == input_size) {
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
        .Filter([size_mod1](const IndexRank& a) {
                    return a.index < size_mod1;
                })
        .Map([](const IndexRank& a) {
                 // add one to ranks such that zero can be used as sentinel
                 // for suffixes beyond the end of the string.
                 return a.rank + Index(1);
             });

    auto ranks_mod2 =
        ranks_rec
        .Filter([size_mod1](const IndexRank& a) {
                    return a.index >= size_mod1;
                })
        .Map([](const IndexRank& a) {
                 return a.rank + Index(1);
             });

    if (debug_print) {
        triple_chars.Keep().Print("triple_chars");
        ranks_mod1.Keep().Print("ranks_mod1");
        ranks_mod2.Keep().Print("ranks_mod2");
    }

    assert_equal(triple_chars.Keep().Size(), size_mod1);
    assert_equal(ranks_mod1.Keep().Size(), size_mod1);
    assert_equal(ranks_mod2.Keep().Size(), size_mod1 - (input_size % 3 ? 1 : 0));

    // Zip together the three arrays, create pairs, and extract needed
    // tuples into string fragments.

    using StringFragmentMod0 = dc3_local::StringFragmentMod0<Index, Char>;
    using StringFragmentMod1 = dc3_local::StringFragmentMod1<Index, Char>;
    using StringFragmentMod2 = dc3_local::StringFragmentMod2<Index, Char>;

    using CharsRanks12 = dc3_local::CharsRanks12<Index, Char>;
    using IndexCR12Pair = dc3_local::IndexCR12Pair<Index, Char>;

    auto zip_triple_pairs1 =
        ZipPadding(
            [](const Chars& ch, const Index& mod1, const Index& mod2) {
                return CharsRanks12 { ch, mod1, mod2 };
            },
            std::make_tuple(Chars::EndSentinel(), 0, 0),
            triple_chars, ranks_mod1, ranks_mod2);

    if (debug_print)
        zip_triple_pairs1.Keep().Print("zip_triple_pairs1");

    auto zip_triple_pairs =
        zip_triple_pairs1
        .template FlatWindow<IndexCR12Pair>(
            2, [size_mod1](size_t index, const RingBuffer<CharsRanks12>& rb, auto emit) {
                emit(IndexCR12Pair { Index(3 * index), rb[0], rb[1] });
                if (index + 2 == size_mod1) {
                    // emit last sentinel
                    emit(IndexCR12Pair { Index(3 * (index + 1)), rb[1],
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
                     ip.index + Index(1),
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
                     ip.index + Index(2),
                     ip.cr0.chars.ch[2], ip.cr1.chars.ch[0],
                     ip.cr0.rank2, ip.cr1.rank1
                 };
             })
        .Filter([input_size](const StringFragmentMod2& mod2) {
                    return mod2.index < input_size;
                });

    if (debug_print) {
        fragments_mod0.Keep().Print("fragments_mod0");
        fragments_mod1.Keep().Print("fragments_mod1");
        fragments_mod2.Keep().Print("fragments_mod2");
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
        sorted_fragments_mod0.Keep().Print("sorted_fragments_mod0");
        sorted_fragments_mod1.Keep().Print("sorted_fragments_mod1");
        sorted_fragments_mod2.Keep().Print("sorted_fragments_mod2");
    }

    using StringFragment = dc3_local::StringFragment<Index, Char>;

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

    // merge and map to only suffix array

    auto suffix_array =
        Merge(dc3_local::FragmentComparator<StringFragment>(),
              string_fragments_mod0,
              string_fragments_mod1,
              string_fragments_mod2)
        .Map([](const StringFragment& a) { return a.index; })
        .Execute();

    // debug output

    if (debug_print) {
        std::vector<Char> input_vec = input_dia.Keep().AllGather();
        std::vector<Index> vec = suffix_array.Keep().AllGather();

        if (ctx.my_rank() == 0) {
            for (const Index& index : vec)
            {
                std::cout << std::setw(5) << index << " =";
                for (Index i = index; i < index + Index(64) && i < input_size; ++i) {
                    std::cout << ' ' << input_vec[i];
                }
                std::cout << '\n';
            }
        }
    }

    // check intermediate result, requires an input_dia.Keep() above!
    // die_unless(CheckSA(input_dia, suffix_array.Keep()));

    return suffix_array.Collapse();
}

template DIA<uint32_t> DC3<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size);

} // namespace suffix_sorting
} // namespace examples

/******************************************************************************/
