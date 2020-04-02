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
#include <thrill/api/max.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/api/prefix_sum.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_window.hpp>
#include <thrill/api/zip_with_index.hpp>
#include <thrill/common/radix_sort.hpp>
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
        return std::tie(ch[0], ch[1], ch[2])
               == std::tie(b.ch[0], b.ch[1], b.ch[2]);
    }

    bool operator < (const Chars& b) const {
        return std::tie(ch[0], ch[1], ch[2])
               < std::tie(b.ch[0], b.ch[1], b.ch[2]);
    }

    friend std::ostream& operator << (std::ostream& os, const Chars& chars) {
        return os << '[' << chars.ch[0] << ',' << chars.ch[1]
                  << ',' << chars.ch[2] << ']';
    }

    static Chars Lowest() {
        return Chars {
            {
                std::numeric_limits<AlphabetType>::lowest(),
                std::numeric_limits<AlphabetType>::lowest(),
                std::numeric_limits<AlphabetType>::lowest()
            }
        };
    }
} TLX_ATTRIBUTE_PACKED;

//! A triple with index (i,t_i,t_{i+1},t_{i+2}).
template <typename Index, typename AlphabetType>
struct IndexChars {
    Index               index;
    Chars<AlphabetType> chars;

    AlphabetType at_radix(size_t depth) const { return chars.ch[depth]; }

    friend std::ostream& operator << (std::ostream& os, const IndexChars& tc) {
        return os << '[' << tc.index << '|' << tc.chars << ']';
    }
} TLX_ATTRIBUTE_PACKED;

//! A pair (index, rank)
template <typename Index>
struct IndexRank {
    Index index;
    Index rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& tr) {
        return os << '(' << tr.index << '|' << tr.rank << ')';
    }
} TLX_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 0 Mod 3.
template <typename Index, typename AlphabetType>
struct StringFragmentMod0 {
    Index        index;
    Index        r1, r2;
    AlphabetType t0, t1;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod0& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1
                  << " r1=" << sf.r1 << " r2=" << sf.r2;
    }
} TLX_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 1 Mod 3.
template <typename Index, typename AlphabetType>
struct StringFragmentMod1 {
    Index        index;
    Index        r0, r1;
    AlphabetType t0;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod1& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " r0=" << sf.r0 << " r1=" << sf.r1;
    }
} TLX_ATTRIBUTE_PACKED;

//! Fragments at String Positions i = 2 Mod 3.
template <typename Index, typename AlphabetType>
struct StringFragmentMod2 {
    Index        index;
    Index        r0, r2;
    AlphabetType t0, t1;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod2& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " r0=" << sf.r0
                  << " t1=" << sf.t1 << " r2=" << sf.r2;
    }
} TLX_ATTRIBUTE_PACKED;

//! Union of String Fragments with Index
template <typename Index, typename AlphabetType>
struct StringFragment {

    struct Common {
        Index        index;
        Index        ranks[2];
        AlphabetType text[2];
    } TLX_ATTRIBUTE_PACKED;

    union {
        Index                                   index;
        Common                                  common;
        StringFragmentMod0<Index, AlphabetType> mod0;
        StringFragmentMod1<Index, AlphabetType> mod1;
        StringFragmentMod2<Index, AlphabetType> mod2;
    } TLX_ATTRIBUTE_PACKED;

    StringFragment() = default;

    // conversion from StringFragmentMod0
    explicit StringFragment(
        const StringFragmentMod0<Index, AlphabetType>& _mod0) : mod0(_mod0) { }

    // conversion from StringFragmentMod1
    explicit StringFragment(
        const StringFragmentMod1<Index, AlphabetType>& _mod1) : mod1(_mod1) { }

    // conversion from StringFragmentMod2
    explicit StringFragment(
        const StringFragmentMod2<Index, AlphabetType>& _mod2) : mod2(_mod2) { }

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragment& tc) {
        os << '[' << std::to_string(tc.index) << '|';
        if (tc.index % 3 == 0)
            return os << "0|" << tc.mod0 << ']';
        else if (tc.index % 3 == 1)
            return os << "1|" << tc.mod1 << ']';
        else if (tc.index % 3 == 2)
            return os << "2|" << tc.mod2 << ']';
        abort();
    }

    AlphabetType at_radix(size_t depth) const { return common.text[depth]; }
    Index sort_rank() const { return common.ranks[0]; }
} TLX_ATTRIBUTE_PACKED;

static constexpr size_t fragment_comparator_params[3][3][3] =
{
    {
        { 1, 0, 0 }, { 1, 0, 1 }, { 2, 1, 1 }
    },
    {
        { 1, 1, 0 }, { 0, 0, 0 }, { 0, 0, 0 }
    },
    {
        { 2, 1, 1 }, { 0, 0, 0 }, { 0, 0, 0 }
    },
};

template <typename StringFragment>
struct FragmentComparator {

    bool operator () (const StringFragment& a, const StringFragment& b) const {

        unsigned ai = a.index % 3, bi = b.index % 3;

        const size_t* params = fragment_comparator_params[ai][bi];

        for (size_t d = 0; d < params[0]; ++d)
        {
            if (a.common.text[d] == b.common.text[d]) continue;
            return (a.common.text[d] < b.common.text[d]);
        }

        return a.common.ranks[params[1]] < b.common.ranks[params[2]];
    }
};

template <typename Index, typename Char>
struct CharsRanks12 {
    Chars<Char> chars;
    Index       rank1;
    Index       rank2;

    friend std::ostream& operator << (
        std::ostream& os, const CharsRanks12& c) {
        return os << "(ch=" << c.chars
                  << " r1=" << c.rank1 << " r2=" << c.rank2 << ")";
    }
} TLX_ATTRIBUTE_PACKED;

template <typename Index, typename Char>
struct IndexCR12Pair {
    Index                     index;
    CharsRanks12<Index, Char> cr0;
    CharsRanks12<Index, Char> cr1;
} TLX_ATTRIBUTE_PACKED;

template <typename Type, size_t MaxDepth>
class RadixSortFragment
{
public:
    explicit RadixSortFragment(size_t K) : K_(K) { }
    template <typename CompareFunction>
    void operator () (
        typename std::vector<Type>::iterator begin,
        typename std::vector<Type>::iterator end,
        const CompareFunction& cmp) const {
        if (K_ <= 4096) {
            thrill::common::radix_sort_CI<MaxDepth>(
                begin, end, K_, cmp, [](auto begin, auto end, auto) {
                    // sub sorter: sort StringFragments by rank
                    std::sort(begin, end, [](const Type& a, const Type& b) {
                                  return a.sort_rank() < b.sort_rank();
                              });
                });
        }
        else {
            std::sort(begin, end, cmp);
        }
    }

private:
    const size_t K_;
};

} // namespace dc3_local

using namespace thrill; // NOLINT
using thrill::common::RingBuffer;

template <typename Index, typename InputDIA>
DIA<dc3_local::StringFragment<Index, typename InputDIA::ValueType> >
DC3Recursive(const InputDIA& input_dia, size_t input_size, size_t K) {

    using Char = typename InputDIA::ValueType;
    using IndexChars = dc3_local::IndexChars<Index, Char>;
    using IndexRank = dc3_local::IndexRank<Index>;
    using Chars = dc3_local::Chars<Char>;

    Context& ctx = input_dia.context();

    auto triple_unsorted =
        input_dia.Keep()
        // map (t_i) -> (i,t_i,t_{i+1},t_{i+2}) where i neq 0 mod 3
        .template FlatWindow<IndexChars>(
            3,
            [](size_t index, const RingBuffer<Char>& rb, auto emit) {
                if (index % 3 != 0)
                    emit(IndexChars { Index(index), {
                                          { rb[0], rb[1], rb[2] }
                                      }
                         });
            },
            [input_size](size_t index, const RingBuffer<Char>& rb, auto emit) {
                // emit last sentinel items.
                if (index % 3 != 0) {
                    emit(IndexChars {
                             Index(index), {
                                 { rb.size() >= 1 ? rb[0] : Char(),
                                   rb.size() >= 2 ? rb[1] : Char(), Char() }
                             }
                         });
                }
                if (index + 1 == input_size && input_size % 3 == 1) {
                    // emit a sentinel tuple for inputs n % 3 == 1 to separate
                    // mod1 and mod2 strings in recursive subproblem. example
                    // which needs this: aaaaaaaaaa.
                    emit(IndexChars { Index(input_size), Chars::Lowest() });
                }
            });

    if (debug_print)
        triple_unsorted.Keep().Print("triple_unsorted");

    auto triple_sorted =
        triple_unsorted
        // sort triples by contained letters
        .Sort([](const IndexChars& a, const IndexChars& b) {
                  return a.chars < b.chars;
              }, common::RadixSort<IndexChars, 3>(K));

    if (debug_print)
        triple_sorted.Keep().Print("triple_sorted");

    // save triple's indexes (sorted by triple content) -> less storage
    auto triple_index_sorted =
        triple_sorted
        .Map([](const IndexChars& tc) { return tc.index; })
        .Cache();

    auto triple_prenames =
        triple_sorted
        .template FlatWindow<Index>(
            2, [](size_t index, const RingBuffer<IndexChars>& rb, auto emit) {
                assert(rb.size() == 2);

                // emit one sentinel for index 0.
                if (index == 0) emit(0);

                // emit 0 or 1 depending on whether previous triple is equal
                emit(rb[0].chars == rb[1].chars ? 0 : 1);
            });

    if (debug_print)
        triple_prenames.Keep().Print("triple_prenames");

    auto triple_lexname_sums = triple_prenames.PrefixSum();

    if (debug_print)
        triple_lexname_sums.Keep().Print("triple_lexname_sums");

    // get the last element via an associative reduce.
    const Index max_lexname = triple_lexname_sums.Keep().Max();

    // compute the size of the 2/3 subproblem.
    const Index size_subp = Index((input_size / 3) * 2 + (input_size % 3 != 0));

    // size of the mod1 part of the recursive subproblem
    const Index size_mod1 = Index(input_size / 3 + (input_size % 3 != 0));

    if (debug_print) {
        sLOG1 << "max_lexname=" << max_lexname
              << " size_subp=" << size_subp
              << " size_mod1=" << size_mod1;
    }

    DIA<IndexRank> ranks_mod12;

    if (max_lexname + Index(1) != size_subp) {

        // some lexical name is not unique -> perform recursion on two
        // substrings (mod 1 and mod 2)

        // zip triples and ranks.
        auto triple_ranks =
            triple_index_sorted
            .Zip(NoRebalanceTag,
                 triple_lexname_sums,
                 [](const Index& triple_index, const Index& rank) {
                     return IndexRank { triple_index, rank };
                 });

        if (debug_print)
            triple_ranks.Keep().Print("triple_ranks");

        // construct recursion string with all ranks at mod 1 indices followed
        // by all ranks at mod 2 indices.
        auto triple_ranks_sorted =
            triple_ranks
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      if (a.index % 3 == b.index % 3)
                          return a.index < b.index;
                      else
                          return a.index % 3 < b.index % 3;
                  });

        if (debug_print)
            triple_ranks_sorted.Keep().Print("triple_ranks_sorted");

        DIA<Index> string_mod12 =
            triple_ranks_sorted
            .Map([](const IndexRank& tr) {
                     return tr.rank;
                 })
            .Cache();

        if (debug_print)
            string_mod12.Keep().Print("string_mod12");

        using RecStringFragment = dc3_local::StringFragment<Index, Index>;

        DIA<RecStringFragment> suffix_array_rec = DC3Recursive<Index>(
            string_mod12, size_subp, max_lexname + Index(1));

        // reverse suffix array of recursion strings to find ranks for mod 1
        // and mod 2 positions.

        if (debug_print)
            suffix_array_rec.Keep().Print("suffix_array_rec");

        ranks_mod12 =
            suffix_array_rec
            .ZipWithIndex([](const RecStringFragment& sa, const size_t& i) {
                      // add one to ranks such that zero can be used as sentinel
                      // for suffixes beyond the end of the string.
                              return IndexRank { sa.index, Index(i + 1) };
                          })
            .Sort([size_mod1](const IndexRank& a, const IndexRank& b) {
                      // use sort order to interleave ranks mod 1/2
                      return a.index % size_mod1 < b.index % size_mod1 || (
                          a.index % size_mod1 == b.index % size_mod1 &&
                          a.index < b.index);
                  });

        if (debug_print) {
            // check that ranks are correctly interleaved
            ranks_mod12.Keep()
            .Window(
                DisjointTag, 2,
                [size_mod1](size_t, const std::vector<IndexRank>& ir) {
                    die_unless(ir[0].index < size_mod1);
                    die_unless(ir[1].index >= size_mod1 || ir[1].rank == Index(0));
                    return true;
                })
            .Execute();
            ranks_mod12.Keep().Print("ranks_mod12");
        }
    }
    else {
        if (ctx.my_rank() == 0)
            sLOG1 << "*** recursion finished ***";

        if (debug_print)
            triple_index_sorted.Keep().Print("triple_index_sorted");

        ranks_mod12 =
            triple_index_sorted
            .ZipWithIndex([](const Index& sa, const size_t& i) {
                      // add one to ranks such that zero can be used as sentinel
                      // for suffixes beyond the end of the string.
                              return IndexRank { sa, Index(i + 1) };
                          })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      // use sort order to interleave ranks mod 1/2
                      return a.index / 3 < b.index / 3 || (
                          a.index / 3 == b.index / 3 &&
                          a.index < b.index);
                  });

        if (debug_print) {
            // check that ranks are correctly interleaved
            ranks_mod12.Keep()
            .Window(
                DisjointTag, 2,
                [](size_t, const std::vector<IndexRank>& ir) {
                    die_unless(ir[0].index % 3 == 1);
                    die_unless(ir[1].index % 3 != 1 || ir[1].rank == Index(0));
                    return true;
                })
            .Execute();
            ranks_mod12.Keep().Print("ranks_mod12");
        }
    }

    // *** construct StringFragments ***

    // Zip together the two arrays, create pairs, and extract needed
    // tuples into string fragments.

    using StringFragmentMod0 = dc3_local::StringFragmentMod0<Index, Char>;
    using StringFragmentMod1 = dc3_local::StringFragmentMod1<Index, Char>;
    using StringFragmentMod2 = dc3_local::StringFragmentMod2<Index, Char>;

    using CharsRanks12 = dc3_local::CharsRanks12<Index, Char>;
    using IndexCR12Pair = dc3_local::IndexCR12Pair<Index, Char>;

    auto zip_triple_pairs1 =
        ZipWindow(
            ArrayTag, PadTag, /* window_size */ {
                { 3, 2 }
            },
            [](const std::array<Char, 3>& ch, const std::array<IndexRank, 2>& mod12) {
                return CharsRanks12 {
                    {
                        { ch[0], ch[1], ch[2] }
                    }, mod12[0].rank, mod12[1].rank
                };
            },
            std::make_tuple(std::numeric_limits<Char>::lowest(), IndexRank { 0, 0 }),
            input_dia, ranks_mod12);

    if (debug_print)
        zip_triple_pairs1.Keep().Print("zip_triple_pairs1");

    auto zip_triple_pairs =
        zip_triple_pairs1
        .template FlatWindow<IndexCR12Pair>(
            2, [size_mod1](size_t index,
                           const RingBuffer<CharsRanks12>& rb, auto emit) {
                emit(IndexCR12Pair { Index(3 * index), rb[0], rb[1] });
                if (index + 2 == size_mod1) {
                    // emit last sentinel
                    emit(IndexCR12Pair { Index(3 * (index + 1)), rb[1],
                                         CharsRanks12 { Chars::Lowest(), 0, 0 }
                         });
                }
            });

    auto fragments_mod0 =
        zip_triple_pairs
        .Map([](const IndexCR12Pair& ip) {
                 return StringFragmentMod0 {
                     ip.index,
                     ip.cr0.rank1, ip.cr0.rank2,
                     ip.cr0.chars.ch[0], ip.cr0.chars.ch[1]
                 };
             })
        .Filter([input_size](const StringFragmentMod0& mod0) {
                    return mod0.index < Index(input_size);
                });

    auto fragments_mod1 =
        zip_triple_pairs
        .Map([](const IndexCR12Pair& ip) {
                 return StringFragmentMod1 {
                     ip.index + Index(1),
                     ip.cr0.rank1, ip.cr0.rank2,
                     ip.cr0.chars.ch[1]
                 };
             })
        .Filter([input_size](const StringFragmentMod1& mod1) {
                    return mod1.index < Index(input_size);
                });

    auto fragments_mod2 =
        zip_triple_pairs
        .Map([](const IndexCR12Pair& ip) {
                 return StringFragmentMod2 {
                     ip.index + Index(2),
                     ip.cr0.rank2, ip.cr1.rank1,
                     ip.cr0.chars.ch[2], ip.cr1.chars.ch[0]
                 };
             })
        .Filter([input_size](const StringFragmentMod2& mod2) {
                    return mod2.index < Index(input_size);
                });

    if (debug_print) {
        fragments_mod0.Keep().Print("fragments_mod0");
        fragments_mod1.Keep().Print("fragments_mod1");
        fragments_mod2.Keep().Print("fragments_mod2");
    }

    // Sort/Merge and map to only suffix array

    using StringFragment = dc3_local::StringFragment<Index, Char>;

    auto string_fragments_mod0 =
        fragments_mod0
        .Map([](const StringFragmentMod0& mod0)
             { return StringFragment(mod0); });

    auto string_fragments_mod1 =
        fragments_mod1
        .Map([](const StringFragmentMod1& mod1)
             { return StringFragment(mod1); });

    auto string_fragments_mod2 =
        fragments_mod2
        .Map([](const StringFragmentMod2& mod2)
             { return StringFragment(mod2); });

    auto suffix_array =
        Union(string_fragments_mod0,
              string_fragments_mod1,
              string_fragments_mod2)
        .Sort(dc3_local::FragmentComparator<StringFragment>())
        .Execute();

    // debug output

    if (debug_print) {
        std::vector<Char> input_vec = input_dia.Keep().AllGather();
        std::vector<Index> vec =
            suffix_array.Keep()
            .Map([](const StringFragment& a) { return a.index; })
            .Gather();

        if (ctx.my_rank() == 0) {
            for (const Index& index : vec)
            {
                std::cout << std::setw(5) << index << " =";
                for (Index i = index;
                     i < index + Index(64) && i < Index(input_size); ++i) {
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

template <typename Index, typename InputDIA>
DIA<Index> DC3(const InputDIA& input_dia, size_t input_size, size_t K) {

    using Char = typename InputDIA::ValueType;
    using StringFragment = dc3_local::StringFragment<Index, Char>;

    return DC3Recursive<Index>(input_dia, input_size, K)
           .Map([](const StringFragment& a) { return a.index; })
           .Collapse();
}

template DIA<uint32_t> DC3<uint32_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, size_t K);

#if !THRILL_ON_TRAVIS

template DIA<common::uint40> DC3<common::uint40>(
    const DIA<uint8_t>& input_dia, size_t input_size, size_t K);

template DIA<uint64_t> DC3<uint64_t>(
    const DIA<uint8_t>& input_dia, size_t input_size, size_t K);

#endif

} // namespace suffix_sorting
} // namespace examples

/******************************************************************************/
