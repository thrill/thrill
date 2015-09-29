/*******************************************************************************
 * benchmarks/dc3/dc3.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
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
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>

#include <algorithm>
#include <limits>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

//! A triple with index (i,t_i,t_{i+1},t_{i+2}).
template <typename AlphabetType>
struct IndexChars {
    size_t       index;
    AlphabetType triple[3];

    friend std::ostream& operator << (std::ostream& os, const IndexChars& tc) {
        return os << "[" << std::to_string(tc.index) << "|"
                  << tc.triple[0] << tc.triple[1] << tc.triple[2] << "]";
    }
} __attribute__ ((packed));

//! A triple with index (i,t_i,t_{i+1},t_{i+2}).
template <typename AlphabetType>
struct Chars {
    AlphabetType triple[3];

    friend std::ostream& operator << (std::ostream& os, const Chars& ch) {
        return os << "["
                  << ch.triple[0] << ch.triple[1] << ch.triple[2] << "]";
    }
} __attribute__ ((packed));

//! A pair (index, rank)
struct IndexRank {
    size_t index;
    size_t rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& tr) {
        return os << "(" << std::to_string(tr.index) << "|"
                  << std::to_string(tr.rank) << ")";
    }
} __attribute__ ((packed));

//! Fragments at String Positions i = 0 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod0
{
    size_t       index;
    AlphabetType t0, t1;
    size_t       r1, r2;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod0& sf) {
        return os << "t0=" << sf.t0 << ",t1=" << sf.t1
                  << ",r1=" << sf.r1 << ",r2=" << sf.r2;
    }
} __attribute__ ((packed));

//! Fragments at String Positions i = 1 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod1
{
    size_t       index;
    AlphabetType t0;
    size_t       r0, r1;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod1& sf) {
        return os << "r0=" << sf.r0 << ",t0=" << sf.t0 << ",r1=" << sf.r1;
    }
} __attribute__ ((packed));

//! Fragments at String Positions i = 2 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod2
{
    size_t       index;
    AlphabetType t0, t1;
    size_t       r0, r2;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod2& sf) {
        return os << "r0=" << sf.r0 << ",t0=" << sf.t0 << ","
                  << "t1=" << sf.t1 << ",r2=" << sf.r2;
    }
} __attribute__ ((packed));

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

    friend std::ostream& operator << (std::ostream& os, const StringFragment& tc) {
        os << "[" << std::to_string(tc.index) << "|";
        if (tc.index % 3 == 0)
            return os << "0|" << tc.mod0 << "]";
        if (tc.index % 3 == 1)
            return os << "1|" << tc.mod1 << "]";
        if (tc.index % 3 == 2)
            return os << "2|" << tc.mod2 << "]";
    }
} __attribute__ ((packed));

DIA<size_t> Recursion(const DIA<size_t>& _input) {
    // this is cheating: perform naive suffix sorting. TODO: templatize
    // algorithm and call recursively.

    std::vector<size_t> input = _input.Gather();
    std::vector<size_t> output;

    if (_input.ctx().my_rank() == 0)
    {
        output.resize(input.size());

        for (size_t i = 0; i < output.size(); ++i)
            output[i] = i;

        std::sort(output.begin(), output.end(),
                  [&input](const size_t& a, const size_t& b) {
                      return std::lexicographical_compare(
                          input.begin() + a, input.end(),
                          input.begin() + b, input.end());
                  });
    }

    return api::DistributeFrom(_input.ctx(), std::move(output));
}

void StartDC3(api::Context& ctx) {

    using Char = char;
    using IndexChars = ::IndexChars<Char>;
    using Chars = ::Chars<Char>;

    std::string input = "bananabananabananabanana";
    std::vector<Char> input_vec(input.begin(), input.end());

    // auto input_dia = api::ReadBinary<Char>(ctx, "Makefile");
    auto input_dia = api::Distribute<Char>(ctx, input_vec);

    // TODO(tb): have this passed to the method, this costs extra data round.
    size_t input_size = input_dia.Size();

    auto triple_sorted =
        input_dia
        // map (t_i) -> (i,t_i,t_{i+1},t_{i+2}) where i neq 0 mod 3
        .FlatWindow<IndexChars>(
            3, [](size_t index, const common::RingBuffer<Char>& rb, auto emit) {
                assert(rb.size() == 3);
                // TODO(tb): missing last sentinel items.
                if (index % 3 != 0)
                    emit(IndexChars { index, rb[0], rb[1], rb[2] });
            })
        // sort triples by contained letters
        .Sort([](const IndexChars& a, const IndexChars& b) {
                  return std::lexicographical_compare(
                      a.triple + 0, a.triple + 3, b.triple + 0, b.triple + 3);
              })
        .Cache();

    if (1)
        triple_sorted.Print("triple_sorted");

    auto triple_prerank_sums =
        triple_sorted
        .FlatWindow<size_t>(
            2, [](size_t index, const common::RingBuffer<IndexChars>& rb, auto emit) {
                assert(rb.size() == 2);

                // emit one sentinel for index 0.
                if (index == 0) emit(1);

                // return 0 or 1 depending on whether previous triple is equal
                size_t b = std::equal(rb[0].triple, rb[0].triple + 3,
                                      rb[1].triple) ? 0 : 1;
                emit(b);
            })
        .PrefixSum();

    if (0)
        triple_prerank_sums.Print("triple_prerank_sums");

    if (true) {

        // perform recursion on two substrings (mod 1 and mod 2)

        // zip triples and ranks.
        auto triple_ranks =
            triple_sorted
            .Zip(
                triple_prerank_sums,
                [](const IndexChars& tc, size_t rank) {
                    return IndexRank { tc.index, rank };
                });
        if (1)
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

        string_mod12.Print("string_mod12");

        // TODO(tb): this can be calculated from input_size.
        // size_t size_mod1 = string_mod1.Size();
        size_t size_mod1 = input_size / 3;

        DIA<size_t> suffix_array_rec = Recursion(string_mod12);

        suffix_array_rec.Print("suffix_array_rec");

        // reverse suffix array of recursion strings to find ranks for mod 1
        // and mod 2 positions.

        size_t rec_size = suffix_array_rec.Size();

        auto ranks_rec =
            suffix_array_rec
            .Zip(Generate(ctx, rec_size),
                 [](size_t sa, size_t i) {
                     return IndexRank { sa, i };
                 })
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      // TODO(tb): change sort order for better locality later.
                      return a.index < b.index;
                  });

        ranks_rec.Print("ranks_rec");

        // *** construct StringFragments ***

        auto triple_chars =
            input_dia
            // map (t_i) -> (i,t_i,t_{i+1},t_{i+2}) where i neq 0 mod 3
            .FlatWindow<Chars>(
                3, [](size_t index, const common::RingBuffer<Char>& rb, auto emit) {
                    assert(rb.size() == 3);
                    if (index % 3 == 0)
                        emit(Chars { rb[0], rb[1], rb[2] });
                });

        auto ranks_mod1 =
            ranks_rec
            .Filter([&](const IndexRank& a) {
                        return a.index < size_mod1;
                    })
            .Map([](const IndexRank& a) {
                     return a.rank;
                 })
            .Collapse();

        auto ranks_mod2 =
            ranks_rec
            .Filter([&](const IndexRank& a) {
                        return a.index >= size_mod1;
                    })
            .Map([](const IndexRank& a) {
                     return a.rank;
                 })
            .Collapse();

        triple_chars.Print("triple_chars");
        ranks_mod1.Print("ranks_mod1");
        ranks_mod2.Print("ranks_mod2");

        // TODO(tb): Yay. Need MultiZip!

        using StringFragmentMod0 = ::StringFragmentMod0<Char>;
        using StringFragmentMod1 = ::StringFragmentMod1<Char>;
        using StringFragmentMod2 = ::StringFragmentMod2<Char>;

        struct CharsRanks12 {
            Chars  chars;
            size_t rank1;
            size_t rank2;
        };

        auto zip_triple_pairs =
            triple_chars
            .Zip(ranks_mod1
                 .Zip(ranks_mod2, [](const size_t& mod1, const size_t& mod2) {
                          return std::make_pair(mod1, mod2);
                      }),
                 [](const Chars& ch, const std::pair<size_t, size_t>& mod12) {
                     return CharsRanks12 { ch, mod12.first, mod12.second };
                 })
            .Window(
                2, [](size_t index,
                      const common::RingBuffer<CharsRanks12>& rb) {
                    return std::make_tuple(3 * index, rb[0], rb[1]);
                });

        // size_t idiv3mod1 = i / 3;
        // size_t idiv3mod2 = idiv3mod1 + size_mod1;

        auto fragments_mod0 =
            zip_triple_pairs
            .Map([](const std::tuple<size_t, CharsRanks12, CharsRanks12>& chp) {
                     size_t index = std::get<0>(chp);
                     const CharsRanks12& cr0 = std::get<1>(chp);

                     return StringFragmentMod0 {
                         index,
                         cr0.chars.triple[0], cr0.chars.triple[1],
                         cr0.rank1, cr0.rank2
                     };
                     //     sf.mod0.t0 = inputString.rank(i);
                     // sf.mod0.t1 =
                     //     i + 1 < inputString.size() ? inputString.rank(i + 1)
                     //     : std::numeric_limits<char>::max();
                     // sf.mod0.r1 =
                     //     idiv3mod1 < size_mod1 ? ranksRec.rank(idiv3mod1)
                     //     : std::numeric_limits<size_t>::min();
                     // sf.mod0.r2 =
                     //     idiv3mod2 < ranksRec.size() ? ranksRec.rank(idiv3mod2)
                     //     : std::numeric_limits<size_t>::max();
                 });

        auto fragments_mod1 =
            zip_triple_pairs
            .Map([](const std::tuple<size_t, CharsRanks12, CharsRanks12>& chp) {
                     size_t index = std::get<0>(chp);
                     const CharsRanks12& cr0 = std::get<1>(chp);

                     return StringFragmentMod1 {
                         index + 1,
                         cr0.chars.triple[1],
                         cr0.rank1, cr0.rank2
                     };

                     // sf.mod1.t0 = inputString.rank(i);
                     // sf.mod1.r0 = ranksRec.rank(idiv3mod1);
                     // sf.mod1.r1 =
                     // idiv3mod2 < ranksRec.size() ? ranksRec.rank(idiv3mod2)
                     // : std::numeric_limits<size_t>::max();
                 });

        auto fragments_mod2 =
            zip_triple_pairs
            .Map([](const std::tuple<size_t, CharsRanks12, CharsRanks12>& chp) {
                     size_t index = std::get<0>(chp);
                     const CharsRanks12& cr0 = std::get<1>(chp);
                     const CharsRanks12& cr1 = std::get<2>(chp);

                     return StringFragmentMod2 {
                         index + 2,
                         cr0.chars.triple[2], cr1.chars.triple[0],
                         cr0.rank2, cr1.rank1
                     };

                     // sf.mod2.t0 = inputString.rank(i);
                     // sf.mod2.t1 =
                     //     i + 1 < inputString.size() ? inputString.rank(i + 1)
                     //     : std::numeric_limits<char>::max();
                     // sf.mod2.r0 = ranksRec.rank(idiv3mod2);
                     // sf.mod2.r2 =
                     //     idiv3mod1 + 1 < size_mod1 ? ranksRec.rank(idiv3mod1 + 1)
                     //     : std::numeric_limits<size_t>::max();
                 });

        fragments_mod0.Print("fragments_mod0");
        fragments_mod1.Print("fragments_mod1");
        fragments_mod2.Print("fragments_mod2");

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

        sorted_fragments_mod0.Print("sorted_fragments_mod0");
        sorted_fragments_mod1.Print("sorted_fragments_mod1");
        sorted_fragments_mod2.Print("sorted_fragments_mod2");

#ifdef OLD_PROTOTYPE
        using StringFragment = ::StringFragment<Char>;

        // Multi-way merge the three string fragment arrays: TODO also fake
        // currently.

        using StringFragmentIterator = std::vector<StringFragment>::iterator;

        std::vector<StringFragment>
        vSortedStringFragMod0 = SortedStringFragMod0.evilGetData();
        std::vector<StringFragment>
        vSortedStringFragMod1 = SortedStringFragMod1.evilGetData();
        std::vector<StringFragment>
        vSortedStringFragMod2 = SortedStringFragMod2.evilGetData();

        std::pair<StringFragmentIterator, StringFragmentIterator> seqs[3];
        seqs[0] = std::make_pair(
            vSortedStringFragMod0.begin(), vSortedStringFragMod0.end());
        seqs[1] = std::make_pair(
            vSortedStringFragMod1.begin(), vSortedStringFragMod1.end());
        seqs[2] = std::make_pair(
            vSortedStringFragMod2.begin(), vSortedStringFragMod2.end());

        std::vector<StringFragment> output(inputString.size());

        auto fragmentComparator =
            [](const StringFragment& a, const StringFragment& b)
            {
                unsigned ai = a.index % 3, bi = b.index % 3;
                assert(ai != bi);

                if (ai == 0 && bi == 1)
                    return a.mod0.t0 == b.mod1.t0 ?
                           a.mod0.r1 < b.mod1.r1 :
                           a.mod0.t0 < b.mod1.t0;

                if (ai == 0 && bi == 2)
                    return a.mod0.t0 == b.mod2.t0 ? (
                        a.mod0.t1 == b.mod2.t1 ?
                        a.mod0.r2 < b.mod2.r2 :
                        a.mod0.t1 < b.mod2.t1)
                           : a.mod0.t0 < b.mod2.t0;

                if (ai == 1 && bi == 0)
                    return a.mod1.t0 == b.mod0.t0 ?
                           a.mod1.r1 < b.mod0.r1 :
                           a.mod1.t0 < b.mod0.t0;

                if (ai == 1 && bi == 2)
                    return a.mod1.r1 < b.mod2.r0;

                if (ai == 2 && bi == 0)
                    return a.mod2.t0 == b.mod0.t0 ? (
                        a.mod2.t1 == b.mod0.t1 ?
                        a.mod2.r2 < b.mod0.r2 :
                        a.mod2.t1 < b.mod0.t1)
                           : a.mod2.t0 < b.mod0.t0;

                if (ai == 2 && bi == 1)
                    return a.mod2.r0 < b.mod1.r0;

                abort();
            };

        __gnu_parallel::multiway_merge(seqs, seqs + 3,
                                       output.begin(), inputString.size(),
                                       fragmentComparator,
                                       __gnu_parallel::sequential_tag());

        // map to only suffix array

        DIA<size_t> SuffixArray = DIA<StringFragment>(output)
                                  .Map([](const StringFragment& a) { return a.index; });

        // debug output

        for (const size_t& index : SuffixArray.evilGetData())
        {
            std::string sub(theRealInputString.substr(index, 128));
            boost::replace_all(sub, "\n", " ");
            boost::replace_all(sub, "\t", " ");

            std::cout << std::setw(5) << index << " = " << sub << "\n";
        }
#endif
    }
}

int main() {
    return api::Run(StartDC3);
}

/******************************************************************************/
