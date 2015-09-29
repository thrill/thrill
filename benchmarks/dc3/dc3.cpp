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
struct Triple {
    size_t       index;
    AlphabetType triple[3];

    bool operator < (const Triple& b) const noexcept {
        return std::lexicographical_compare(
            triple + 0, triple + 3, b.triple + 0, b.triple + 3);
    }

    friend std::ostream& operator << (std::ostream& os, const Triple& tc) {
        return os << "[" << std::to_string(tc.index) << "|"
                  << tc.triple[0] << tc.triple[1] << tc.triple[2] << "]";
    }
};

//! A pair (index, rank)
struct TripleRank {
    size_t index;
    size_t rank;

    friend std::ostream& operator << (std::ostream& os, const TripleRank& tr) {
        return os << "(" << std::to_string(tr.index) << "|"
                  << std::to_string(tr.rank) << ")";
    }
};

//! Fragments at String Positions i = 0 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod0
{
    AlphabetType t0, t1;
    size_t       r1, r2;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod0& sf) {
        return os << "t0=" << sf.t0 << ",t1=" << sf.t1
                  << ",r1=" << sf.r1 << ",r2=" << sf.r2;
    }
};

//! Fragments at String Positions i = 1 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod1
{
    AlphabetType t0;
    size_t       r0, r1;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod1& sf) {
        return os << "t0=" << sf.t0 << ",r0=" << sf.r0 << ",r1=" << sf.r1;
    }
};

//! Fragments at String Positions i = 2 Mod 3.
template <typename AlphabetType>
struct StringFragmentMod2
{
    AlphabetType t0, t1;
    size_t       r0, r2;

    friend std::ostream& operator << (std::ostream& os, const StringFragmentMod2& sf) {
        return os << "t0=" << sf.t0 << ",t1=" << sf.t1
                  << ",r0=" << sf.r0 << ",r2=" << sf.r2;
    }
};

//! Union of String Fragments with Index
template <typename AlphabetType>
struct StringFragment
{
    size_t index;
    union {
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
};

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

    auto input_dia = api::ReadBinary<uint8_t>(ctx, "Makefile");

    using TripleChar = Triple<uint8_t>;

    auto triple_sorted =
        input_dia
        // map (t_i) -> (i,t_i,t_{i+1},t_{i+2}) where i neq 0 mod 3
        .Window(
            3, [](size_t index, const common::RingBuffer<uint8_t>& rb) {
                assert(rb.size() == 3);
                // TODO(tb): filter out 0 mod 3 ? or change to FlatMap.
                // also missing last items.
                return TripleChar { index, rb[0], rb[1], rb[2] };
            })
        // sort triples by contained letters
        .Sort()
        .Cache();

    if (0)
        triple_sorted.Print("triple_sorted");

    auto triple_prerank_sums =
        triple_sorted
        .Window(
            2, [](size_t /* index */, const common::RingBuffer<TripleChar>& rb) {
                assert(rb.size() == 2);

                // return 0 or 1 depending on whether previous triple is equal
                size_t b = std::equal(rb[0].triple, rb[0].triple + 3,
                                      rb[1].triple) ? 0 : 1;
                return b;
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
                [](const TripleChar& tc, size_t rank) {
                    return TripleRank { tc.index, rank };
                })
            .Keep(); // is this needed?

        if (0)
            triple_ranks.Print("triple_ranks");

        // construct recursion string with all ranks at mod 1 indices
        DIA<size_t> string_mod1 =
            triple_ranks
            .Filter([](const TripleRank& tr) {
                        return tr.index % 3 == 1;
                    })
            .Sort([](const TripleRank& a, const TripleRank& b) {
                      return a.index < b.index;
                  })
            .Keep() // TODO(sl): this Keep should not be needed?
            .Map([](const TripleRank& tr) {
                     return tr.rank;
                 })
            .Collapse();

        // construct recursion string with all ranks at mod 2 indices
        DIA<size_t> string_mod2 =
            triple_ranks
            .Filter([](const TripleRank& tr) {
                        return tr.index % 3 == 2;
                    })
            .Sort([](const TripleRank& a, const TripleRank& b) {
                      return a.index < b.index;
                  })
            .Keep() // TODO(sl): this Keep should not be needed?
            .Map([](const TripleRank& tr) {
                     return tr.rank;
                 })
            .Collapse();

        // TODO(tb): this is actually an ActionFuture.
        // size_t size_mod1 = string_mod1.Size();

        string_mod1.Print("string_mod1");
        string_mod2.Print("string_mod2");

        // not available yet.
        // DIA<size_t> string_rec = string_mod1.Concat(string_mod2);

        // emulate Concat (badly)
        std::vector<size_t> rec;
        {
            std::vector<size_t> out1 = string_mod1.Gather();
            std::vector<size_t> out2 = string_mod2.Gather();

            rec.reserve(out1.size() + out2.size());
            rec.insert(rec.end(), out1.begin(), out1.end());
            rec.insert(rec.end(), out2.begin(), out2.end());
        }

        DIA<size_t> string_rec = DistributeFrom(ctx, rec);

        DIA<size_t> suffix_array_rec = Recursion(string_rec);

        suffix_array_rec.Keep();

        suffix_array_rec.Print("suffix_array_rec");

        // reverse suffix array of recursion strings to find ranks for mod 1
        // and mod 2 positions.

        size_t rec_size = suffix_array_rec.Size();

        return;

        auto enumerate = Generate(ctx,
                                  [](const size_t& index) { return index; },
                                  rec_size);

        DIA<size_t> ranks_rec =
            suffix_array_rec
            .Zip(enumerate,
                 [](size_t sa, size_t i) {
                     return TripleRank { sa, i };
                 })
            .Sort([](const TripleRank& a, const TripleRank& b) {
                      return a.index < b.index;
                  })
            .Map([](const TripleRank& a) {
                     return a.rank;
                 });

        ranks_rec.Print("ranks_rec");

#if OLD_PROTOTYPE
        // *** construct StringFragments ***

        // how to do a synchronized parallel scan over two or three arrays?
        // currently: fake it.

        using StringFragment = StringFragment<char>;

        std::vector<StringFragment>
        StringFragMod0, StringFragMod1, StringFragMod2;

        for (size_t i = 0; i < inputString.size(); ++i)
        {
            StringFragment sf {
                i
            };
            size_t idiv3mod1 = i / 3;
            size_t idiv3mod2 = idiv3mod1 + sizeMod1;

            if (i % 3 == 0)
            {
                sf.mod0.t0 = inputString.rank(i);
                sf.mod0.t1 =
                    i + 1 < inputString.size() ? inputString.rank(i + 1)
                    : std::numeric_limits<char>::max();
                sf.mod0.r1 =
                    idiv3mod1 < sizeMod1 ? ranksRec.rank(idiv3mod1)
                    : std::numeric_limits<size_t>::min();
                sf.mod0.r2 =
                    idiv3mod2 < ranksRec.size() ? ranksRec.rank(idiv3mod2)
                    : std::numeric_limits<size_t>::max();
                StringFragMod0.push_back(sf);
            }
            else if (i % 3 == 1)
            {
                sf.mod1.t0 = inputString.rank(i);
                sf.mod1.r0 = ranksRec.rank(idiv3mod1);
                sf.mod1.r1 =
                    idiv3mod2 < ranksRec.size() ? ranksRec.rank(idiv3mod2)
                    : std::numeric_limits<size_t>::max();
                StringFragMod1.push_back(sf);
            }
            else if (i % 3 == 2)
            {
                sf.mod2.t0 = inputString.rank(i);
                sf.mod2.t1 =
                    i + 1 < inputString.size() ? inputString.rank(i + 1)
                    : std::numeric_limits<char>::max();
                sf.mod2.r0 = ranksRec.rank(idiv3mod2);
                sf.mod2.r2 =
                    idiv3mod1 + 1 < sizeMod1 ? ranksRec.rank(idiv3mod1 + 1)
                    : std::numeric_limits<size_t>::max();
                StringFragMod2.push_back(sf);
            }
        }

        // Sort the three string fragment sets

        DIA<StringFragment> SortedStringFragMod0 =
            DIA<StringFragment>(StringFragMod0)
            .sort([](const StringFragment& a, const StringFragment& b)
                  {
                      return a.mod0.t0 == b.mod0.t0
                      ? a.mod0.r1 < b.mod0.r1
                      : a.mod0.t0 < b.mod0.t0;
                  });

        DIA<StringFragment> SortedStringFragMod1 =
            DIA<StringFragment>(StringFragMod1)
            .sort([](const StringFragment& a, const StringFragment& b)
                  { return a.mod1.r0 < b.mod1.r0; });

        DIA<StringFragment> SortedStringFragMod2 =
            DIA<StringFragment>(StringFragMod2)
            .sort([](const StringFragment& a, const StringFragment& b)
                  { return a.mod2.r0 < b.mod2.r0; });

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
