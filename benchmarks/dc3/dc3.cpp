/*******************************************************************************
 * benchmarks/dc3/dc3.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
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
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_pad.hpp>
#include <thrill/core/multiway_merge.hpp>

#include <algorithm>
#include <limits>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT
using thrill::common::RingBuffer;

//! A triple with index (i,t_i,t_{i+1},t_{i+2}).
template <typename AlphabetType>
struct IndexChars {
    size_t       index;
    AlphabetType triple[3];

    friend std::ostream& operator << (std::ostream& os, const IndexChars& tc) {
        return os << "[" << std::to_string(tc.index) << "|"
                  << tc.triple[0] << tc.triple[1] << tc.triple[2] << "]";
    }
} __attribute__ ((packed)); // NOLINT

//! A triple with index (i,t_i,t_{i+1},t_{i+2}).
template <typename AlphabetType>
struct Chars {
    AlphabetType triple[3];

    friend std::ostream& operator << (std::ostream& os, const Chars& ch) {
        return os << "["
                  << ch.triple[0] << ch.triple[1] << ch.triple[2] << "]";
    }

    static Chars EndSentinel() {
        return Chars {
                   std::numeric_limits<char>::lowest(),
                   std::numeric_limits<char>::lowest(),
                   std::numeric_limits<char>::lowest()
        };
    }
} __attribute__ ((packed)); // NOLINT

//! A pair (index, rank)
struct IndexRank {
    size_t index;
    size_t rank;

    friend std::ostream& operator << (std::ostream& os, const IndexRank& tr) {
        return os << "(" << std::to_string(tr.index) << "|"
                  << std::to_string(tr.rank) << ")";
    }
} __attribute__ ((packed)); // NOLINT

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
} __attribute__ ((packed)); // NOLINT

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
} __attribute__ ((packed)); // NOLINT

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
} __attribute__ ((packed)); // NOLINT

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
        os << "[" << std::to_string(tc.index) << "|";
        if (tc.index % 3 == 0)
            return os << "0|" << tc.mod0 << "]";
        if (tc.index % 3 == 1)
            return os << "1|" << tc.mod1 << "]";
        if (tc.index % 3 == 2)
            return os << "2|" << tc.mod2 << "]";
    }
} __attribute__ ((packed)); // NOLINT

DIA<size_t> Recursion(const DIA<size_t>& _input) {
    // this is cheating: perform naive suffix sorting. TODO(tb): templatize
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

template <typename Char>
struct Index3 {
    size_t index;
    size_t next;
    Char   ch;

    friend std::ostream& operator << (std::ostream& os, const Index3& i) {
        return os << "(index=" << i.index << " next=" << i.next << " ch=" << i.ch << ")";
    }
};

template <typename Char>
struct CharsRanks12 {
    Chars<Char> chars;
    size_t      rank1;
    size_t      rank2;

    friend std::ostream& operator << (std::ostream& os, const CharsRanks12& c) {
        return os << "(ch=" << c.chars << " r1=" << c.rank1 << " r2=" << c.rank2 << ")";
    }
} __attribute__ ((packed)); // NOLINT

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
    bool perm_check =
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

    bool order_check_sum =
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

std::string g_input = "dbacbacbd";

void StartDC3(api::Context& ctx) {

    using Char = char;
    using IndexChars = ::IndexChars<Char>;
    using Chars = ::Chars<Char>;

    std::vector<Char> input_vec(g_input.begin(), g_input.end());

    // auto input_dia = api::ReadBinary<Char>(ctx, "Makefile");
    auto input_dia = api::Distribute<Char>(ctx, input_vec);

    // TODO(tb): have this passed to the method, this costs an extra data round.
    size_t input_size = input_dia.Size();

    auto triple_sorted =
        input_dia
        // map (t_i) -> (i,t_i,t_{i+1},t_{i+2}) where i neq 0 mod 3
        .FlatWindow<IndexChars>(
            3, [input_size](size_t index, const RingBuffer<Char>& rb, auto emit) {
                if (index % 3 != 0)
                    emit(IndexChars { index, rb[0], rb[1], rb[2] });

                if (index == input_size - 3) {
                    // emit last sentinel items.
                    if ((index + 1) % 3 != 0)
                        emit(IndexChars { index + 1, rb[1], rb[2], Char() });
                    if ((index + 2) % 3 != 0)
                        emit(IndexChars { index + 2, rb[2], Char(), Char() });
                }
            })
        // sort triples by contained letters
        .Sort([](const IndexChars& a, const IndexChars& b) {
                  return std::lexicographical_compare(
                      a.triple + 0, a.triple + 3, b.triple + 0, b.triple + 3);
              });

    if (1)
        triple_sorted.Print("triple_sorted");

    // save triple's indexes (sorted by triple content) -> less storage
    auto triple_index_sorted =
        triple_sorted
        .Map([](const IndexChars& tc) { return tc.index; });

    auto triple_prerank_sums =
        triple_sorted
        .FlatWindow<size_t>(
            2, [](size_t index, const RingBuffer<IndexChars>& rb, auto emit) {
                assert(rb.size() == 2);

                // emit one sentinel for index 0.
                if (index == 0) emit(0);

                // emit 0 or 1 depending on whether previous triple is equal
                emit(std::equal(rb[0].triple, rb[0].triple + 3,
                                rb[1].triple) ? 0 : 1);
            })
        .PrefixSum();

    if (1)
        triple_prerank_sums.Print("triple_prerank_sums");

    if (true) {

        // perform recursion on two substrings (mod 1 and mod 2)

        // zip triples and ranks.
        auto triple_ranks =
            triple_index_sorted
            .Zip(triple_prerank_sums,
                 [](const size_t& triple_index, size_t rank) {
                     return IndexRank { triple_index, rank };
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

        // compute the size of the 2/3 subproblem.
        size_t size_subp = (input_size / 3) * 2 + (input_size % 3 == 2);

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
                3, [input_size](size_t index, const RingBuffer<Char>& rb, auto emit) {
                    if (index % 3 == 0)
                        emit(Chars { rb[0], rb[1], rb[2] });

                    if (index == input_size - 3) {
                        // emit sentinel
                        if ((index + 1) % 3 == 0)
                            emit(Chars { rb[1], rb[2], Char() });
                        if ((index + 2) % 3 == 0)
                            emit(Chars { rb[2], Char(), Char() });
                    }
                });

        auto ranks_mod1 =
            ranks_rec
            .Filter([&](const IndexRank& a) {
                        return a.index < size_mod1;
                    })
            .Map([](const IndexRank& a) {
                     return a.rank;
                 });

        auto ranks_mod2 =
            ranks_rec
            .Filter([&](const IndexRank& a) {
                        return a.index >= size_mod1;
                    })
            .Map([](const IndexRank& a) {
                     return a.rank;
                 });

        triple_chars.Print("triple_chars");
        ranks_mod1.Print("ranks_mod1");
        ranks_mod2.Print("ranks_mod2");

        assert(triple_chars.Size() == size_mod1 + (input_size % 3 ? 1 : 0));
        assert(ranks_mod1.Size() == size_mod1);
        assert(ranks_mod2.Size() == size_mod1);

        size_t zip_size = size_mod1 + (input_size % 3 ? 1 : 0);
        sLOG1 << "zip_size" << zip_size;

        // Zip together the three arrays, create pairs, and extract needed
        // tuples into string fragments.

        using StringFragmentMod0 = ::StringFragmentMod0<Char>;
        using StringFragmentMod1 = ::StringFragmentMod1<Char>;
        using StringFragmentMod2 = ::StringFragmentMod2<Char>;

        using CharsRanks12 = ::CharsRanks12<Char>;

        struct IndexCR12Pair {
            size_t       index;
            CharsRanks12 cr0;
            CharsRanks12 cr1;
        } __attribute__ ((packed)); // NOLINT

        auto zip_triple_pairs1 =
            ZipPadding(
                [](const Chars& ch, const size_t& mod1, const size_t& mod2) {
                    return CharsRanks12 { ch, mod1, mod2 };
                },
                std::make_tuple(Chars::EndSentinel(), 0, 0),
                triple_chars, ranks_mod1, ranks_mod2);

        zip_triple_pairs1.Print("zip_triple_pairs1");

        auto zip_triple_pairs =
            zip_triple_pairs1
            .FlatWindow<IndexCR12Pair>(
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
                         ip.cr0.chars.triple[0],
                         ip.cr0.chars.triple[1],
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
                         ip.cr0.chars.triple[1],
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
                         ip.cr0.chars.triple[2], ip.cr1.chars.triple[0],
                         ip.cr0.rank2, ip.cr1.rank1
                     };
                 })
            .Filter([input_size](const StringFragmentMod2& mod2) {
                        return mod2.index < input_size;
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

        using StringFragment = ::StringFragment<Char>;

        // Multi-way merge the three string fragment arrays: TODO(tb): currently
        // not distributed, FAKE FAKE FAKE!

        using StringFragmentIterator = std::vector<StringFragment>::iterator;

        std::vector<StringFragment> vec_fragments_mod0 =
            sorted_fragments_mod0
            .Map([](const StringFragmentMod0& mod0)
                 { return StringFragment(mod0); })
            .AllGather();

        std::vector<StringFragment> vec_fragments_mod1 =
            sorted_fragments_mod1
            .Map([](const StringFragmentMod1& mod1)
                 { return StringFragment(mod1); })
            .AllGather();

        std::vector<StringFragment> vec_fragments_mod2 =
            sorted_fragments_mod2
            .Map([](const StringFragmentMod2& mod2)
                 { return StringFragment(mod2); })
            .AllGather();

        std::pair<StringFragmentIterator, StringFragmentIterator> seqs[3];
        seqs[0] = std::make_pair(
            vec_fragments_mod0.begin(), vec_fragments_mod0.end());
        seqs[1] = std::make_pair(
            vec_fragments_mod1.begin(), vec_fragments_mod1.end());
        seqs[2] = std::make_pair(
            vec_fragments_mod2.begin(), vec_fragments_mod2.end());

        std::vector<StringFragment> output(input_size);

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
                    return a.mod1.r0 < b.mod2.r0;

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

        core::sequential_multiway_merge<false, false>(
            seqs, seqs + 3,
            output.begin(), input_size,
            fragmentComparator);

        // map to only suffix array

        auto suffix_array =
            api::Distribute<StringFragment>(ctx, output)
            .Map([](const StringFragment& a) { return a.index; });

        // debug output

        if (1) {
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

        assert(CheckSA(input_dia, suffix_array));
    }
}

int main(int argc, char* argv[]) {
    if (argc == 2) g_input = argv[1];
    return api::Run(StartDC3);
}

/******************************************************************************/
