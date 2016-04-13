/*******************************************************************************
 * examples/suffix_sorting/dc7.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/suffix_sorting/bwt_generator.hpp>
#include <examples/suffix_sorting/sa_checker.hpp>

#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/equal_to_dia.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/merge.hpp>
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

bool debug_print = false;
bool generate_bwt = false;

using namespace thrill; // NOLINT
using thrill::common::RingBuffer;

//! A tuple with index (i,t_i,t_{i+1},t_{i+2},...,t_{i+6}).
template <typename AlphabetType>
struct Chars {
    AlphabetType ch[7];

    bool operator == (const Chars& b) const {
        return std::equal(ch + 0, ch + 7, b.ch + 0);
    }

    bool operator < (const Chars& b) const {
        return std::lexicographical_compare(
            ch + 0, ch + 7, b.ch + 0, b.ch + 7);
    }

    friend std::ostream& operator << (std::ostream& os, const Chars& chars) {
        return os << '[' << chars.ch[0] << ',' << chars.ch[1]
                  << ',' << chars.ch[2] << ',' << chars.ch[3]
                  << ',' << chars.ch[4] << ',' << chars.ch[5]
                  << ',' << chars.ch[6] << ']';
    }

    static Chars EndSentinel() {
        return Chars {
                   {
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest(),
                       std::numeric_limits<AlphabetType>::lowest()
                   }
        };
    }
} THRILL_ATTRIBUTE_PACKED;

//! A tuple with index (i,t_i,t_{i+1},t_{i+2}).
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

//! fragments at string positions i = 0 mod 7.
template <typename Index, typename AlphabetType>
struct StringFragmentMod0 {
    Index        index;
    AlphabetType t0, t1, t2;
    Index        r0, r1, r3;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod0& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1 << " t2=" << sf.t2
                  << " r0=" << sf.r0 << " r1=" << sf.r1 << " r3=" << sf.r3;
    }
} THRILL_ATTRIBUTE_PACKED;

//! fragments at string positions i = 1 mod 7.
template <typename Index, typename AlphabetType>
struct StringFragmentMod1 {
    Index        index;
    AlphabetType t0, t1, t2, t3, t4, t5;
    Index        r0, r2, r6;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod1& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1 << " t2=" << sf.t2
                  << " t3=" << sf.t3 << " t4=" << sf.t4 << " t5=" << sf.t5
                  << " r0=" << sf.r0 << " r2=" << sf.r2 << " r6=" << sf.r6;
    }
} THRILL_ATTRIBUTE_PACKED;

//! fragments at string positions i = 2 mod 7.
template <typename Index, typename AlphabetType>
struct StringFragmentMod2 {
    Index        index;
    AlphabetType t0, t1, t2, t3, t4, t5;
    Index        r1, r5, r6;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod2& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1 << " t2=" << sf.t2
                  << " t3=" << sf.t3 << " t4=" << sf.t4 << " t5=" << sf.t5
                  << " r1=" << sf.r1 << " r5=" << sf.r5 << " r6=" << sf.r6;
    }
} THRILL_ATTRIBUTE_PACKED;

//! fragments at string positions i = 3 mod 7.
template <typename Index, typename AlphabetType>
struct StringFragmentMod3 {

    Index        index;
    AlphabetType t0, t1, t2, t3, t4;
    Index        r0, r4, r5;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod3& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1 << " t2=" << sf.t2
                  << " t3=" << sf.t3 << " t4=" << sf.t4
                  << " r0=" << sf.r0 << " r4=" << sf.r4 << " r5=" << sf.r5;
    }
} THRILL_ATTRIBUTE_PACKED;

//! fragments at string positions i = 4 mod 7.
template <typename Index, typename AlphabetType>
struct StringFragmentMod4 {
    Index        index;
    AlphabetType t0, t1, t2, t3, t4, t5;
    Index        r3, r4, r6;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod4& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1 << " t2=" << sf.t2
                  << " t3=" << sf.t3 << " t4=" << sf.t4 << " t5=" << sf.t5
                  << " r3=" << sf.r3 << " r4=" << sf.r4 << " r6=" << sf.r6;
    }
} THRILL_ATTRIBUTE_PACKED;

//! fragments at string positions i = 5 mod 7.
template <typename Index, typename AlphabetType>
struct StringFragmentMod5 {
    Index        index;
    AlphabetType t0, t1, t2, t3, t4;
    Index        r2, r3, r5;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod5& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1 << " t2=" << sf.t2
                  << " t3=" << sf.t3 << " t4=" << sf.t4
                  << " r2=" << sf.r2 << " r3=" << sf.r3 << " r5=" << sf.r5;
    }
} THRILL_ATTRIBUTE_PACKED;

//! fragments at string positions i = 6 mod 7.
template <typename Index, typename AlphabetType>
struct StringFragmentMod6 {
    Index        index;
    AlphabetType t0, t1, t2, t3;
    Index        r1, r2, r4;

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragmentMod6& sf) {
        return os << "i=" << sf.index
                  << " t0=" << sf.t0 << " t1=" << sf.t1 << " t2=" << sf.t2
                  << " t3=" << sf.t3
                  << " r1=" << sf.r1 << " r2=" << sf.r2 << " r4=" << sf.r4;
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
        StringFragmentMod3<Index, AlphabetType> mod3;
        StringFragmentMod4<Index, AlphabetType> mod4;
        StringFragmentMod5<Index, AlphabetType> mod5;
        StringFragmentMod6<Index, AlphabetType> mod6;
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

    // conversion from StringFragmentMod3
    explicit StringFragment(const StringFragmentMod3<Index, AlphabetType>& _mod3)
        : mod3(_mod3) { }

    // conversion from StringFragmentMod4
    explicit StringFragment(const StringFragmentMod4<Index, AlphabetType>& _mod4)
        : mod4(_mod4) { }

    // conversion from StringFragmentMod5
    explicit StringFragment(const StringFragmentMod5<Index, AlphabetType>& _mod5)
        : mod5(_mod5) { }

    // conversion from StringFragmentMod6
    explicit StringFragment(const StringFragmentMod6<Index, AlphabetType>& _mod6)
        : mod6(_mod6) { }

    friend std::ostream& operator << (std::ostream& os,
                                      const StringFragment& tc) {
        os << '[' << std::to_string(tc.index) << '|';
        if (tc.index % 7 == 0)
            return os << "0|" << tc.mod0 << ']';
        else if (tc.index % 7 == 1)
            return os << "1|" << tc.mod1 << ']';
        else if (tc.index % 7 == 2)
            return os << "2|" << tc.mod2 << ']';
        else if (tc.index % 7 == 3)
            return os << "3|" << tc.mod3 << ']';
        else if (tc.index % 7 == 4)
            return os << "4|" << tc.mod4 << ']';
        else if (tc.index % 7 == 5)
            return os << "5|" << tc.mod5 << ']';
        else if (tc.index % 7 == 6)
            return os << "6|" << tc.mod6 << ']';
        abort();
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename StringFragment>
struct FragmentComparator {

    template <typename FragmentA, typename FragmentB>
    bool cmp0(const FragmentA& a, const FragmentB& b) const {
        return a.r0 < b.r0;
    }

    template <typename FragmentA, typename FragmentB>
    bool cmp1(const FragmentA& a, const FragmentB& b) const {
        return std::tie(a.t0, a.r1) < std::tie(b.t0, b.r1);
    }

    template <typename FragmentA, typename FragmentB>
    bool cmp2(const FragmentA& a, const FragmentB& b) const {
        return std::tie(a.t0, a.t1, a.r2) < std::tie(b.t0, b.t1, b.r2);
    }

    template <typename FragmentA, typename FragmentB>
    bool cmp3(const FragmentA& a, const FragmentB& b) const {
        return std::tie(a.t0, a.t1, a.t2, a.r3)
               < std::tie(b.t0, b.t1, b.t2, b.r3);
    }

    template <typename FragmentA, typename FragmentB>
    bool cmp4(const FragmentA& a, const FragmentB& b) const {
        return std::tie(a.t0, a.t1, a.t2, a.t3, a.r4)
               < std::tie(b.t0, b.t1, b.t2, b.t3, b.r4);
    }

    template <typename FragmentA, typename FragmentB>
    bool cmp5(const FragmentA& a, const FragmentB& b) const {
        return std::tie(a.t0, a.t1, a.t2, a.t3, a.t4, a.r5)
               < std::tie(b.t0, b.t1, b.t2, b.t3, b.t4, b.r5);
    }

    template <typename FragmentA, typename FragmentB>
    bool cmp6(const FragmentA& a, const FragmentB& b) const {
        return std::tie(a.t0, a.t1, a.t2, a.t3, a.t4, a.t5, a.r6)
               < std::tie(b.t0, b.t1, b.t2, b.t3, b.t4, b.t5, b.r6);
    }

    bool operator () (const StringFragment& a, const StringFragment& b) const {
        unsigned ai = a.index % 7, bi = b.index % 7;

        if (ai == 0 && bi == 0)
            return cmp0(a.mod0, b.mod0);

        if (ai == 0 && bi == 1)
            return cmp0(a.mod0, b.mod1);

        if (ai == 0 && bi == 2)
            return cmp1(a.mod0, b.mod2);

        if (ai == 0 && bi == 3)
            return cmp0(a.mod0, b.mod3);

        if (ai == 0 && bi == 4)
            return cmp3(a.mod0, b.mod4);

        if (ai == 0 && bi == 5)
            return cmp3(a.mod0, b.mod5);

        if (ai == 0 && bi == 6)
            return cmp1(a.mod0, b.mod6);

        /**********************************************************************/

        if (ai == 1 && bi == 0)
            return cmp0(a.mod1, b.mod0);

        if (ai == 1 && bi == 1)
            return cmp0(a.mod1, b.mod1);

        if (ai == 1 && bi == 2)
            return cmp6(a.mod1, b.mod2);

        if (ai == 1 && bi == 3)
            return cmp0(a.mod1, b.mod3);

        if (ai == 1 && bi == 4)
            return cmp6(a.mod1, b.mod4);

        if (ai == 1 && bi == 5)
            return cmp2(a.mod1, b.mod5);

        if (ai == 1 && bi == 6)
            return cmp2(a.mod1, b.mod6);

        /**********************************************************************/

        if (ai == 2 && bi == 0)
            return cmp1(a.mod2, b.mod0);

        if (ai == 2 && bi == 1)
            return cmp6(a.mod2, b.mod1);

        if (ai == 2 && bi == 2)
            return cmp1(a.mod2, b.mod2);

        if (ai == 2 && bi == 3)
            return cmp5(a.mod2, b.mod3);

        if (ai == 2 && bi == 4)
            return cmp6(a.mod2, b.mod4);

        if (ai == 2 && bi == 5)
            return cmp5(a.mod2, b.mod5);

        if (ai == 2 && bi == 6)
            return cmp1(a.mod2, b.mod6);

        /**********************************************************************/

        if (ai == 3 && bi == 0)
            return cmp0(a.mod3, b.mod0);

        if (ai == 3 && bi == 1)
            return cmp0(a.mod3, b.mod1);

        if (ai == 3 && bi == 2)
            return cmp5(a.mod3, b.mod2);

        if (ai == 3 && bi == 3)
            return cmp0(a.mod3, b.mod3);

        if (ai == 3 && bi == 4)
            return cmp4(a.mod3, b.mod4);

        if (ai == 3 && bi == 5)
            return cmp5(a.mod3, b.mod5);

        if (ai == 3 && bi == 6)
            return cmp4(a.mod3, b.mod6);

        /**********************************************************************/

        if (ai == 4 && bi == 0)
            return cmp3(a.mod4, b.mod0);

        if (ai == 4 && bi == 1)
            return cmp6(a.mod4, b.mod1);

        if (ai == 4 && bi == 2)
            return cmp6(a.mod4, b.mod2);

        if (ai == 4 && bi == 3)
            return cmp4(a.mod4, b.mod3);

        if (ai == 4 && bi == 4)
            return cmp3(a.mod4, b.mod4);

        if (ai == 4 && bi == 5)
            return cmp3(a.mod4, b.mod5);

        if (ai == 4 && bi == 6)
            return cmp4(a.mod4, b.mod6);

        /**********************************************************************/

        if (ai == 5 && bi == 0)
            return cmp3(a.mod5, b.mod0);

        if (ai == 5 && bi == 1)
            return cmp2(a.mod5, b.mod1);

        if (ai == 5 && bi == 2)
            return cmp5(a.mod5, b.mod2);

        if (ai == 5 && bi == 3)
            return cmp5(a.mod5, b.mod3);

        if (ai == 5 && bi == 4)
            return cmp3(a.mod5, b.mod4);

        if (ai == 5 && bi == 5)
            return cmp2(a.mod5, b.mod5);

        if (ai == 5 && bi == 6)
            return cmp2(a.mod5, b.mod6);

        /**********************************************************************/

        if (ai == 6 && bi == 0)
            return cmp1(a.mod6, b.mod0);

        if (ai == 6 && bi == 1)
            return cmp2(a.mod6, b.mod1);

        if (ai == 6 && bi == 2)
            return cmp1(a.mod6, b.mod2);

        if (ai == 6 && bi == 3)
            return cmp4(a.mod6, b.mod3);

        if (ai == 6 && bi == 4)
            return cmp4(a.mod6, b.mod4);

        if (ai == 6 && bi == 5)
            return cmp2(a.mod6, b.mod5);

        if (ai == 6 && bi == 6)
            return cmp1(a.mod6, b.mod6);

        /**********************************************************************/

        abort();
    }
};

template <typename Index, typename Char>
struct CharsRanks013 {
    Chars<Char> chars;
    Index       rank0;
    Index       rank1;
    Index       rank3;

    friend std::ostream& operator << (std::ostream& os, const CharsRanks013& c) {
        return os << "(ch=" << c.chars
                  << " r0=" << c.rank0 << " r1=" << c.rank1 << " r3=" << c.rank3 << ")";
    }
} THRILL_ATTRIBUTE_PACKED;

template <typename Index, typename Char>
struct IndexCR013Pair {
    Index                      index;
    CharsRanks013<Index, Char> cr0;
    CharsRanks013<Index, Char> cr1;
} THRILL_ATTRIBUTE_PACKED;

bool IsDiffCover(size_t i) {
    size_t m = i % 7;
    return m == 0 || m == 1 || m == 3;
}

template <typename Index, typename InputDIA>
DIA<Index> DC7(const InputDIA& input_dia, size_t input_size) {
    input_dia.context().enable_consume(!debug_print && !generate_bwt);

    using Char = typename InputDIA::ValueType;
    using IndexChars = suffix_sorting::IndexChars<Index, Char>;
    using IndexRank = suffix_sorting::IndexRank<Index>;
    using Chars = suffix_sorting::Chars<Char>;

    Context& ctx = input_dia.context();

    auto tuple_sorted =
        input_dia.Keep()
        // map (t_i) -> (i,t_i,t_{i+1},...,t_{i+6}) where i = 0,1,3 mod 7
        .template FlatWindow<IndexChars>(
            7, [input_size](size_t index, const RingBuffer<Char>& r, auto emit) {
                if (IsDiffCover(index))
                    emit(IndexChars {
                             Index(index), {
                                 { r[0], r[1], r[2], r[3], r[4], r[5], r[6] }
                             }
                         });

                if (index + 7 == input_size) {
                    // emit last sentinel items.
                    if (IsDiffCover(index + 1))
                        emit(IndexChars {
                                 Index(index + 1), {
                                     { r[1], r[2], r[3], r[4],
                                       r[5], r[6], Char() }
                                 }
                             });
                    if (IsDiffCover(index + 2))
                        emit(IndexChars {
                                 Index(index + 2), {
                                     { r[2], r[3], r[4], r[5],
                                       r[6], Char(), Char() }
                                 }
                             });
                    if (IsDiffCover(index + 3))
                        emit(IndexChars {
                                 Index(index + 3), {
                                     { r[3], r[4], r[5], r[6],
                                       Char(), Char(), Char() }
                                 }
                             });
                    if (IsDiffCover(index + 4))
                        emit(IndexChars {
                                 Index(index + 4), {
                                     { r[4], r[5], r[6], Char(),
                                       Char(), Char(), Char() }
                                 }
                             });
                    if (IsDiffCover(index + 5))
                        emit(IndexChars {
                                 Index(index + 5), {
                                     { r[5], r[6], Char(), Char(),
                                       Char(), Char(), Char() }
                                 }
                             });
                    if (IsDiffCover(index + 6))
                        emit(IndexChars {
                                 Index(index + 6), {
                                     { r[6], Char(), Char(), Char(),
                                       Char(), Char(), Char() }
                                 }
                             });

                    if (input_size % 7 == 0) {
                        // emit a sentinel tuple for inputs n % 7 == 0 to
                        // separate mod0 and mod1 strings in recursive
                        // subproblem. example which needs this: aaaaaaaaaa.
                        emit(IndexChars { Index(input_size), Chars::EndSentinel() });
                    }
                    if (input_size % 7 == 1) {
                        // emit a sentinel tuple for inputs n % 7 == 1 to
                        // separate mod1 and mod3 strings in recursive
                        // subproblem. example which needs this: aaaaaaaaaa.
                        emit(IndexChars { Index(input_size), Chars::EndSentinel() });
                    }
                }
            })
        // sort tuples by contained letters
        .Sort([](const IndexChars& a, const IndexChars& b) {
                  return a.chars < b.chars;
              });

    if (debug_print)
        tuple_sorted.Keep().Print("tuple_sorted");

    // save tuple's indexes (sorted by tuple content) -> less storage
    auto tuple_index_sorted =
        tuple_sorted
        .Map([](const IndexChars& tc) { return tc.index; })
        .Cache();

    auto tuple_prerank_sums =
        tuple_sorted
        .template FlatWindow<Index>(
            2, [](size_t index, const RingBuffer<IndexChars>& rb, auto emit) {
                assert(rb.size() == 2);

                // emit one sentinel for index 0.
                if (index == 0) emit(0);

                // emit 0 or 1 depending on whether previous tuple is equal
                emit(rb[0].chars == rb[1].chars ? 0 : 1);
            })
        .PrefixSum();

    if (debug_print)
        tuple_prerank_sums.Keep().Print("tuple_prerank_sums");

    // get the last element via an associative reduce.
    Index max_lexname = tuple_prerank_sums.Keep().Max();

    // size of the mod0 part of the recursive subproblem
    const Index size_mod0 = input_size / 7 + 1;

    // size of the mod1 part of the recursive subproblem
    const Index size_mod1 = input_size / 7 + (input_size % 7 >= 1);

    // size of the mod3 part of the recursive subproblem
    const Index size_mod3 = input_size / 7 + (input_size % 7 >= 4);

    // size of both the mod0 and mod1 parts
    const Index size_mod01 = size_mod0 + size_mod1;

    // compute the size of the 3/7 subproblem.
    const Index size_subp = size_mod01 + size_mod3;

    if (debug_print) {
        sLOG1 << "max_lexname=" << max_lexname
              << " size_subp=" << size_subp
              << " size_mod0=" << size_mod0
              << " size_mod1=" << size_mod1
              << " size_mod3=" << size_mod3;
    }

    if (debug_print) {
        assert(tuple_sorted.Filter([](const IndexChars& a) {
                                       return a.index % 7 == 0;
                                   }).Size() == size_mod0);

        assert(tuple_sorted.Filter([](const IndexChars& a) {
                                       return a.index % 7 == 1;
                                   }).Size() == size_mod1);

        assert(tuple_sorted.Filter([](const IndexChars& a) {
                                       return a.index % 7 == 3;
                                   }).Size() == size_mod3);
    }

    assert(tuple_index_sorted.Keep().Size() == size_subp);

    DIA<IndexRank> ranks_rec;

    if (max_lexname + Index(1) != size_subp) {
        // some lexical name is not unique -> perform recursion on three
        // substrings (mod 0, mod 1, and mod 3)

        // zip tuples and ranks.
        auto tuple_ranks =
            tuple_index_sorted
            .Zip(tuple_prerank_sums,
                 [](const Index& tuple_index, const Index& rank) {
                     return IndexRank { tuple_index, rank };
                 });

        if (debug_print)
            tuple_ranks.Keep().Print("tuple_ranks");

        // construct recursion string with all ranks at mod 0 indices followed
        // by all ranks at mod 1 indices followed by all ranks at mod 3 indices.
        DIA<Index> string_mod013 =
            tuple_ranks
            .Sort([](const IndexRank& a, const IndexRank& b) {
                      if (a.index % 7 == b.index % 7)
                          return a.index < b.index;
                      else
                          return a.index % 7 < b.index % 7;
                  })
            .Map([](const IndexRank& tr) {
                     return tr.rank;
                 })
            .Cache();

        if (debug_print)
            string_mod013.Keep().Print("string_mod013");

        assert(string_mod013.Keep().Size() == size_subp);

        auto suffix_array_rec = DC7<Index>(string_mod013, size_subp);

        // reverse suffix array of recursion strings to find ranks for mod 0,
        // mod 1, and mod 3 positions.

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

                      return a.index < b.index;
                      // return a.index / size_mod1 < b.index / size_mod1 || (
                      //     a.index / size_mod1 == b.index / size_mod1 &&
                      //     a.index < b.index);
                  });

        if (debug_print)
            ranks_rec.Keep().Print("ranks_rec");
    }
    else {
        if (debug_print)
            tuple_index_sorted.Keep().Print("tuple_index_sorted");

        ranks_rec =
            tuple_index_sorted
            .Zip(Generate(ctx, size_subp + 1),
                 [](const Index& sa, const Index& i) {
                     return IndexRank { sa, i };
                 })
            .Sort([size_mod1](const IndexRank& a, const IndexRank& b) {
                      if (a.index % 7 == b.index % 7) {
                          // DONE(tb): changed sort order for better locality
                          // later. ... but slower?

                          return a.index < b.index;
                          // return a.index / size_mod1 < b.index / size_mod1 || (
                          //     a.index / size_mod1 == b.index / size_mod1 &&
                          //     a.index < b.index);
                      }
                      else
                          return a.index % 7 < b.index % 7;
                  })
            .Map([size_mod0, size_mod01](const IndexRank& a) {
                     return IndexRank {
                         a.index % 7 == 0 ? Index(0) :
                         a.index % 7 == 1 ? size_mod0 : size_mod01, a.rank
                     };
                 })
            .Collapse();

        if (debug_print)
            ranks_rec.Keep().Print("ranks_rec");
    }

    // *** construct StringFragments ***

    auto tuple_chars =
        input_dia
        // map (t_i) -> (i,t_i,t_{i+1},...,t_{i+6}) where i != 0,1,3 mod 7
        .template FlatWindow<Chars>(
            7, [input_size](size_t index, const RingBuffer<Char>& r, auto emit) {
                if (index % 7 == 0)
                    emit(Chars {
                             { r[0], r[1], r[2], r[3], r[4], r[5], r[6] }
                         });

                if (index + 7 == input_size) {
                    // emit last sentinel items.
                    if ((index + 1) % 7 == 0)
                        emit(Chars {
                                 { r[1], r[2], r[3], r[4], r[5], r[6], Char() }
                             });
                    if ((index + 2) % 7 == 0)
                        emit(Chars {
                                 { r[2], r[3], r[4], r[5], r[6], Char(), Char() }
                             });
                    if ((index + 3) % 7 == 0)
                        emit(Chars {
                                 { r[3], r[4], r[5], r[6], Char(), Char(), Char() }
                             });
                    if ((index + 4) % 7 == 0)
                        emit(Chars {
                                 { r[4], r[5], r[6], Char(), Char(), Char(), Char() }
                             });
                    if ((index + 5) % 7 == 0)
                        emit(Chars {
                                 { r[5], r[6], Char(), Char(), Char(), Char(), Char() }
                             });
                    if ((index + 6) % 7 == 0)
                        emit(Chars {
                                 { r[6], Char(), Char(), Char(), Char(), Char(), Char() }
                             });
                }
            });

    auto ranks_mod0 =
        ranks_rec
        .Filter([size_mod0](const IndexRank& a) {
                    return a.index < size_mod0;
                })
        .Map([](const IndexRank& a) {
                 // add one to ranks such that zero can be used as sentinel
                 // for suffixes beyond the end of the string.
                 return a.rank + Index(1);
             });

    auto ranks_mod1 =
        ranks_rec
        .Filter([size_mod0, size_mod01](const IndexRank& a) {
                    return a.index >= size_mod0 && a.index < size_mod01;
                })
        .Map([](const IndexRank& a) {
                 return a.rank + Index(1);
             });

    auto ranks_mod3 =
        ranks_rec
        .Filter([size_mod01](const IndexRank& a) {
                    return a.index >= size_mod01;
                })
        .Map([](const IndexRank& a) {
                 return a.rank + Index(1);
             });

    if (debug_print) {
        tuple_chars.Keep().Print("tuple_chars");
        ranks_mod0.Keep().Print("ranks_mod0");
        ranks_mod1.Keep().Print("ranks_mod1");
        ranks_mod3.Keep().Print("ranks_mod3");
    }

    assert(ranks_mod0.Keep().Size() == size_mod0);
    assert(ranks_mod1.Keep().Size() == size_mod1);
    assert(ranks_mod3.Keep().Size() == size_mod3);

    // Zip together the three arrays, create pairs, and extract needed
    // tuples into string fragments.

    using StringFragmentMod0 = suffix_sorting::StringFragmentMod0<Index, Char>;
    using StringFragmentMod1 = suffix_sorting::StringFragmentMod1<Index, Char>;
    using StringFragmentMod2 = suffix_sorting::StringFragmentMod2<Index, Char>;
    using StringFragmentMod3 = suffix_sorting::StringFragmentMod3<Index, Char>;
    using StringFragmentMod4 = suffix_sorting::StringFragmentMod4<Index, Char>;
    using StringFragmentMod5 = suffix_sorting::StringFragmentMod5<Index, Char>;
    using StringFragmentMod6 = suffix_sorting::StringFragmentMod6<Index, Char>;

    using CharsRanks013 = suffix_sorting::CharsRanks013<Index, Char>;
    using IndexCR013Pair = suffix_sorting::IndexCR013Pair<Index, Char>;

    auto zip_tuple_pairs1 =
        ZipPadding(
            [](const Chars& ch,
               const Index& mod0, const Index& mod1, const Index& mod3) {
                return CharsRanks013 { ch, mod0, mod1, mod3 };
            },
            std::make_tuple(Chars::EndSentinel(), 0, 0, 0),
            tuple_chars, ranks_mod0, ranks_mod1, ranks_mod3);

    if (debug_print)
        zip_tuple_pairs1.Keep().Print("zip_tuple_pairs1");

    auto zip_tuple_pairs =
        zip_tuple_pairs1
        .template FlatWindow<IndexCR013Pair>(
            2, [size_mod0](
                size_t index, const RingBuffer<CharsRanks013>& rb, auto emit) {
                emit(IndexCR013Pair { Index(7 * index), rb[0], rb[1] });
                if (index + 2 == size_mod0) {
                    // emit last sentinel
                    emit(IndexCR013Pair {
                             Index(7 * (index + 1)), rb[1],
                             CharsRanks013 { Chars::EndSentinel(), 0, 0, 0 }
                         });
                }
            });

    auto fragments_mod0 =
        zip_tuple_pairs
        .Map([](const IndexCR013Pair& ip) {
                 return StringFragmentMod0 {
                     ip.index,
                     ip.cr0.chars.ch[0], ip.cr0.chars.ch[1], ip.cr0.chars.ch[2],
                     ip.cr0.rank0, ip.cr0.rank1, ip.cr0.rank3
                 };
             })
        .Filter([input_size](const StringFragmentMod0& mod0) {
                    return mod0.index < input_size;
                });

    auto fragments_mod1 =
        zip_tuple_pairs
        .Map([](const IndexCR013Pair& ip) {
                 return StringFragmentMod1 {
                     ip.index + Index(1),
                     ip.cr0.chars.ch[1], ip.cr0.chars.ch[2], ip.cr0.chars.ch[3],
                     ip.cr0.chars.ch[4], ip.cr0.chars.ch[5], ip.cr0.chars.ch[6],
                     ip.cr0.rank1, ip.cr0.rank3, ip.cr1.rank0
                 };
             })
        .Filter([input_size](const StringFragmentMod1& mod1) {
                    return mod1.index < input_size;
                });

    auto fragments_mod2 =
        zip_tuple_pairs
        .Map([](const IndexCR013Pair& ip) {
                 return StringFragmentMod2 {
                     ip.index + Index(2),
                     ip.cr0.chars.ch[2], ip.cr0.chars.ch[3], ip.cr0.chars.ch[4],
                     ip.cr0.chars.ch[5], ip.cr0.chars.ch[6], ip.cr1.chars.ch[0],
                     ip.cr0.rank3, ip.cr1.rank0, ip.cr1.rank1
                 };
             })
        .Filter([input_size](const StringFragmentMod2& mod2) {
                    return mod2.index < input_size;
                });

    auto fragments_mod3 =
        zip_tuple_pairs
        .Map([](const IndexCR013Pair& ip) {
                 return StringFragmentMod3 {
                     ip.index + Index(3),
                     ip.cr0.chars.ch[3], ip.cr0.chars.ch[4], ip.cr0.chars.ch[5],
                     ip.cr0.chars.ch[6], ip.cr1.chars.ch[0],
                     ip.cr0.rank3, ip.cr1.rank0, ip.cr1.rank1
                 };
             })
        .Filter([input_size](const StringFragmentMod3& mod3) {
                    return mod3.index < input_size;
                });

    auto fragments_mod4 =
        zip_tuple_pairs
        .Map([](const IndexCR013Pair& ip) {
                 return StringFragmentMod4 {
                     ip.index + Index(4),
                     ip.cr0.chars.ch[4], ip.cr0.chars.ch[5], ip.cr0.chars.ch[6],
                     ip.cr1.chars.ch[0], ip.cr1.chars.ch[1], ip.cr1.chars.ch[2],
                     ip.cr1.rank0, ip.cr1.rank1, ip.cr1.rank3
                 };
             })
        .Filter([input_size](const StringFragmentMod4& mod4) {
                    return mod4.index < input_size;
                });

    auto fragments_mod5 =
        zip_tuple_pairs
        .Map([](const IndexCR013Pair& ip) {
                 return StringFragmentMod5 {
                     ip.index + Index(5),
                     ip.cr0.chars.ch[5], ip.cr0.chars.ch[6], ip.cr1.chars.ch[0],
                     ip.cr1.chars.ch[1], ip.cr1.chars.ch[2],
                     ip.cr1.rank0, ip.cr1.rank1, ip.cr1.rank3
                 };
             })
        .Filter([input_size](const StringFragmentMod5& mod5) {
                    return mod5.index < input_size;
                });

    auto fragments_mod6 =
        zip_tuple_pairs
        .Map([](const IndexCR013Pair& ip) {
                 return StringFragmentMod6 {
                     ip.index + Index(6),
                     ip.cr0.chars.ch[6], ip.cr1.chars.ch[0], ip.cr1.chars.ch[1],
                     ip.cr1.chars.ch[2],
                     ip.cr1.rank0, ip.cr1.rank1, ip.cr1.rank3
                 };
             })
        .Filter([input_size](const StringFragmentMod6& mod6) {
                    return mod6.index < input_size;
                });

    if (debug_print) {
        fragments_mod0.Keep().Print("fragments_mod0");
        fragments_mod1.Keep().Print("fragments_mod1");
        fragments_mod2.Keep().Print("fragments_mod2");
        fragments_mod3.Keep().Print("fragments_mod3");
        fragments_mod4.Keep().Print("fragments_mod4");
        fragments_mod5.Keep().Print("fragments_mod5");
        fragments_mod6.Keep().Print("fragments_mod6");
    }

    // Sort the three string fragment sets

    auto sorted_fragments_mod0 =
        fragments_mod0
        .Sort([](const StringFragmentMod0& a, const StringFragmentMod0& b) {
                  return a.r0 < b.r0;
              });

    auto sorted_fragments_mod1 =
        fragments_mod1
        .Sort([](const StringFragmentMod1& a, const StringFragmentMod1& b) {
                  return a.r0 < b.r0;
              });

    auto sorted_fragments_mod2 =
        fragments_mod2
        .Sort([](const StringFragmentMod2& a, const StringFragmentMod2& b) {
                  return std::tie(a.t0, a.r1) < std::tie(b.t0, b.r1);
              });

    auto sorted_fragments_mod3 =
        fragments_mod3
        .Sort([](const StringFragmentMod3& a, const StringFragmentMod3& b) {
                  return a.r0 < b.r0;
              });

    auto sorted_fragments_mod4 =
        fragments_mod4
        .Sort([](const StringFragmentMod4& a, const StringFragmentMod4& b) {
                  return std::tie(a.t0, a.t1, a.t2, a.r3)
                  < std::tie(b.t0, b.t1, b.t2, b.r3);
              });

    auto sorted_fragments_mod5 =
        fragments_mod5
        .Sort([](const StringFragmentMod5& a, const StringFragmentMod5& b) {
                  return std::tie(a.t0, a.t1, a.r2) < std::tie(b.t0, b.t1, b.r2);
              });

    auto sorted_fragments_mod6 =
        fragments_mod6
        .Sort([](const StringFragmentMod6& a, const StringFragmentMod6& b) {
                  return std::tie(a.t0, a.r1) < std::tie(b.t0, b.r1);
              });

    if (debug_print) {
        sorted_fragments_mod0.Keep().Print("sorted_fragments_mod0");
        sorted_fragments_mod1.Keep().Print("sorted_fragments_mod1");
        sorted_fragments_mod2.Keep().Print("sorted_fragments_mod2");
        sorted_fragments_mod3.Keep().Print("sorted_fragments_mod3");
        sorted_fragments_mod4.Keep().Print("sorted_fragments_mod4");
        sorted_fragments_mod5.Keep().Print("sorted_fragments_mod5");
        sorted_fragments_mod6.Keep().Print("sorted_fragments_mod6");
    }

    using StringFragment = suffix_sorting::StringFragment<Index, Char>;

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

    auto string_fragments_mod3 =
        sorted_fragments_mod3
        .Map([](const StringFragmentMod3& mod3)
             { return StringFragment(mod3); });

    auto string_fragments_mod4 =
        sorted_fragments_mod4
        .Map([](const StringFragmentMod4& mod4)
             { return StringFragment(mod4); });

    auto string_fragments_mod5 =
        sorted_fragments_mod5
        .Map([](const StringFragmentMod5& mod5)
             { return StringFragment(mod5); });

    auto string_fragments_mod6 =
        sorted_fragments_mod6
        .Map([](const StringFragmentMod6& mod6)
             { return StringFragment(mod6); });

    // merge and map to only suffix array

    auto suffix_array =
        Merge(FragmentComparator<StringFragment>(),
              string_fragments_mod0,
              string_fragments_mod1,
              string_fragments_mod2,
              string_fragments_mod3,
              string_fragments_mod4,
              string_fragments_mod5,
              string_fragments_mod6)
        .Map([](const StringFragment& a) { return a.index; })
        .Execute();

    // debug output

    if (debug_print) {
        std::vector<Char> input_vec = input_dia.Keep().Gather();
        std::vector<Index> vec = suffix_array.Keep().Gather();

        if (ctx.my_rank() == 0) {
            size_t p = 0;
            for (const Index& index : vec) {
                std::cout << std::setw(5) << p << std::setw(5) << index << " =";
                for (Index i = index; i < index + Index(64) && i < input_size; ++i) {
                    if (input_vec[i] == '\n')
                        std::cout << ' ' << ' ';
                    else
                        std::cout << ' ' << input_vec[i];
                }
                std::cout << '\n';
                ++p;
            }
        }
    }

    // check result: requires enable_consume(false)
    // die_unless(CheckSA(input_dia, suffix_array.Collapse()));

    return suffix_array.Collapse();
}

/*!
 * Class to encapsulate all
 */
class StartDC7
{
public:
    StartDC7(
        Context& ctx,
        const std::string& input_path, const std::string& output_path,
        uint64_t sizelimit,
        bool text_output_flag,
        bool check_flag,
        bool input_verbatim,
        size_t sa_index_bytes)
        : ctx_(ctx),
          input_path_(input_path), output_path_(output_path),
          sizelimit_(sizelimit),
          text_output_flag_(text_output_flag),
          check_flag_(check_flag),
          input_verbatim_(input_verbatim),
          sa_index_bytes_(sa_index_bytes) { }

    void Run() {
        ctx_.enable_consume();
        if (input_verbatim_) {
            // take path as verbatim text
            std::vector<uint8_t> input_vec(input_path_.begin(), input_path_.end());
            DIA<uint8_t> input_dia = EqualToDIA<uint8_t>(ctx_, input_vec);
            SwitchDC7IndexType(input_dia, input_vec.size());
        }
        else if (input_path_ == "unary") {
            if (sizelimit_ == std::numeric_limits<size_t>::max()) {
                LOG1 << "You must provide -s <size> for generated inputs.";
                return;
            }

            DIA<uint8_t> input_dia = Generate(
                ctx_, [](size_t /* i */) { return uint8_t('a'); }, sizelimit_);
            SwitchDC7IndexType(input_dia, sizelimit_);
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
                .Cache().KeepForever();
            SwitchDC7IndexType(input_dia, sizelimit_);
        }
        else {
            DIA<uint8_t> input_dia = ReadBinary<uint8_t>(ctx_, input_path_);
            size_t input_size = input_dia.Size();
            SwitchDC7IndexType(input_dia, input_size);
        }
    }

    template <typename Index, typename InputDIA>
    void StartDC7Input(const InputDIA& input_dia, uint64_t input_size) {

        // run DC7
        auto suffix_array = DC7<Index>(input_dia, input_size);
        InputDIA bw_transform;
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
        if (generate_bwt) {
            bw_transform = GenerateBWT(input_dia, suffix_array);

            if (text_output_flag_) {
                bw_transform.Print("bw_transform");
            }
            if (output_path_.size()) {
                LOG1 << "writing Burrows–Wheeler transform to " << output_path_;
                bw_transform.WriteBinary(output_path_ + "bwt");
            }
        }
    }

    template <typename InputDIA>
    void SwitchDC7IndexType(const InputDIA& input_dia, uint64_t input_size) {
        if (sa_index_bytes_ == 4)
            return StartDC7Input<uint32_t>(input_dia, input_size);
#ifdef NDEBUG
        else if (sa_index_bytes_ == 5)
            return StartDC7Input<common::uint40>(input_dia, input_size);
        else if (sa_index_bytes_ == 6)
            return StartDC7Input<common::uint48>(input_dia, input_size);
        else if (sa_index_bytes_ == 8)
            return StartDC7Input<uint64_t>(input_dia, input_size);
#endif
        else
            die("Unsupported index byte size: " << sa_index_bytes_);
    }

protected:
    Context& ctx_;

    std::string input_path_;
    std::string output_path_;

    uint64_t sizelimit_;
    bool text_output_flag_;
    bool check_flag_;
    bool input_verbatim_;
    size_t sa_index_bytes_;
};

} // namespace suffix_sorting
} // namespace examples

int main(int argc, char* argv[]) {

    using namespace thrill; // NOLINT

    common::CmdlineParser cp;

    cp.SetDescription("DC7 aka skew7 algorithm for suffix array construction.");
    cp.SetAuthor("Timo Bingmann <tb@panthema.net>");

    std::string input_path, output_path;
    uint64_t sizelimit = std::numeric_limits<uint64_t>::max();
    bool text_output_flag = false;
    bool check_flag = false;
    bool input_verbatim = false;
    size_t sa_index_bytes = 4;

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
    cp.AddFlag('d', "debug", examples::suffix_sorting::debug_print,
               "Print debug info.");
    cp.AddSizeT('b', "bytes", sa_index_bytes,
                "suffix array bytes per index: "
                "4 (32-bit), 5 (40-bit), 6 (48-bit), 8 (64-bit)");
    cp.AddFlag('w', "bwt", examples::suffix_sorting::generate_bwt,
               "Compute the Burrows–Wheeler transform in addition to the "
               "suffix array.");

    // process command line
    if (!cp.Process(argc, argv))
        return -1;

    return Run(
        [&](Context& ctx) {
            return examples::suffix_sorting::StartDC7(
                ctx, input_path, output_path,
                sizelimit,
                text_output_flag,
                check_flag,
                input_verbatim, sa_index_bytes).Run();
        });
}

/******************************************************************************/
