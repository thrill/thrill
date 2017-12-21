/*******************************************************************************
 * thrill/common/radix_sort.hpp
 *
 * An implementations of generic 8-bit radix sort using key caching (requires n
 * extra bytes of memory) and in-place permutation reordering.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2012-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_RADIX_SORT_HEADER
#define THRILL_COMMON_RADIX_SORT_HEADER

#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <functional>
#include <type_traits>

namespace thrill {
namespace common {

/*!
 * Internal helper method, use radix_sort_CI below.
 */
template <
    size_t MaxDepth, typename Iterator, typename Char,
    typename Comparator =
        std::less<typename std::iterator_traits<Iterator>::value_type>,
    typename SubSorter = NoOperation<void> >
static inline
void radix_sort_CI(Iterator begin, Iterator end, size_t K,
                   const Comparator& cmp,
                   const SubSorter& sub_sort, size_t depth,
                   Char* char_cache) {

    const size_t size = end - begin;
    if (size < 32)
        return std::sort(begin, end, cmp);

    using value_type = typename std::iterator_traits<Iterator>::value_type;

    // cache characters
    Char* cc = char_cache;
    for (Iterator it = begin; it != end; ++it, ++cc) {
        *cc = it->at_radix(depth);
        assert(static_cast<size_t>(*cc) < K);
    }

    // count character occurrences
    size_t* bkt_size = reinterpret_cast<size_t*>(alloca(K * sizeof(size_t)));
    std::fill(bkt_size, bkt_size + K, 0);
    for (const Char* cci = char_cache; cci != char_cache + size; ++cci)
        ++bkt_size[*cci];

    // inclusive prefix sum
    size_t* bkt_index = reinterpret_cast<size_t*>(alloca(K * sizeof(size_t)));
    bkt_index[0] = bkt_size[0];
    size_t last_bkt_size = bkt_size[0];
    for (size_t i = 1; i < K; ++i) {
        bkt_index[i] = bkt_index[i - 1] + bkt_size[i];
        if (bkt_size[i]) last_bkt_size = bkt_size[i];
    }

    // premute in-place
    for (size_t i = 0, j; i < size - last_bkt_size; )
    {
        value_type v = std::move(begin[i]);
        Char vc = std::move(char_cache[i]);
        while ((j = --bkt_index[vc]) > i)
        {
            using std::swap;
            swap(v, begin[j]);
            swap(vc, char_cache[j]);
        }
        begin[i] = std::move(v);
        i += bkt_size[vc];
    }

    if (depth + 1 == MaxDepth) {
        // allow post-radix sorting when max depth is reached
        size_t bsum = 0;
        for (size_t i = 0; i < K; bsum += bkt_size[i++]) {
            if (bkt_size[i] <= 1) continue;
            sub_sort(begin + bsum, begin + bsum + bkt_size[i], cmp);
        }
    }
    else {
        // recurse
        size_t bsum = 0;
        for (size_t i = 0; i < K; bsum += bkt_size[i++]) {
            if (bkt_size[i] <= 1) continue;
            radix_sort_CI<MaxDepth>(
                begin + bsum, begin + bsum + bkt_size[i],
                K, cmp, sub_sort, depth + 1, char_cache);
        }
    }
}

/*!
 * Radix sort the iterator range [begin,end). Sort unconditionally up to depth
 * MaxDepth, then call the sub_sort method for further sorting. Small buckets
 * are sorted using std::sort() with given comparator. Characters are extracted
 * from items in the range using the at_radix(depth) method. All character
 * values must be less than K (the counting array size).
 */
template <
    size_t MaxDepth, typename Iterator,
    typename Comparator =
        std::less<typename std::iterator_traits<Iterator>::value_type>,
    typename SubSorter = NoOperation<void> >
static inline
void radix_sort_CI(Iterator begin, Iterator end, size_t K,
                   const Comparator& cmp = Comparator(),
                   const SubSorter& sub_sort = SubSorter()) {

    if (MaxDepth == 0) {
        // allow post-radix sorting when max depth is reached
        sub_sort(begin, end, cmp);
        return;
    }

    const size_t size = end - begin;

    using CharRet = decltype(begin->at_radix(0));
    using Char = typename std::remove_cv<
              typename std::remove_reference<CharRet>::type>::type;

    // allocate character cache once
    Char* char_cache = new Char[size];
    radix_sort_CI<MaxDepth>(
        begin, end, K, cmp, sub_sort, /* depth */ 0, char_cache);
    delete[] char_cache;
}

/*!
 * SortAlgorithm class for use with api::Sort() which calls radix_sort_CI() if K
 * is small enough.
 */
template <typename Type, size_t MaxDepth>
class RadixSort
{
public:
    explicit RadixSort(size_t K) : K_(K) { }
    template <typename Iterator, typename CompareFunction>
    void operator () (Iterator begin, Iterator end,
                      const CompareFunction& cmp) const {
        if (K_ < 4096)
            thrill::common::radix_sort_CI<MaxDepth>(begin, end, K_, cmp);
        else
            std::sort(begin, end, cmp);
    }

private:
    const size_t K_;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_RADIX_SORT_HEADER

/******************************************************************************/
