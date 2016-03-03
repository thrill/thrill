/*******************************************************************************
 * thrill/core/multiway_merge.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_MULTIWAY_MERGE_HEADER
#define THRILL_CORE_MULTIWAY_MERGE_HEADER

#include <thrill/core/losertree.hpp>

#include <algorithm>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

template <typename ValueIn, typename Comparator>
class MultiwayMergeTreePuller
{
public:
    using Iterator = thrill::core::FileIteratorWrapper<ValueIn>;
    using IteratorListIterator = typename std::vector<std::pair<Iterator, Iterator> >::iterator;
    using LoserTreeType = core::LoserTreePointer<true, ValueIn, Comparator>;
    using source_type = typename LoserTreeType::source_type;

    //! Length of a sequence described by a pair of iterators.
    template <typename RandomAccessIteratorPair>
    typename std::iterator_traits<
        typename RandomAccessIteratorPair::first_type
        >::difference_type
    iterpair_size(const RandomAccessIteratorPair& p) {
        return p.second - p.first;
    }

    MultiwayMergeTreePuller(IteratorListIterator seqs_begin_,
                            IteratorListIterator seqs_end_,
                            size_t length,
                            Comparator comp_)
        : seqs_begin(seqs_begin_),
          seqs_end(seqs_end_),
          comp(comp_),
          k(static_cast<source_type>(seqs_end - seqs_begin)),
          lt(k, comp),
          counter(0),
          total_length(0),
          arbitrary_element(nullptr),
          is_multiway_merged(false) {
        // find an arbitrary element to avoid default construction
        for (source_type t = 0; t < k; ++t)
        {
            if (!arbitrary_element && iterpair_size(seqs_begin[t]) > 0)
                arbitrary_element = &(*seqs_begin[t].first);

            total_length += iterpair_size(seqs_begin[t]);
        }

        for (source_type t = 0; t < k; ++t)
        {
            if (THRILL_UNLIKELY(seqs_begin[t].first == seqs_begin[t].second))
                lt.insert_start(*arbitrary_element, t, true);
            else
                lt.insert_start(*seqs_begin[t].first, t, false);
        }

        lt.init();
        total_length = std::min(total_length, length);
    }

    bool HasNext() {
        return (counter < total_length);
    }

    ValueIn Next() {
        assert(counter < total_length);

        // take out
        source_type source = lt.get_min_source();
        ValueIn res = *seqs_begin[source].first;
        ++seqs_begin[source].first;

        // feed
        if (seqs_begin[source].first == seqs_begin[source].second)
            lt.delete_min_insert(*arbitrary_element, true);
        else
            // replace from same source
            lt.delete_min_insert(*seqs_begin[source].first, false);

        ++counter;
        return res;
    }

    IteratorListIterator seqs_begin;
    IteratorListIterator seqs_end;
    Comparator comp;
    source_type k;
    LoserTreeType lt;
    size_t counter;
    size_t total_length;
    const ValueIn* arbitrary_element;
    const bool is_multiway_merged;
};

/*!
 * Sequential multi-way merging switch for a file writer as output
 *
 * The decision if based on the branching factor and runtime settings.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param length Maximum length to merge.
 * \param comp Comparator.
 * \tparam Stable Stable merging incurs a performance penalty.
 * \tparam Sentinels The sequences have a sentinel element.
 * \return End iterator of output sequence.
 */
template <bool Stable, bool Sentinels,
          typename RandomAccessIteratorIterator,
          typename DiffType, typename Comparator>
// return type of hell
MultiwayMergeTreePuller<
    typename std::iterator_traits<
        typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type>::value_type,
    Comparator>
// RandomAccessIterator3
get_sequential_file_multiway_merge_tree(RandomAccessIteratorIterator seqs_begin,
                                        RandomAccessIteratorIterator seqs_end,
                                        DiffType length,
                                        Comparator comp) {
    typedef typename std::iterator_traits<RandomAccessIteratorIterator>
        ::value_type::first_type RandomAccessIterator;
    typedef typename std::iterator_traits<RandomAccessIterator>
        ::value_type value_type;

    assert(static_cast<int>(seqs_end - seqs_begin) > 1);
    MultiwayMergeTreePuller<value_type, Comparator> tree(seqs_begin, seqs_end, length, comp);

    return tree;
}

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_MULTIWAY_MERGE_HEADER

/******************************************************************************/
