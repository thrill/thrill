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

    MultiwayMergeTreePuller(IteratorListIterator seqs_begin,
                            IteratorListIterator seqs_end,
                            size_t length, const Comparator& comp)
        : seqs_begin_(seqs_begin),
          seqs_end_(seqs_end),
          comp_(comp),
          k_(static_cast<source_type>(seqs_end_ - seqs_begin_)),
          lt_(k_, comp_),
          counter_(0),
          total_length_(0),
          arbitrary_element_(nullptr) {
        // find an arbitrary element to avoid default construction
        for (source_type t = 0; t < k_; ++t)
        {
            if (!arbitrary_element_ && iterpair_size(seqs_begin_[t]) > 0)
                arbitrary_element_ = &(*seqs_begin_[t].first);

            total_length_ += iterpair_size(seqs_begin_[t]);
        }

        for (source_type t = 0; t < k_; ++t)
        {
            if (THRILL_UNLIKELY(seqs_begin_[t].first == seqs_begin_[t].second))
                lt_.insert_start(*arbitrary_element_, t, true);
            else
                lt_.insert_start(*seqs_begin_[t].first, t, false);
        }

        lt_.init();
        total_length_ = std::min(total_length_, length);
    }

    bool HasNext() {
        return (counter_ < total_length_);
    }

    ValueIn Next() {
        assert(counter_ < total_length_);

        // take out
        source_type source = lt_.get_min_source();
        ValueIn res = *seqs_begin_[source].first;
        ++seqs_begin_[source].first;

        // feed
        if (seqs_begin_[source].first == seqs_begin_[source].second)
            lt_.delete_min_insert(*arbitrary_element_, true);
        else
            // replace from same source
            lt_.delete_min_insert(*seqs_begin_[source].first, false);

        ++counter_;
        return res;
    }

    IteratorListIterator seqs_begin_;
    IteratorListIterator seqs_end_;
    Comparator comp_;
    source_type k_;
    LoserTreeType lt_;
    size_t counter_;
    size_t total_length_;
    const ValueIn* arbitrary_element_;
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
