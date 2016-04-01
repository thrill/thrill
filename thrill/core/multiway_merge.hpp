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

template <typename ValueType, typename ReaderIterator, typename Comparator>
class MultiwayMergeTree
{
public:
    using Reader = typename std::iterator_traits<ReaderIterator>::value_type;

    using LoserTreeType = typename core::LoserTreeTraits<
              /* stable */ false, ValueType, Comparator>::Type;

    MultiwayMergeTree(ReaderIterator readers_begin, ReaderIterator readers_end,
                      const Comparator& comp)
        : readers_(readers_begin),
          num_inputs_(static_cast<unsigned>(readers_end - readers_begin)),
          remaining_inputs_(num_inputs_),
          lt_(static_cast<unsigned>(num_inputs_), comp),
          current_(num_inputs_) {

        for (unsigned t = 0; t < num_inputs_; ++t)
        {
            if (THRILL_LIKELY(readers_[t].HasNext())) {
                current_[t].first = true;
                current_[t].second = readers_[t].template Next<ValueType>();
                lt_.insert_start(&current_[t].second, t, false);
            }
            else {
                current_[t].first = false;
                lt_.insert_start(nullptr, t, true);
                assert(remaining_inputs_ > 0);
                --remaining_inputs_;
            }
        }

        lt_.init();
    }

    bool HasNext() const {
        return (remaining_inputs_ != 0);
    }

    ValueType Next() {

        // take next smallest element out
        unsigned top = lt_.min_source();
        ValueType res = std::move(current_[top].second);

        if (THRILL_LIKELY(readers_[top].HasNext())) {
            current_[top].first = true;
            current_[top].second = readers_[top].template Next<ValueType>();
            lt_.delete_min_insert(&current_[top].second, false);
        }
        else {
            current_[top].first = false;
            lt_.delete_min_insert(nullptr, true);
            assert(remaining_inputs_ > 0);
            --remaining_inputs_;
        }

        return res;
    }

private:
    ReaderIterator readers_;
    unsigned num_inputs_;
    size_t remaining_inputs_;

    LoserTreeType lt_;
    //! current values in each input (exist flag, value)
    std::vector<std::pair<bool, ValueType> > current_;
};

/*!
 * Sequential multi-way merging switch for a file writer as output
 *
 * The decision if based on the branching factor and runtime settings.
 *
 * \param seqs_begin Begin iterator of iterator pair input sequence.
 * \param seqs_end End iterator of iterator pair input sequence.
 * \param comp Comparator.
 * \tparam Stable Stable merging incurs a performance penalty.
 * \tparam Sentinels The sequences have a sentinel element.
 * \return End iterator of output sequence.
 */
template <typename ValueType, typename ReaderIterator, typename Comparator>
auto make_multiway_merge_tree(
    ReaderIterator seqs_begin, ReaderIterator seqs_end,
    const Comparator &comp) {

    assert(seqs_end - seqs_begin >= 1);
    return MultiwayMergeTree<
        ValueType, ReaderIterator,
        Comparator>(seqs_begin, seqs_end, comp);
}

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_MULTIWAY_MERGE_HEADER

/******************************************************************************/
