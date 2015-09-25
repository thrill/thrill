/*******************************************************************************
 * thrill/api/groupby_iterator.hpp
 *
 * DIANode for a groupby operation. Performs the actual groupby operation
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GROUPBY_ITERATOR_HEADER
#define THRILL_API_GROUPBY_ITERATOR_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/iterator_wrapper.hpp>
#include <thrill/core/losertree.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

template <typename ValueIn,
          typename Comparator>
struct MultiwayMergeTreePuller {
    using Iterator = thrill::core::FileIteratorWrapper<ValueIn>;
    using IteratorListIterator = typename std::vector<std::pair<Iterator, Iterator> >::iterator;
    using LoserTreeType = core::LoserTreePointer<true, ValueIn, Comparator>;
    using source_type = typename LoserTreeType::source_type;

    MultiwayMergeTreePuller(IteratorListIterator seqs_begin_,
                            IteratorListIterator seqs_end_,
                            size_t length,
                            Comparator comp_) :
        seqs_begin(seqs_begin_),
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

    bool    HasNext() {
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
    Comparator           comp;
    source_type          k;
    LoserTreeType        lt;
    size_t               counter;
    size_t               total_length;
    const ValueIn        * arbitrary_element;
    const bool           is_multiway_merged;
};

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

// forward declarations for friend classes
template <typename ValueType, typename ParentDIA,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByNode;

template <typename ValueType, typename ParentDIA,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByIndexNode;

////////////////////////////////////////////////////////////////////////////////

template <typename ValueType, typename KeyExtractor, typename Comparator>
class GroupByIterator
{
    template <typename T1,
              typename T2,
              typename T3,
              typename T4,
              typename T5>
    friend class GroupByNode;

    template <typename T1,
              typename T2,
              typename T3,
              typename T4,
              typename T5>
    friend class GroupByIndexNode;

public:
    static const bool debug = false;
    using ValueIn = ValueType;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using Reader = typename data::File::Reader;

    GroupByIterator(Reader& reader, const KeyExtractor& key_extractor)
        : reader_(reader),
          key_extractor_(key_extractor),
          is_first_elem_(true),
          is_reader_empty(false),
          elem_(reader_.template Next<ValueIn>()),
          old_key_(key_extractor_(elem_)),
          new_key_(old_key_) { }

    bool HasNext() {
        return (!is_reader_empty && old_key_ == new_key_) || is_first_elem_;
    }

    ValueIn Next() {
        assert(!is_reader_empty);
        ValueIn elem = elem_;
        GetNextElem();
        return elem;
    }

private:
    bool HasNextForReal() {
        is_first_elem_ = true;
        return !is_reader_empty;
    }

    Key GetNextKey() {
        return new_key_;
    }

private:
    Reader& reader_;
    const KeyExtractor& key_extractor_;
    bool is_first_elem_;
    bool is_reader_empty;
    ValueIn elem_;
    Key old_key_;
    Key new_key_;

    void GetNextElem() {
        is_first_elem_ = false;
        if (reader_.HasNext()) {
            elem_ = reader_.template Next<ValueIn>();
            old_key_ = new_key_;
            new_key_ = key_extractor_(elem_);
        }
        else {
            is_reader_empty = true;
        }
    }

    void SetFirstElem() {
        assert(reader_.HasNext());
        is_first_elem_ = true;
        elem_ = reader_.template Next<ValueIn>();
        old_key_ = key_extractor_(elem_);
        new_key_ = old_key_;
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename ValueType, typename KeyExtractor, typename Comparator>
class GroupByMultiwayMergeIterator
{
    template <typename T1,
              typename T2,
              typename T3,
              typename T4,
              typename T5>
    friend class GroupByNode;

    template <typename T1,
              typename T2,
              typename T3,
              typename T4,
              typename T5>
    friend class GroupByIndexNode;

public:
    static const bool debug = false;
    using ValueIn = ValueType;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using Puller = MultiwayMergeTreePuller<ValueIn, Comparator>;

    GroupByMultiwayMergeIterator(Puller& reader, const KeyExtractor& key_extractor)
        : reader_(reader),
          key_extractor_(key_extractor),
          is_first_elem_(true),
          is_reader_empty(false),
          elem_(reader_.Next()),
          old_key_(key_extractor_(elem_)),
          new_key_(old_key_) { }

    bool HasNext() {
        return (!is_reader_empty && old_key_ == new_key_) || is_first_elem_;
    }

    ValueIn Next() {
        assert(!is_reader_empty);
        ValueIn elem = elem_;
        GetNextElem();
        return elem;
    }

private:
    bool HasNextForReal() {
        is_first_elem_ = true;
        return !is_reader_empty;
    }

    Key GetNextKey() {
        return new_key_;
    }

private:
    Puller& reader_;
    const KeyExtractor& key_extractor_;
    bool is_first_elem_;
    bool is_reader_empty;
    ValueIn elem_;
    Key old_key_;
    Key new_key_;

    void GetNextElem() {
        is_first_elem_ = false;
        if (reader_.HasNext()) {
            elem_ = reader_.Next();
            old_key_ = new_key_;
            new_key_ = key_extractor_(elem_);
        }
        else {
            is_reader_empty = true;
        }
    }

    void SetFirstElem() {
        assert(reader_.HasNext());
        is_first_elem_ = true;
        elem_ = reader_.Next();
        old_key_ = key_extractor_(elem_);
        new_key_ = old_key_;
    }
};

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUPBY_ITERATOR_HEADER

/******************************************************************************/
