/*******************************************************************************
 * thrill/api/group_by_iterator.hpp
 *
 * DIANode for a groupby operation. Performs the actual groupby operation
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GROUP_BY_ITERATOR_HEADER
#define THRILL_API_GROUP_BY_ITERATOR_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \ingroup api_layer
//! \{

////////////////////////////////////////////////////////////////////////////////

// forward declarations for friend classes
template <typename ValueType,
          typename KeyExtractor, typename GroupFunction, typename HashFunction,
          bool UseLocationDetection>
class GroupByNode;

template <typename ValueType,
          typename KeyExtractor, typename GroupFunction>
class GroupToIndexNode;

////////////////////////////////////////////////////////////////////////////////

template <typename ValueType, typename KeyExtractor, typename Comparator>
class GroupByIterator
{
    template <typename T1,
              typename T2,
              typename T3,
              typename T4,
              bool T5>
    friend class GroupByNode;

    template <typename T1,
              typename T2,
              typename T3>
    friend class GroupToIndexNode;

public:
    static constexpr bool debug = false;
    using ValueIn = ValueType;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using Reader = typename data::File::Reader;

    GroupByIterator(Reader& reader, const KeyExtractor& key_extractor)
        : reader_(reader),
          key_extractor_(key_extractor),
          is_reader_empty_(false),
          equal_key_(true),
          elem_(reader_.template Next<ValueIn>()),
          key_(key_extractor_(elem_)) { }

    //! non-copyable: delete copy-constructor
    GroupByIterator(const GroupByIterator&) = delete;
    //! non-copyable: delete assignment operator
    GroupByIterator& operator = (const GroupByIterator&) = delete;
    //! move-constructor: default
    GroupByIterator(GroupByIterator&&) = default;
    //! move-assignment operator: default
    GroupByIterator& operator = (GroupByIterator&&) = default;

    bool HasNext() {
        return (!is_reader_empty_ && equal_key_);
    }

    ValueIn Next() {
        assert(!is_reader_empty_);
        ValueIn elem = elem_;
        GetNextElem();
        return elem;
    }

private:
    bool HasNextForReal() {
        return !is_reader_empty_;
    }

    const Key& GetNextKey() {
        equal_key_ = true;
        return key_;
    }

private:
    Reader& reader_;
    const KeyExtractor& key_extractor_;
    bool is_reader_empty_;
    bool equal_key_;
    ValueIn elem_;
    Key key_;

    void GetNextElem() {
        if (reader_.HasNext()) {
            elem_ = reader_.template Next<ValueIn>();
            Key key = key_extractor_(elem_);
            if (key != key_) {
                key_ = std::move(key);
                equal_key_ = false;
            }
        }
        else {
            is_reader_empty_ = true;
        }
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
              bool T5>
    friend class GroupByNode;

    template <typename T1,
              typename T2,
              typename T3>
    friend class GroupToIndexNode;

public:
    static constexpr bool debug = false;
    using ValueIn = ValueType;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using Puller = core::MultiwayMergeTree<
        ValueIn, std::vector<data::File::Reader>::iterator, Comparator>;

    GroupByMultiwayMergeIterator(Puller& reader, const KeyExtractor& key_extractor)
        : reader_(reader),
          key_extractor_(key_extractor),
          is_reader_empty_(false),
          equal_key_(true),
          elem_(reader_.Next()),
          key_(key_extractor_(elem_)) { }

    //! non-copyable: delete copy-constructor
    GroupByMultiwayMergeIterator(const GroupByMultiwayMergeIterator&) = delete;
    //! non-copyable: delete assignment operator
    GroupByMultiwayMergeIterator& operator = (const GroupByMultiwayMergeIterator&) = delete;
    //! move-constructor: default
    GroupByMultiwayMergeIterator(GroupByMultiwayMergeIterator&&) = default;
    //! move-assignment operator: default
    GroupByMultiwayMergeIterator& operator = (GroupByMultiwayMergeIterator&&) = default;

    bool HasNext() {
        return (!is_reader_empty_ && equal_key_);
    }

    ValueIn Next() {
        assert(!is_reader_empty_);
        ValueIn elem = elem_;
        GetNextElem();
        return elem;
    }

private:
    bool HasNextForReal() {
        return !is_reader_empty_;
    }

    const Key& GetNextKey() {
        equal_key_ = true;
        return key_;
    }

private:
    Puller& reader_;
    const KeyExtractor& key_extractor_;
    bool is_reader_empty_;
    bool equal_key_;
    ValueIn elem_;
    Key key_;

    void GetNextElem() {
        if (reader_.HasNext()) {
            elem_ = reader_.Next();
            Key next_key = key_extractor_(elem_);
            if (next_key != key_) {
                key_ = std::move(next_key);
                equal_key_ = false;
            }
        }
        else {
            is_reader_empty_ = true;
        }
    }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUP_BY_ITERATOR_HEADER

/******************************************************************************/
