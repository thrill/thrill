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
template <typename ValueType, typename ParentDIA,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByNode;

template <typename ValueType, typename ParentDIA,
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
              typename T5>
    friend class GroupByNode;

    template <typename T1,
              typename T2,
              typename T3,
              typename T4>
    friend class GroupToIndexNode;

public:
    static constexpr bool debug = false;
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
            old_key_ = std::move(new_key_);
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
              typename T4>
    friend class GroupToIndexNode;

public:
    static constexpr bool debug = false;
    using ValueIn = ValueType;
    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;
    using Puller = core::MultiwayMergeTree<
              ValueIn, std::vector<data::File::ConsumeReader>::iterator, Comparator>;

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
            old_key_ = std::move(new_key_);
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

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUP_BY_ITERATOR_HEADER

/******************************************************************************/
