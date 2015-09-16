/*******************************************************************************
 * thrill/api/groupby.hpp
 *
 * DIANode for a groupby operation. Performs the actual groupby operation
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_GROUPBY_ITERATOR_HEADER
#define THRILL_API_GROUPBY_ITERATOR_HEADER

#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/iterator_wrapper.hpp>

#include <utility>
#include <vector>


namespace thrill {
namespace api {

template <typename ValueType, typename ParentDIARef,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByNode;

template <typename ValueType, typename ParentDIARef,
          typename KeyExtractor, typename GroupFunction, typename HashFunction>
class GroupByIndexNode;

template <typename ValueType, typename KeyExtractor>
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

    GroupByIterator(Reader& reader, KeyExtractor& key_extractor)
        : reader_(reader),
          key_extractor_(key_extractor),
          is_first_elem_(true),
          is_reader_empty(false),
          elem_(reader.template Next<ValueIn>()),
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

protected:
    bool HasNextForReal() {
        is_first_elem_ = true;
        return !is_reader_empty;
    }

    Key GetNextKey() {
        return new_key_;
    }

private:
    Reader& reader_;
    KeyExtractor& key_extractor_;
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

} // namespace api
} // namespace thrill

#endif // !THRILL_API_GROUPBY_ITERATOR_HEADER

/******************************************************************************/
