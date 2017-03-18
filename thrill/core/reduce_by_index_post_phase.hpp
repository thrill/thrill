/*******************************************************************************
 * thrill/core/reduce_by_index_post_phase.hpp
 *
 * Hash table with support for reduce.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_BY_INDEX_POST_PHASE_HEADER
#define THRILL_CORE_REDUCE_BY_INDEX_POST_PHASE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

template <typename TableItem, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool VolatileKey,
          typename ReduceConfig_ = DefaultReduceConfig>
class ReduceByIndexPostPhase
{
    static constexpr bool debug = false;

public:
    using ReduceConfig = ReduceConfig_;
    using MakeTableItem = ReduceMakeTableItem<Value, TableItem, VolatileKey>;
    using PhaseEmitter = ReducePostPhaseEmitter<
              TableItem, Value, Emitter, VolatileKey>;

    /*!
     * A data structure which takes an arbitrary value and extracts an index using
     * a key extractor function from that value. Afterwards, values with the same index
     * are merged together
     */
    ReduceByIndexPostPhase(
        Context& ctx,
        size_t dia_id,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const Emitter& emitter,
        const ReduceConfig& config = ReduceConfig(),
        const Value& neutral_element = Value())
        : ctx_(ctx), dia_id_(dia_id),
          key_extractor_(key_extractor), reduce_function_(reduce_function),
          config_(config), emitter_(emitter),
          neutral_element_(neutral_element) { }

    //! non-copyable: delete copy-constructor
    ReduceByIndexPostPhase(const ReduceByIndexPostPhase&) = delete;
    //! non-copyable: delete assignment operator
    ReduceByIndexPostPhase& operator = (const ReduceByIndexPostPhase&) = delete;

    void Initialize(size_t limit_memory_bytes) {
        assert(range_.IsValid() || range_.IsEmpty());
        limit_memory_bytes_ = limit_memory_bytes;

        TableItem neutral = MakeTableItem::Make(neutral_element_, key_extractor_);
        neutral_element_key_ = MakeTableItem::GetKey(neutral, key_extractor_);
        items_.resize(range_.size(), neutral);
        // TODO too big for RAM
    }

    bool Insert(const TableItem& kv) {
        size_t key = MakeTableItem::GetKey(kv, key_extractor_);
        size_t local_index = key - range_.begin;

        if (key != neutral_element_key_) { // normal index
            if (MakeTableItem::GetKey(items_[local_index], key_extractor_) == key) {
                items_[local_index] = MakeTableItem::Reduce(items_[local_index], kv, reduce_function_);
                return false;
            } else {
                items_[local_index] = kv;
                return true;
            }
        } else { // special handling for element with neutral index
            if (neutral_element_index_occupied_) {
                items_[local_index] = MakeTableItem::Reduce(items_[local_index], kv, reduce_function_);
                return false;
            } else {
                items_[local_index] = kv;
                neutral_element_index_occupied_ = true;
                return true;
            }
        }
    }

    void PushData(bool consume = false) {
        Flush(consume);
    }

    void Dispose() {
        std::vector<TableItem>().swap(items_);
    }

    //! \name Accessors
    //! \{

    //! Returns range reference for post phase on this worker
    common::Range& range() { return range_; }

    //! \}

private:

    void Flush(bool consume = false) {
        TableItem neutral = MakeTableItem::Make(neutral_element_, key_extractor_);
        for (size_t index = 0; index < range_.size(); index++) {
            emitter_.Emit(items_[index]);
            if (consume) {
                items_[index] = neutral;
            }
        }
        neutral_element_index_occupied_ = false;
    }

    //! Context
    Context& ctx_;

    //! Associated DIA id
    size_t dia_id_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Size of the table in bytes
    size_t limit_memory_bytes_ = 0;

    //! Store for items in range of this workers
    std::vector<TableItem> items_;

    //! The index where the neutral element would go if acutally inserted
    size_t neutral_element_key_ = 0;

    //! Is there an actual element at the index of the neutral element?
    bool neutral_element_index_occupied_ = false;

    //! Range for post phase on this worker
    common::Range range_;

    //! Stored reduce config to initialize the subtable.
    ReduceConfig config_;

    //! Emitters used to parameterize hash table for output to next DIA node.
    PhaseEmitter emitter_;

    //! neutral element to fill holes in output
    Value neutral_element_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_BY_INDEX_POST_PHASE_HEADER

/******************************************************************************/
