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
     * A data structure which takes an arbitrary value and extracts an index
     * using a key extractor function from that value. Afterwards, values with
     * the same index are merged together
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

        TableItem neutral =
            MakeTableItem::Make(neutral_element_, key_extractor_);
        neutral_element_key_ = key(neutral);

        if (range_.size() * sizeof(TableItem) < limit_memory_bytes) {
            // all good, we can store the whole index range
            items_.resize(range_.size(), neutral);
        } else {
            // we have to outsource some subranges
            size_t num_subranges =
                1 + (range_.size() * sizeof(TableItem) / limit_memory_bytes);
            // we keep the first subrange in memory and only the other ones go
            // into a file
            range_ = full_range_.Partition(0, num_subranges);
            for (size_t partition = 1; partition < num_subranges; partition++) {
                auto file = ctx_.GetFile(dia_id_);
                auto writer = file.GetWriter();
                common::Range subrange =
                    full_range_.Partition(partition, num_subranges);
                subrange_files_.emplace_back(
                    subrange, std::move(file), std::move(writer));
            }
        }
    }

    bool Insert(const TableItem& kv) {
        size_t item_key = key(kv);
        assert(item_key >= full_range_.begin && item_key < full_range_.end);
        size_t offset = item_key - range_.begin;

        if (item_key >= range_.begin && item_key < range_.end) {
            // store elements in reverse order
            size_t local_index = range_.size() - offset - 1;

            if (item_key != neutral_element_key_) { // normal index
                if (key(items_[local_index]) == item_key) {
                    items_[local_index] = reduce(items_[local_index], kv);
                    return false;
                } else {
                    items_[local_index] = kv;
                    return true;
                }
            } else { // special handling for element with neutral index
                if (neutral_element_index_occupied_) {
                    items_[local_index] = reduce(items_[local_index], kv);
                    return false;
                } else {
                    items_[local_index] = kv;
                    neutral_element_index_occupied_ = true;
                    return true;
                }
            }
        } else {
            common::Range& subrange =
                std::get<0>(subrange_files_[offset / range_.size() - 1]);
            common::UNUSED(subrange); // for release build
            data::File::Writer& writer =
                std::get<2>(subrange_files_[offset / range_.size() - 1]);
            assert(item_key >= subrange.begin && item_key < subrange.end);
            writer.Put(kv);
            return false;
        }
    }

    void PushData(bool consume = false) {
        for (auto& subrange_file : subrange_files_) {
            std::get<2>(subrange_file).Close();
        }
        consume ? FlushAndConsume() : Flush();

        for (auto& subrange_file : subrange_files_) {
            ReduceByIndexPostPhase<TableItem, Key, Value, KeyExtractor,
                                   ReduceFunction, Emitter, VolatileKey,
                                   ReduceConfig_>
                subtable(ctx_, dia_id_, key_extractor_, reduce_function_,
                         emitter_.emit_, config_, neutral_element_);
            subtable.SetRange(std::get<0>(subrange_file));
            subtable.Initialize(limit_memory_bytes_);
            auto reader = std::get<1>(subrange_file).GetReader(consume);
            while (reader.HasNext()) {
                subtable.Insert(reader.template Next<TableItem>());
            }
            subtable.PushData(consume);
        }
    }

    void Dispose() {
        std::vector<TableItem>().swap(items_);
        std::vector<std::tuple<common::Range, data::File, data::File::Writer>>()
            .swap(subrange_files_);
    }

    //! \name Accessors
    //! \{

    //! Sets the range of indexes to be handled by this index table
    void SetRange(const common::Range& range) {
        range_ = range;
        full_range_ = range;
    }

    //! \}

private:

    void Flush() {
        for (auto iterator = items_.rbegin(); iterator != items_.rend(); iterator++) {
            emitter_.Emit(*iterator);
        }
    }

    void FlushAndConsume() {
        while (!items_.empty()) {
            emitter_.Emit(items_.back());
            items_.pop_back();
        }
        neutral_element_index_occupied_ = false;
    }

    Key key(const TableItem& t) {
        return MakeTableItem::GetKey(t, key_extractor_);
    }

    TableItem reduce(const TableItem& a, const TableItem& b) {
        return MakeTableItem::Reduce(a, b, reduce_function_);
    }

    //! Context
    Context& ctx_;

    //! Associated DIA id
    size_t dia_id_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Stored reduce config to initialize the subtable.
    ReduceConfig config_;

    //! Emitters used to parameterize hash table for output to next DIA node.
    PhaseEmitter emitter_;

    //! neutral element to fill holes in output
    Value neutral_element_;

    //! Size of the table in bytes
    size_t limit_memory_bytes_ = 0;

    //! The index where the neutral element would go if acutally inserted
    size_t neutral_element_key_ = 0;

    //! Is there an actual element at the index of the neutral element?
    bool neutral_element_index_occupied_ = false;

    //! Range of indexes actually managed in this instance -
    //! not including subranges
    common::Range range_;

    //! Full range of indexes actually managed in this instance -
    //! including subranges
    common::Range full_range_;

    //! Store for items in range of this workers.
    //! Stored in reverse order so we can consume while emitting.
    std::vector<TableItem> items_;

    //! Store for items in nonactive subranges
    std::vector<std::tuple<common::Range, data::File, data::File::Writer>> subrange_files_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_BY_INDEX_POST_PHASE_HEADER

/******************************************************************************/
