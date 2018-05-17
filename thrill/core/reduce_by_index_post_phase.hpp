/*******************************************************************************
 * thrill/core/reduce_by_index_post_phase.hpp
 *
 * Hash table with support for reduce.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2017 Tim Zeitz <dev.tim.zeitz@gmail.com>
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

    //! Sets the range of indexes to be handled by this index table
    void SetRange(const common::Range& range) {
        range_ = range;
        full_range_ = range;
    }

    void Initialize(size_t limit_memory_bytes) {
        assert(range_.IsValid() || range_.IsEmpty());
        limit_memory_bytes_ = limit_memory_bytes;

        TableItem neutral =
            MakeTableItem::Make(neutral_element_, key_extractor_);
        neutral_element_key_ = key(neutral);

        if (range_.size() * sizeof(TableItem) < limit_memory_bytes) {
            num_subranges_ = 0;

            // all good, we can store the whole index range
            items_.resize(range_.size(), neutral);

            LOG << "ReduceByIndexPostPhase()"
                << " limit_memory_bytes_=" << limit_memory_bytes_
                << " num_subranges_=" << 0
                << " range_=" << range_;
        }
        else {
            // we have to outsource some subranges
            num_subranges_ =
                1 + (range_.size() * sizeof(TableItem) / limit_memory_bytes);
            // we keep the first subrange in memory and only the other ones go
            // into a file
            range_ = full_range_.Partition(0, num_subranges_);

            items_.resize(range_.size(), neutral);

            LOG << "ReduceByIndexPostPhase()"
                << " limit_memory_bytes_=" << limit_memory_bytes_
                << " num_subranges_=" << num_subranges_
                << " full_range_=" << full_range_
                << " range_=" << range_
                << " range_.size()=" << range_.size();

            subranges_.reserve(num_subranges_ - 1);
            subrange_files_.reserve(num_subranges_ - 1);
            subrange_writers_.reserve(num_subranges_ - 1);

            for (size_t partition = 1; partition < num_subranges_; partition++) {
                auto file = ctx_.GetFilePtr(dia_id_);
                auto writer = file->GetWriter();
                common::Range subrange =
                    full_range_.Partition(partition, num_subranges_);

                LOG << "ReduceByIndexPostPhase()"
                    << " partition=" << partition
                    << " subrange=" << subrange;

                subranges_.emplace_back(subrange);
                subrange_files_.emplace_back(std::move(file));
                subrange_writers_.emplace_back(std::move(writer));
            }
        }
    }

    bool Insert(const TableItem& kv) {
        size_t item_key = key(kv);
        assert(item_key >= full_range_.begin && item_key < full_range_.end);

        LOG << "Insert() item_key=" << item_key
            << " full_range_=" << full_range_
            << " range_" << range_;

        if (item_key < range_.end) {
            // items is in the main range
            size_t offset = item_key - full_range_.begin;

            if (item_key != neutral_element_key_) {
                // normal index
                if (key(items_[offset]) == item_key) {
                    items_[offset] = reduce(items_[offset], kv);
                    return false;
                }
                else {
                    items_[offset] = kv;
                    return true;
                }
            }
            else {
                // special handling for element with neutral index
                if (neutral_element_index_occupied_) {
                    items_[offset] = reduce(items_[offset], kv);
                    return false;
                }
                else {
                    items_[offset] = kv;
                    neutral_element_index_occupied_ = true;
                    return true;
                }
            }
        }
        else {
            // items has to be stored in an overflow File
            size_t r = full_range_.FindPartition(item_key, num_subranges_) - 1;

            const common::Range& subrange = subranges_.at(r);
            data::File::Writer& writer = subrange_writers_.at(r);

            LOG << "Insert() item_key=" << item_key
                << " r=" << r
                << " subrange=" << subrange;

            assert(item_key >= subrange.begin && item_key < subrange.end);
            writer.Put(kv);
            return false;
        }
    }

    void PushData(bool consume = false, data::File::Writer* pwriter = nullptr) {
        assert(!pwriter || consume);

        if (cache_) {
            // previous PushData() has stored data in cache_
            data::File::Reader reader = cache_->GetReader(consume);
            while (reader.HasNext())
                emitter_.Emit(reader.Next<TableItem>());
            return;
        }

        if (!consume) {
            if (subranges_.empty()) {
                Flush();
            }
            else {
                data::FilePtr cache = ctx_.GetFilePtr(dia_id_);
                data::File::Writer writer = cache_->GetWriter();
                PushData(true, &writer);
                cache_ = cache;
                writer.Close();
            }
            return;
        }

        // close File writers
        for (auto& w : subrange_writers_) {
            w.Close();
        }

        if (pwriter) {
            FlushAndConsume<true>(pwriter);
        }
        else {
            FlushAndConsume();
        }

        for (size_t i = 0; i < subranges_.size(); ++i) {
            ReduceByIndexPostPhase<TableItem, Key, Value, KeyExtractor,
                                   ReduceFunction, Emitter, VolatileKey,
                                   ReduceConfig>
            subtable(ctx_, dia_id_, key_extractor_, reduce_function_,
                     emitter_.emit_, config_, neutral_element_);
            subtable.SetRange(subranges_[i]);
            subtable.Initialize(limit_memory_bytes_);

            {
                // insert items
                auto reader = subrange_files_[i]->GetConsumeReader();
                while (reader.HasNext()) {
                    subtable.Insert(reader.template Next<TableItem>());
                }
            }

            subtable.PushData(consume || pwriter, pwriter);

            // delete File
            subrange_files_[i].reset();
        }
    }

    void Dispose() {
        std::vector<TableItem>().swap(items_);

        std::vector<common::Range>().swap(subranges_);
        std::vector<data::FilePtr>().swap(subrange_files_);
        std::vector<data::File::Writer>().swap(subrange_writers_);
    }

private:
    void Flush() {
        for (auto it = items_.begin(); it != items_.end(); ++it) {
            emitter_.Emit(*it);
        }
    }

    template <bool DoCache = false>
    void FlushAndConsume(data::File::Writer* writer = nullptr) {
        for (auto it = items_.begin(); it != items_.end(); ++it) {
            emitter_.Emit(*it);
            if (DoCache) { writer->Put(*it); }
        }
        neutral_element_index_occupied_ = false;
        // free array
        std::vector<TableItem>().swap(items_);
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

    //! Store for items in range of this worker.
    //! Stored in reverse order so we can consume while emitting.
    std::vector<TableItem> items_;

    //! number of subranges
    size_t num_subranges_;

    //! Subranges
    std::vector<common::Range> subranges_;

    //! Subranges external Files
    std::vector<data::FilePtr> subrange_files_;

    //! Subranges external File Writers
    std::vector<data::File::Writer> subrange_writers_;

    //! File for storing data in-case we need multiple re-reduce levels.
    data::FilePtr cache_ = nullptr;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_BY_INDEX_POST_PHASE_HEADER

/******************************************************************************/
