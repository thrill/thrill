/*******************************************************************************
 * thrill/core/reduce_by_index_post_stage.hpp
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
#ifndef THRILL_CORE_REDUCE_BY_INDEX_POST_STAGE_HEADER
#define THRILL_CORE_REDUCE_BY_INDEX_POST_STAGE_HEADER

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

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the post-stage
//! are passed to the next DIA node for processing.
template <typename KeyValuePair, typename ValueType,
          typename Emitter, bool SendPair>
class ReduceByIndexPostStageEmitter
{
public:
    explicit ReduceByIndexPostStageEmitter(const Emitter& emit)
        : emit_(emit) { }

    //! output an element into a partition, template specialized for SendPair
    //! and non-SendPair types
    void Emit(const KeyValuePair& p) {
        ReducePostStageEmitterSwitch<
            KeyValuePair, ValueType, Emitter, SendPair>::Put(p, emit_);
    }

    void Emit(size_t /* partition_id */, const KeyValuePair& p) {
        Emit(p);
    }

public:
    //! Set of emitters, one per partition.
    Emitter emit_;
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool SendPair = false,
          typename ReduceConfig_ = DefaultReduceConfig,
          typename IndexFunction = ReduceByIndex<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class ReduceByIndexPostStage
{
    static constexpr bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;
    using ReduceConfig = ReduceConfig_;

    using StageEmitter = ReduceByIndexPostStageEmitter<
              KeyValuePair, ValueType, Emitter, SendPair>;

    using Table = typename ReduceTableSelect<
              ReduceConfig::table_impl_,
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, StageEmitter,
              !SendPair, ReduceConfig, IndexFunction, EqualToFunction>::type;

    /*!
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     */
    ReduceByIndexPostStage(
        Context& ctx,
        size_t dia_id,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const Emitter& emitter,
        const ReduceConfig& config = ReduceConfig(),
        const IndexFunction& index_function = IndexFunction(),
        const Value& neutral_element = Value(),
        const EqualToFunction& equal_to_function = EqualToFunction())
        : config_(config),
          emitter_(emitter),
          table_(ctx, dia_id,
                 key_extractor, reduce_function, emitter_,
                 /* num_partitions */ 32, /* TODO(tb): parameterize */
                 config, false,
                 index_function, equal_to_function),
          neutral_element_(neutral_element) { }

    //! non-copyable: delete copy-constructor
    ReduceByIndexPostStage(const ReduceByIndexPostStage&) = delete;
    //! non-copyable: delete assignment operator
    ReduceByIndexPostStage& operator = (const ReduceByIndexPostStage&) = delete;

    void Initialize(size_t limit_memory_bytes) {
        table_.Initialize(limit_memory_bytes);
    }

    void Insert(const Value& p) {
        return table_.Insert(p);
    }

    void Insert(const KeyValuePair& kv) {
        return table_.Insert(kv);
    }

    using RangeFilePair = std::pair<common::Range, data::File>;

    //! Flush contents of table into emitter and return remaining files
    template <bool DoCache>
    void FlushTableInto(
        Table& table, std::vector<RangeFilePair>& remaining_files,
        bool consume, data::File::Writer* writer = nullptr) {

        std::vector<data::File>& files = table.partition_files();

        size_t id = 0;
        for ( ; id < files.size(); ++id)
        {
            // get the actual reader from the file
            data::File& file = files[id];

            if (file.num_items() > 0) {
                // if items have been spilled, switch to second loop, which
                // stores items for a second reduce
                break;
            }
            else {
                // calculate key range for the file
                common::Range file_range = table.key_range(id);

                sLOG << "partition" << id << "range" << file_range
                     << "contains" << table.items_per_partition(id)
                     << "fully reduced items";

                size_t index = file_range.begin;

                table.FlushPartitionEmit(
                    id, consume, /* grow */ false,
                    [this, &table, &file_range, &index, writer](
                        const size_t& /* partition_id */, const KeyValuePair& p) {
                        for ( ; index < p.first; ++index) {
                            KeyValuePair kv = std::make_pair(index, neutral_element_);
                            emitter_.Emit(kv);
                            if (DoCache) writer->Put(kv);

                                // sLOG << "emit hole" << index << "-" << neutral_element_;
                        }
                        emitter_.Emit(p);
                        if (DoCache) writer->Put(p);

                        // sLOG << "emit" << p.first << "-" << p.second;
                        ++index;
                    });

                for ( ; index < file_range.end; ++index) {
                    KeyValuePair kv = std::make_pair(index, neutral_element_);
                    emitter_.Emit(kv);
                    if (DoCache) writer->Put(kv);
                }
            }
        }

        for ( ; id < files.size(); ++id)
        {
            // get the actual reader from the file
            data::File& file = files[id];

            // calculate key range for the file
            common::Range file_range = table.key_range(id);

            if (file.num_items() > 0) {
                // if items have been spilled, store for a second reduce
                table.SpillPartition(id);

                sLOG << "partition" << id << "range" << file_range
                     << "contains" << file.num_items()
                     << "partially reduced items";

                remaining_files.emplace_back(
                    RangeFilePair(file_range, std::move(file)));
            }
            else {
                // no items have been spilled, but we cannot keep them in
                // memory due to a second reduce, which is necessary.
                sLOG << "partition" << id << "range" << file_range
                     << "contains" << table.items_per_partition(id)
                     << "fully reduced items";

                table.SpillPartition(id);

                assert(file_range.IsValid());
                // swap file range to signal fully reduced items
                file_range.Swap();

                remaining_files.emplace_back(
                    RangeFilePair(file_range, std::move(file)));
            }
        }
    }

    //! Flushes all items in the whole table. Since we have to flush recursively
    //! such that the order of all indexes remains correct, we use an imaginary
    //! deque of remaining files. In each iteration, the first remaining file is
    //! further reduced and replaced by more files if necessary. Since the deque
    //! is only extended in the front, we use a vector in reverse order.
    template <bool DoCache>
    void Flush(bool consume, data::File::Writer* writer = nullptr) {
        LOG << "Flushing items";

        // list of remaining files, containing only partially reduced item pairs
        // or items. in reverse order.
        std::vector<RangeFilePair> remaining_files;

        // read primary hash table, since ReduceByHash delivers items in any
        // order, we can just emit items from fully reduced partitions.
        FlushTableInto<DoCache>(table_, remaining_files, consume, writer);

        if (remaining_files.size() == 0) {
            LOG << "Flushed items directly.";
            return;
        }

        table_.Dispose();

        assert(consume && "Items were spilled hence Flushing must consume");

        // reverse order in remaining files
        std::reverse(remaining_files.begin(), remaining_files.end());

        // if partially reduce files remain, create new hash tables to process
        // them iteratively.

        Table subtable(
            table_.ctx(), table_.dia_id(),
            table_.key_extractor(), table_.reduce_function(), emitter_,
            /* num_partitions */ 32, config_, false,
            table_.index_function(),
            table_.equal_to_function());

        subtable.Initialize(table_.limit_memory_bytes());

        size_t iteration = 1;

        sLOG << "ReduceToIndexPostStage: re-reducing items from"
             << remaining_files.size() << "spilled files";

        while (remaining_files.size())
        {
            sLOG << "ReduceToIndexPostStage: re-reducing items from"
                 << remaining_files.size() << "remaining files"
                 << "iteration" << iteration;

            std::vector<RangeFilePair> next_remaining_files;

            size_t num_subfile = 0;

            // take last remaining file and reduce or output it.
            RangeFilePair pair = std::move(remaining_files.back());
            remaining_files.pop_back();

            common::Range range = pair.first;
            data::File file = std::move(pair.second);

            assert(!range.IsEmpty());

            if (!range.IsValid())
            {
                range.Swap();

                // directly emit all items from the fully reduced file
                sLOG << "emitting subfile" << num_subfile++
                     << "range" << range;

                data::File::ConsumeReader reader = file.GetConsumeReader();

                size_t index = range.begin;

                while (reader.HasNext()) {
                    KeyValuePair p = reader.Next<KeyValuePair>();

                    for ( ; index < p.first; ++index) {
                        KeyValuePair kv = std::make_pair(index, neutral_element_);
                        emitter_.Emit(kv);
                        if (DoCache) writer->Put(kv);

                        // sLOG << "emit hole" << index << "-" << neutral_element_;
                    }

                    // sLOG << "emit" << p.first << "-" << p.second;
                    emitter_.Emit(p);
                    if (DoCache) writer->Put(p);
                    ++index;
                }

                for ( ; index < range.end; ++index) {
                    KeyValuePair kv = std::make_pair(index, neutral_element_);
                    emitter_.Emit(kv);
                    if (DoCache) writer->Put(kv);
                }
            }
            else
            {
                // change table's range
                subtable.index_function().set_range(range);

                // insert all items from the partially reduced file
                sLOG << "re-reducing subfile" << num_subfile++
                     << "range" << range;

                data::File::ConsumeReader reader = file.GetConsumeReader();

                while (reader.HasNext()) {
                    subtable.Insert(reader.Next<KeyValuePair>());
                }

                // after insertion, flush fully reduced partitions and save
                // remaining files for next iteration.

                FlushTableInto<DoCache>(
                    subtable, next_remaining_files, /* consume */ true, writer);

                for (auto it = next_remaining_files.rbegin();
                     it != next_remaining_files.rend(); ++it)
                {
                    remaining_files.emplace_back(std::move(*it));
                }

                ++iteration;
            }
        }

        LOG << "Flushed items";
    }

    void PushData(bool consume = false) {
        if (!cache_)
        {
            if (!table_.has_spilled_data()) {
                // no items were spilled to disk, hence we can emit all data
                // from RAM.
                Flush</* DoCache */ false>(consume);
            }
            else {
                // items were spilled, hence the reduce table must be emptied
                // and we have to cache the output stream.
                cache_ = table_.ctx().GetFilePtr(table_.dia_id());
                data::File::Writer writer = cache_->GetWriter();
                Flush</* DoCache */ true>(true, &writer);
            }
        }
        else
        {
            // previous PushData() has stored data in cache_
            data::File::Reader reader = cache_->GetReader(consume);
            while (reader.HasNext())
                emitter_.Emit(reader.Next<KeyValuePair>());
        }
    }

    void Dispose() {
        table_.Dispose();
        if (cache_) cache_.reset();
    }

    //! \name Accessors
    //! \{

    //! Returns mutable reference to first table_
    Table& table() { return table_; }

    //! Returns the total num of items in the table.
    size_t num_items() const { return table_.num_items(); }

    //! \}

private:
    //! Stored reduce config to initialize the subtable.
    ReduceConfig config_;

    //! Emitters used to parameterize hash table for output to next DIA node.
    StageEmitter emitter_;

    //! the first-level hash table implementation
    Table table_;

    //! neutral element to fill holes in output
    Value neutral_element_;

    //! File for storing data in-case we need multiple re-reduce levels.
    data::FilePtr cache_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_BY_INDEX_POST_STAGE_HEADER

/******************************************************************************/
