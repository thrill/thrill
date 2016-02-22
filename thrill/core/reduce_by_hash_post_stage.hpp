/*******************************************************************************
 * thrill/core/reduce_by_hash_post_stage.hpp
 *
 * Hash table with support for reduce.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_BY_HASH_POST_STAGE_HEADER
#define THRILL_CORE_REDUCE_BY_HASH_POST_STAGE_HEADER

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
template <
    typename KeyValuePair, typename ValueType, typename Emitter, bool SendPair>
class ReduceByHashPostStageEmitter
{
public:
    explicit ReduceByHashPostStageEmitter(const Emitter& emit)
        : emit_(emit) { }

    //! output an element into a partition, template specialized for SendPair
    //! and non-SendPair types
    void Emit(const KeyValuePair& p) {
        ReducePostStageEmitterSwitch<
            KeyValuePair, ValueType, Emitter, SendPair>::Put(p, emit_);
    }

    //! output an element into a partition, template specialized for SendPair
    //! and non-SendPair types
    void Emit(const size_t& /* partition_id */, const KeyValuePair& p) {
        Emit(p);
    }

public:
    //! Set of emitters, one per partition.
    Emitter emit_;
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool SendPair,
          typename IndexFunction,
          typename ReduceStageConfig,
          typename EqualToFunction,
          template <typename _ValueType, typename _Key, typename _Value,
                    typename _KeyExtractor, typename _ReduceFunction, typename _Emitter,
                    const bool _RobustKey,
                    typename _IndexFunction, typename _ReduceStageConfig,
                    typename _EqualToFunction> class HashTable>
class ReduceByHashPostStage
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using StageEmitter = ReduceByHashPostStageEmitter<
              KeyValuePair, ValueType, Emitter, SendPair>;

    using Table = HashTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, StageEmitter,
              !SendPair, IndexFunction, ReduceStageConfig, EqualToFunction>;

    /**
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     */
    ReduceByHashPostStage(
        Context& ctx,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const Emitter& emit,
        const IndexFunction& index_function = IndexFunction(),
        const ReduceStageConfig& config = ReduceStageConfig(),
        const EqualToFunction& equal_to_function = EqualToFunction())
        : config_(config),
          emitter_(emit),
          table_(ctx,
                 key_extractor, reduce_function, emitter_,
                 /* num_partitions */ 32, /* TODO(tb): parameterize */
                 config, false,
                 index_function, equal_to_function) { }

    //! non-copyable: delete copy-constructor
    ReduceByHashPostStage(const ReduceByHashPostStage&) = delete;
    //! non-copyable: delete assignment operator
    ReduceByHashPostStage& operator = (const ReduceByHashPostStage&) = delete;

    void Initialize() {
        table_.Initialize();
    }

    void Insert(const Value& p) {
        return table_.Insert(p);
    }

    void Insert(const KeyValuePair& kv) {
        return table_.Insert(kv);
    }

    //! Flushes all items in the whole table.
    template <bool DoCache>
    void Flush(bool consume, data::File::Writer* writer = nullptr) {
        LOG << "Flushing items";

        // list of remaining files, containing only partially reduced item pairs
        // or items
        std::vector<data::File> remaining_files;

        // read primary hash table, since ReduceByHash delivers items in any
        // order, we can just emit items from fully reduced partitions.

        {
            std::vector<data::File>& files = table_.partition_files();

            for (size_t id = 0; id < files.size(); ++id)
            {
                // get the actual reader from the file
                data::File& file = files[id];

                // if items have been spilled, store for a second reduce
                if (file.num_items() > 0) {
                    table_.SpillPartition(id);

                    LOG << "partition " << id << " contains "
                        << file.num_items() << " partially reduced items";

                    remaining_files.emplace_back(std::move(file));
                }
                else {
                    LOG << "partition " << id << " contains "
                        << table_.items_per_partition(id)
                        << " fully reduced items";

                    table_.FlushPartitionEmit(
                        id, consume,
                        [this, writer](
                            const size_t& partition_id, const KeyValuePair& p) {
                            if (DoCache) writer->Put(p);
                            emitter_.Emit(partition_id, p);
                        });
                }
            }
        }

        if (remaining_files.size() == 0) {
            LOG << "Flushed items directly.";
            return;
        }

        table_.Dispose();

        assert(consume && "Items were spilled hence Flushing must consume");

        // if partially reduce files remain, create new hash tables to process
        // them iteratively.

        size_t iteration = 1;

        while (remaining_files.size())
        {
            sLOG1 << "ReducePostStage: re-reducing items from"
                  << remaining_files.size() << "spilled files"
                  << "iteration" << iteration;
            sLOG1 << "-- Try to increase the amount of RAM to avoid this.";

            std::vector<data::File> next_remaining_files;

            Table subtable(
                table_.ctx(),
                table_.key_extractor(), table_.reduce_function(), emitter_,
                /* num_partitions */ 32, config_, false,
                IndexFunction(iteration, table_.index_function()),
                table_.equal_to_function());

            subtable.Initialize();

            size_t num_subfile = 0;

            for (data::File& file : remaining_files)
            {
                // insert all items from the partially reduced file
                sLOG << "re-reducing subfile" << num_subfile++
                     << "containing" << file.num_items() << "items";

                data::File::ConsumeReader reader = file.GetConsumeReader();

                while (reader.HasNext()) {
                    subtable.Insert(reader.Next<KeyValuePair>());
                }

                // after insertion, flush fully reduced partitions and save
                // remaining files for next iteration.

                std::vector<data::File>& subfiles = subtable.partition_files();

                for (size_t id = 0; id < subfiles.size(); ++id)
                {
                    // get the actual reader from the file
                    data::File& subfile = subfiles[id];

                    // if items have been spilled, store for a second reduce
                    if (subfile.num_items() > 0) {
                        subtable.SpillPartition(id);

                        sLOG << "partition" << id << "contains"
                             << subfile.num_items() << "partially reduced items";

                        next_remaining_files.emplace_back(std::move(subfile));
                    }
                    else {
                        sLOG << "partition" << id << "contains"
                             << subtable.items_per_partition(id)
                             << "fully reduced items";

                        subtable.FlushPartitionEmit(
                            id, /* consume */ true,
                            [this, writer](
                                const size_t& partition_id, const KeyValuePair& p) {
                                if (DoCache) writer->Put(p);
                                emitter_.Emit(partition_id, p);
                            });
                    }
                }
            }

            remaining_files = std::move(next_remaining_files);
            ++iteration;
        }

        LOG << "Flushed items";
    }

    //! Push data into emitter
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
                cache_ = table_.ctx().GetFilePtr();
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

    //! \name Accessors
    //! {

    //! Returns the total num of items in the table.
    size_t num_items() const { return table_.num_items(); }

    //! }

private:
    //! Stored reduce config to initialize the subtable.
    ReduceStageConfig config_;

    //! Emitters used to parameterize hash table for output to next DIA node.
    StageEmitter emitter_;

    //! the first-level hash table implementation
    Table table_;

    //! File for storing data in-case we need multiple re-reduce levels.
    data::FilePtr cache_;
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool SendPair = false,
          typename IndexFunction = ReduceByHash<Key>,
          typename ReduceStageConfig = DefaultReduceTableConfig,
          typename EqualToFunction = std::equal_to<Key> >
using ReducePostBucketStage = ReduceByHashPostStage<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction, Emitter,
          SendPair, IndexFunction, ReduceStageConfig, EqualToFunction,
          ReduceBucketHashTable>;

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool SendPair = false,
          typename IndexFunction = ReduceByHash<Key>,
          typename ReduceStageConfig = DefaultReduceTableConfig,
          typename EqualToFunction = std::equal_to<Key> >
using ReducePostProbingStage = ReduceByHashPostStage<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction, Emitter,
          SendPair, IndexFunction, ReduceStageConfig, EqualToFunction,
          ReduceProbingHashTable>;

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_BY_HASH_POST_STAGE_HEADER

/******************************************************************************/
