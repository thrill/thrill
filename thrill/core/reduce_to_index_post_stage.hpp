/*******************************************************************************
 * thrill/core/reduce_to_index_post_stage.hpp
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
#ifndef THRILL_CORE_REDUCE_TO_INDEX_POST_STAGE_HEADER
#define THRILL_CORE_REDUCE_TO_INDEX_POST_STAGE_HEADER

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

//! template specialization switch class to output key+value if SendPair and
//! only value if not SendPair.
template <typename KeyValuePair, typename ValueType, bool SendPair>
class ReduceToIndexPostStageEmitterSwitch;

template <typename KeyValuePair, typename ValueType>
class ReduceToIndexPostStageEmitterSwitch<KeyValuePair, ValueType, false>
{
public:
    static void Put(const KeyValuePair& p,
                    std::function<void(const ValueType&)>& emit) {
        emit(p.second);
    }
};

template <typename KeyValuePair, typename ValueType>
class ReduceToIndexPostStageEmitterSwitch<KeyValuePair, ValueType, true>
{
public:
    static void Put(const KeyValuePair& p,
                    std::function<void(const ValueType&)>& emit) {
        emit(p);
    }
};

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the post-stage
//! are passed to the next DIA node for processing.
template <typename KeyValuePair, typename ValueType, bool SendPair>
class ReduceToIndexPostStageEmitter
{
public:
    using EmitterFunction = std::function<void(const ValueType&)>;

    explicit ReduceToIndexPostStageEmitter(const EmitterFunction& emit)
        : emit_(emit) { }

    //! output an element into a partition, template specialized for SendPair
    //! and non-SendPair types
    void Emit(const KeyValuePair& p) {
        ReduceToIndexPostStageEmitterSwitch<
            KeyValuePair, ValueType, SendPair>::Put(p, emit_);
    }

    void Emit(size_t /* partition_id */, const KeyValuePair& p) {
        Emit(p);
    }

public:
    //! Set of emitters, one per partition.
    EmitterFunction emit_;
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair,
          typename IndexFunction,
          typename EqualToFunction,
          template <typename _ValueType, typename _Key, typename _Value,
                    typename _KeyExtractor, typename _ReduceFunction, typename _Emitter,
                    const bool _RobustKey,
                    typename _IndexFunction,
                    typename _EqualToFunction> class HashTable>
class ReduceToIndexPostStage
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const ValueType&)>;

    using Emitter = ReduceToIndexPostStageEmitter<KeyValuePair, ValueType, SendPair>;

    using Table = HashTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, Emitter,
              !SendPair,
              IndexFunction, EqualToFunction>;

    /**
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     *
     * \param context Context.
     *
     * \param key_extractor Key extractor function to extract a key from a
     * value.
     *
     * \param reduce_function Reduce function to reduce to values.
     *
     * \param emit A set of BlockWriter to flush items. One BlockWriter per
     * partition.
     *
     * \param index_function Function to be used for computing the bucket the
     * item to be inserted.
     *
     * \param begin_local_index Begin index for reduce to index.
     *
     * \param end_local_index End index for reduce to index.
     *
     * \param limit_memory_bytes Maximal size of the table in byte. In case size
     * of table exceeds that value, items are flushed.
     *
     * \param bucket_rate Ratio of number of blocks to number of buckets in the
     * table.
     *
     * \param limit_partition_fill_rate Maximal number of items relative to
     * maximal number of items in a partition. It the number is exceeded, no
     * more blocks are added to a bucket, instead, items get spilled to disk.
     *
     * \param partition_rate Rate of number of buckets to number of
     * partitions. There is one file writer per partition.
     *
     * \param equal_to_function Function for checking equality of two keys.
     */
    ReduceToIndexPostStage(
        Context& ctx,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const EmitterFunction& emit,
        const IndexFunction& index_function,
        const Key& sentinel = Key(),
        size_t limit_memory_bytes = 1024* 1024,
        double limit_partition_fill_rate = 0.6,
        double bucket_rate = 1.0,
        const EqualToFunction& equal_to_function = EqualToFunction())
        : emit_(emit),
          table_(ctx,
                 key_extractor, reduce_function, emit_,
                 /* num_partitions */ 32, /* TODO(tb): parameterize */
                 limit_memory_bytes,
                 limit_partition_fill_rate, bucket_rate, false,
                 sentinel,
                 index_function, equal_to_function) { }

    ReduceToIndexPostStage(Context& ctx, KeyExtractor key_extractor,
                           ReduceFunction reduce_function, EmitterFunction emit)
        : ReduceToIndexPostStage(ctx, key_extractor, reduce_function, emit,
                                 IndexFunction()) { }

    //! non-copyable: delete copy-constructor
    ReduceToIndexPostStage(const ReduceToIndexPostStage&) = delete;
    //! non-copyable: delete assignment operator
    ReduceToIndexPostStage& operator = (const ReduceToIndexPostStage&) = delete;

    void Insert(const Value& p) {
        return table_.Insert(p);
    }

    void Insert(const KeyValuePair& kv) {
        return table_.Insert(kv);
    }

    using RangeFilePair = std::pair<common::Range, data::File>;

    void FlushTableInto(
        Table& table, std::vector<RangeFilePair>& remaining_files) {
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
                common::Range file_range = table.index_function().inverse_range(
                    id, table.num_buckets_per_partition(), table.num_buckets());

                sLOG << "partition" << id << "range" << file_range
                     << "contains" << table.items_per_partition(id)
                     << "fully reduced items";

                size_t index = file_range.begin;

                table.FlushPartitionEmit(
                    id, /* consume */ true,
                    [this, &table, &file_range, &index](
                        const size_t& /* partition_id */, const KeyValuePair& p) {
                        for ( ; index < p.first; ++index) {
                            emit_.Emit(std::make_pair(index, neutral_element_));
                            sLOG << "emit hole" << index << "-" << neutral_element_;
                        }
                        emit_.Emit(p);
                        sLOG << "emit" << p.first << "-" << p.second;
                        ++index;
                    });

                for ( ; index < file_range.end; ++index)
                    emit_.Emit(std::make_pair(index, neutral_element_));
            }
        }

        for ( ; id < files.size(); ++id)
        {
            // get the actual reader from the file
            data::File& file = files[id];

            // calculate key range for the file
            common::Range file_range = table.index_function().inverse_range(
                id, table.num_buckets_per_partition(), table.num_buckets());

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
    void Flush(bool /* consume */ = false) {
        LOG << "Flushing items";

        // list of remaining files, containing only partially reduced item pairs
        // or items. in reverse order.
        std::vector<RangeFilePair> remaining_files;

        // read primary hash table, since ReduceByHash delivers items in any
        // order, we can just emit items from fully reduced partitions.
        FlushTableInto(table_, remaining_files);

        if (remaining_files.size() == 0) {
            LOG << "Flushed items directly.";
            return;
        }

        // reverse order in remaining files
        std::reverse(remaining_files.begin(), remaining_files.end());

        // if partially reduce files remain, create new hash tables to process
        // them iteratively.

        Table subtable(
            table_.ctx(),
            table_.key_extractor(), table_.reduce_function(), emit_,
            /* num_partitions */ 32,
            table_.limit_memory_bytes(),
            0.7 /* TODO(tb): parameterize */, 1.0 /* TODO(tb): parameterize */, false,
            table_.sentinel().first /* TODO(tb): weird */,
            table_.index_function(),
            table_.equal_to_function());

        size_t iteration = 1;

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
                        emit_.Emit(std::make_pair(index, neutral_element_));
                        sLOG << "emit hole" << index << "-" << neutral_element_;
                    }

                    sLOG << "emit" << p.first << "-" << p.second;
                    emit_.Emit(p);
                    ++index;
                }

                for ( ; index < range.end; ++index)
                    emit_.Emit(std::make_pair(index, neutral_element_));
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

                FlushTableInto(subtable, next_remaining_files);

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

    //! \name Accessors
    //! {

    //! Returns the total num of items in the table.
    size_t num_items() const { return table_.num_items(); }

    //! }

private:
    //! Emitters used to parameterize hash table for output to next DIA node.
    Emitter emit_;

    //! the first-level hash table implementation
    Table table_;

    //! neutral element to fill holes in output
    Value neutral_element_ = Value();
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename IndexFunction = PostReduceByIndex<Key>,
          typename EqualToFunction = std::equal_to<Key> >
using ReduceToIndexPostBucketStage = ReduceToIndexPostStage<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          SendPair,
          IndexFunction, EqualToFunction,
          ReduceBucketHashTable>;

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename IndexFunction = PostReduceByIndex<Key>,
          typename EqualToFunction = std::equal_to<Key> >
using ReduceToIndexPostProbingStage = ReduceToIndexPostStage<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          SendPair,
          IndexFunction, EqualToFunction,
          ReduceProbingHashTable>;

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_TO_INDEX_POST_STAGE_HEADER

/******************************************************************************/
