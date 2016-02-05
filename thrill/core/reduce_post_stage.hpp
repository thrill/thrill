/*******************************************************************************
 * thrill/core/reduce_post_stage.hpp
 *
 * Hash table with support for reduce.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_POST_STAGE_HEADER
#define THRILL_CORE_REDUCE_POST_STAGE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_functional.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cassert>
#include <climits>
#include <cmath>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

//! template specialization switch class to output key+value if SendPair and
//! only value if not SendPair.
template <typename KeyValuePair, typename ValueType, bool SendPair>
class ReducePostStageEmitterSwitch;

template <typename KeyValuePair, typename ValueType>
class ReducePostStageEmitterSwitch<KeyValuePair, ValueType, false>
{
public:
    static void Put(const KeyValuePair& p, std::function<void(const ValueType&)>& emit) {
        emit(p.second);
    }
};

template <typename KeyValuePair, typename ValueType>
class ReducePostStageEmitterSwitch<KeyValuePair, ValueType, true>
{
public:
    static void Put(const KeyValuePair& p, std::function<void(const ValueType&)>& emit) {
        emit(p);
    }
};

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the post-stage
//! are passed to the next DIA node for processing.
template <typename KeyValuePair, typename ValueType, bool SendPair>
class ReducePostStageEmitter
{
    static const bool debug = true;

public:
    using EmitterFunction = std::function<void(const ValueType&)>;

    explicit ReducePostStageEmitter(const EmitterFunction& emit)
        : emit_(emit) { }

    //! output an element into a partition, template specialized for SendPair
    //! and non-SendPair types
    void Emit(const size_t& /* partition_id */, const KeyValuePair& p) {
        ReducePostStageEmitterSwitch<KeyValuePair, ValueType, SendPair>::Put(p, emit_);
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
class ReducePostStage
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const ValueType&)>;

    using Emitter = ReducePostStageEmitter<KeyValuePair, ValueType, SendPair>;

    using Table = HashTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, Emitter,
              !SendPair,
              IndexFunction, EqualToFunction>;

    /**
     * A data structure which takes an arbitrary value and extracts a key using a key extractor
     * function from that value. Afterwards, the value is hashed based on the key into some slot.
     *
     * \param context Context.
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param index_function Function to be used for computing the bucket the item to be inserted.
     * \param begin_local_index Begin index for reduce to index.
     * \param end_local_index End index for reduce to index.
     * \param neutral element Neutral element for reduce to index.
     * \param limit_memory_bytes Maximal size of the table in byte. In case size of table exceeds that value, items
     *                  are flushed.
     * \param bucket_rate Ratio of number of blocks to number of buckets in the table.
     * \param limit_partition_fill_rate Maximal number of items relative to maximal number of items in a partition.
     *        It the number is exceeded, no more blocks are added to a bucket, instead, items get spilled to disk.
     * \param partition_rate Rate of number of buckets to number of partitions. There is one file writer per partition.
     * \param equal_to_function Function for checking equality of two keys.
     */
    ReducePostStage(Context& ctx,
                    const KeyExtractor& key_extractor,
                    const ReduceFunction& reduce_function,
                    const EmitterFunction& emit,
                    const IndexFunction& index_function,
                    const Key& sentinel = Key(),
                    size_t limit_memory_bytes = 1024* 1024,
                    double limit_partition_fill_rate = 0.6,
                    double bucket_rate = 1.0,
                    double partition_rate = 0.1,
                    const EqualToFunction& equal_to_function = EqualToFunction())
        : emit_(emit),
          table_(ctx,
                 key_extractor, reduce_function, emit_,
                 std::max<size_t>((size_t)(1.0 / partition_rate), 1),
                 limit_memory_bytes,
                 limit_partition_fill_rate, bucket_rate, false,
                 sentinel,
                 index_function, equal_to_function) {

        assert(partition_rate > 0.0 && partition_rate <= 1.0 &&
               "a partition rate of 1.0 causes exactly one partition.");
    }

    ReducePostStage(Context& ctx, KeyExtractor key_extractor,
                    ReduceFunction reduce_function, EmitterFunction emit)
        : ReducePostStage(ctx, key_extractor, reduce_function, emit,
                          IndexFunction()) { }

    //! non-copyable: delete copy-constructor
    ReducePostStage(const ReducePostStage&) = delete;
    //! non-copyable: delete assignment operator
    ReducePostStage& operator = (const ReducePostStage&) = delete;

    void Insert(const Value& p) {
        return table_.Insert(p);
    }

    void Insert(const KeyValuePair& kv) {
        return table_.Insert(kv);
    }

    //! Flushes all items in the whole table.
    void Flush(bool consume = false) {
        LOG << "Flushing items";

        std::vector<data::File>& partition_files = table_.partition_files();

        for (size_t id = 0; id < partition_files.size(); ++id) {

            // get the actual reader from the file
            data::File& file = partition_files[id];

            // only if items have been spilled, process a second reduce
            if (file.num_items() > 0) {

                data::File::Reader reader = file.GetReader(consume);

                // this does not work -tb
                abort();
                // Reduce<Table, BucketBlock>(ctx, consume, ht, items, offset,
                //                            length, reader, second_reduce,
                //                            fill_rate_num_items_per_partition,
                //                            partition_id, num_items_mem_per_partition, block_pool,
                //                            max_num_blocks_second_reduce, block_size);

                // no spilled items, just flush already reduced
                // data in primary table in current partition
            }
            else {
                /////
                // emit data
                /////
                table_.FlushPartition(id, consume);
            }
        }

        LOG << "Flushed items";
    }

    //! Emits element to all children
    void EmitAll(const size_t& partition_id, const KeyValuePair& p) {
        emit_.Emit(partition_id, p);
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
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename IndexFunction = PostReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
using ReducePostBucketStage = ReducePostStage<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          SendPair,
          IndexFunction, EqualToFunction,
          ReduceBucketHashTable>;

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename IndexFunction = PostReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
using ReducePostProbingStage = ReducePostStage<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          SendPair,
          IndexFunction, EqualToFunction,
          ReduceProbingHashTable>;

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_POST_STAGE_HEADER

/******************************************************************************/
