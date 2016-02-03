/*******************************************************************************
 * thrill/core/reduce_post_probing_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_POST_PROBING_TABLE_HEADER
#define THRILL_CORE_REDUCE_POST_PROBING_TABLE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/post_reduce_flush.hpp>
#include <thrill/core/post_reduce_flush_to_index.hpp>
#include <thrill/core/reduce_probing_table.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cassert>
#include <climits>
#include <cmath>
#include <functional>
#include <limits>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

template <typename Key, typename HashFunction = std::hash<Key> >
class PostReduceByHashKey
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    explicit PostReduceByHashKey(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function) { }

    IndexResult operator () (const Key& k,
                             const size_t& num_partitions,
                             const size_t& num_buckets_per_partition,
                             const size_t& num_buckets_per_table,
                             const size_t& offset) const {

        (void)num_partitions;
        (void)offset;

        size_t global_index = hash_function_(k) % num_buckets_per_table;

        return IndexResult { global_index / num_buckets_per_partition, global_index };
    }

private:
    HashFunction hash_function_;
};

template <typename Key>
class PostReduceByIndex
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    IndexResult operator () (const Key& k,
                             const size_t& num_partitions,
                             const size_t& num_buckets_per_partition,
                             const size_t& num_buckets_per_table,
                             const size_t& offset) const {

        (void)num_buckets_per_partition;

        size_t result = (k - offset) % num_buckets_per_table;

        return IndexResult { result / num_partitions, result };
    }
};

template <bool, typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostProbingEmitImpl;

template <typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostProbingEmitImpl<true, EmitterFunction, KeyValuePair, SendType>{
    void EmitElement(const KeyValuePair& p, EmitterFunction emit) {
        emit(p);
    }
};

template <typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostProbingEmitImpl<false, EmitterFunction, KeyValuePair, SendType>{
    void EmitElement(const KeyValuePair& p, EmitterFunction emit) {
        emit(p.second);
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename FlushFunction = PostReduceFlush<Key, Value, ReduceFunction>,
          typename IndexFunction = PostReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class ReducePostProbingTable
    : public ReduceProbingTable<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          !SendPair,
          IndexFunction, EqualToFunction,
          ReducePostProbingTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, SendPair, FlushFunction,
              IndexFunction, EqualToFunction>
          >
{
    static const bool debug = true;

    using Super = ReduceProbingTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction,
              !SendPair,
              IndexFunction, EqualToFunction, ReducePostProbingTable>;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const ValueType&)>;

    PostProbingEmitImpl<
        SendPair, EmitterFunction, KeyValuePair, ValueType> emit_impl_;

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
     * \param sentinel Sentinel element used to flag free slots.
     *
     * \param begin_local_index Begin index for reduce to index.
     *
     * \param index_function Function to be used for computing the slot the item
     * to be inserted.
     *
     * \param flush_function Function to be used for flushing all items in the
     * table.
     *
     * \param end_local_index End index for reduce to index.
     *
     * \param neutral element Neutral element for reduce to index.
     *
     * \param limit_memory_bytes Maximal size of the table in byte. In case size of table
     * exceeds that value, items are spilled to disk.
     *
     * \param max_partition_fill_rate Maximal number of items per partition relative to
     * number of slots allowed to be filled. It the rate is exceeded, items get
     * spilled to disk.
     *
     * \param partition_rate Rate of number of buckets to number of partitions. There is
     * one file writer per partition.
     *
     * \param equal_to_function Function for checking equality of two keys.
     *
     * \param spill_function Function implementing a strategy to spill items to
     * disk.
     */
    ReducePostProbingTable(
        Context& ctx,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const EmitterFunction& emit,
        const IndexFunction& index_function,
        const FlushFunction& flush_function,
        const common::Range& local_index = common::Range(),
        const Key& sentinel = Key(),
        const Value& neutral_element = Value(),
        size_t limit_memory_bytes = 1024* 16,
        double limit_partition_fill_rate = 0.6,
        double partition_rate = 0.1,
        const EqualToFunction& equal_to_function = EqualToFunction())
        : Super(ctx,
                key_extractor,
                reduce_function,
                index_function,
                equal_to_function,
                std::max<size_t>((size_t)(1.0 / partition_rate), 1),
                limit_memory_bytes,
                limit_partition_fill_rate,
                sentinel),
          flush_function_(flush_function),
          emit_(emit),
          local_index_(local_index),
          neutral_element_(neutral_element) {

        assert(partition_rate > 0.0 && partition_rate <= 1.0 &&
               "a partition rate of 1.0 causes exactly one partition.");
    }

    ReducePostProbingTable(
        Context& ctx, KeyExtractor key_extractor,
        ReduceFunction reduce_function, EmitterFunction emit,
        const Value& sentinel = Value())
        : ReducePostProbingTable(
              ctx, key_extractor, reduce_function, emit,
              IndexFunction(), FlushFunction(reduce_function),
              common::Range(), Key(), sentinel) { }

    //! non-copyable: delete copy-constructor
    ReducePostProbingTable(const ReducePostProbingTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePostProbingTable& operator = (const ReducePostProbingTable&) = delete;

    /*!
     * Flushes all items in the whole table.
     */
    void Flush(bool consume = false) {
        LOG << "Flushing items";

        flush_function_.FlushTable(consume, *this);

        LOG << "Flushed items";
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const size_t& partition_id, const KeyValuePair& p) {
        (void)partition_id;
        emit_impl_.EmitElement(p, emit_);
    }

    /*!
     * Returns the total num of items in the table.
     *
     * \return Number of items in the table.
     */
    size_t NumItems() const {

        size_t total_num_items = 0;
        for (size_t num_items : items_per_partition_) {
            total_num_items += num_items;
        }

        return total_num_items;
    }

    /*!
    * Returns the local index range.
    *
    * \return Begin local index.
    */
    common::Range LocalIndex() const {
        return local_index_;
    }

    /*!
     * Returns the neutral element.
     *
     * \return Neutral element.
     */
    Value NeutralElement() const {
        return neutral_element_;
    }

    /*!
     * Returns the sentinel element.
     *
     * \return Sentinal element.
     */
    KeyValuePair Sentinel() const {
        return sentinel_;
    }

private:
    using Super::num_partitions_;
    using Super::key_extractor_;
    using Super::reduce_function_;
    using Super::index_function_;
    using Super::equal_to_function_;
    using Super::size_;
    using Super::items_;
    using Super::limit_items_per_partition_;
    using Super::items_per_partition_;
    using Super::partition_size_;
    using Super::sentinel_;
    using Super::partition_files_;
    using Super::ctx_;

    //! Comparator function for keys.
    FlushFunction flush_function_;

    //! Emitter function.
    EmitterFunction emit_;

    //! [Begin,end) local index (reduce to index).
    common::Range local_index_;

    //! Neutral element (reduce to index).
    Value neutral_element_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_POST_PROBING_TABLE_HEADER

/******************************************************************************/
