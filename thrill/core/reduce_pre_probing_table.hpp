/*******************************************************************************
 * thrill/core/reduce_pre_probing_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER
#define THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/post_reduce_flush.hpp>
#include <thrill/core/post_reduce_flush_to_index.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
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
class PreReduceByHashKey
{
public:
    struct IndexResult {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    explicit PreReduceByHashKey(const HashFunction& hash_function = HashFunction())
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
class PreReduceByIndex
{
public:
    struct IndexResult {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    size_t size_;

    explicit PreReduceByIndex(size_t size) : size_(size) { }

    IndexResult
    operator () (const Key& k,
                 const size_t& num_partitions,
                 const size_t& num_buckets_per_partition,
                 const size_t& num_buckets_per_table,
                 const size_t& offset) const {

        (void)num_buckets_per_partition;
        (void)offset;

        return IndexResult { k* num_partitions / size_, k* num_buckets_per_table / size_ };
    }
};

template <bool, typename Emitters, typename KeyValuePair>
struct PreProbingEmitImpl;

template <typename Emitters, typename KeyValuePair>
struct PreProbingEmitImpl<true, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p.second);
    }
};

template <typename Emitters, typename KeyValuePair>
struct PreProbingEmitImpl<false, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p);
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename FlushFunction = PostReduceFlush<Key, Value, ReduceFunction>,
          typename IndexFunction = PreReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class ReducePreProbingTable
    : public ReduceProbingHashTable<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          RobustKey,
          IndexFunction, EqualToFunction>
{
    static const bool debug = true;

    using Super = ReduceProbingHashTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction,
              RobustKey,
              IndexFunction, EqualToFunction>;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using Emitters = std::vector<data::DynBlockWriter>;

    PreProbingEmitImpl<RobustKey, Emitters, KeyValuePair> emit_impl_;

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
     * \param max_partition_fill_rate Maximal number of items per partition
     * relative to number of slots allowed to be filled. It the rate is
     * exceeded, items get spilled to disk.
     *
     * \param partition_rate Rate of number of buckets to number of
     * partitions. There is one file writer per partition.
     *
     * \param equal_to_function Function for checking equality of two keys.
     *
     * \param spill_function Function implementing a strategy to spill items to
     * disk.
     */
    ReducePreProbingTable(
        Context& ctx,
        size_t num_partitions,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        std::vector<data::DynBlockWriter>& emit,
        const IndexFunction& index_function,
        const FlushFunction& flush_function,
        const Key& sentinel = Key(),
        const Value& neutral_element = Value(),
        size_t limit_memory_bytes = 1024* 16,
        double limit_partition_fill_rate = 0.6,
        const EqualToFunction& equal_to_function = EqualToFunction())
        : Super(ctx,
                key_extractor,
                reduce_function,
                index_function,
                equal_to_function,
                num_partitions,
                limit_memory_bytes,
                limit_partition_fill_rate,
                sentinel),
          emit_(emit),
          flush_function_(flush_function),
          neutral_element_(neutral_element) {

        assert(num_partitions == emit.size());

        for (size_t i = 0; i < emit.size(); i++) {
            emit_stats_.push_back(0);
        }
    }

    ReducePreProbingTable(
        Context& ctx, size_t num_partitions, KeyExtractor key_extractor,
        ReduceFunction reduce_function, std::vector<data::DynBlockWriter>& emit,
        const Key& sentinel)
        : ReducePreProbingTable(
              ctx, num_partitions, key_extractor, reduce_function,
              emit, sentinel, IndexFunction(),
              FlushFunction(reduce_function))
    { }

    //! non-copyable: delete copy-constructorx
    ReducePreProbingTable(const ReducePreProbingTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreProbingTable& operator = (const ReducePreProbingTable&) = delete;

    /*!
     * Flush.
     */
    void Flush(bool consume = true) {
        for (size_t id = 0; id < partition_files_.size(); ++id) {
            FlushPartition(id, consume);
        }
    }

    /*!
     * Flushes all items of a partition.
     *
     * \param partition_id The id of the partition to be flushed.
     */
    void FlushPartition(size_t partition_id, bool consume) {

        Super::FlushPartitionE(
            partition_id, consume,
            [&](const size_t& partition_id, const KeyValuePair& p) {
                EmitAll(partition_id, p);
            });

        // flush elements pushed into emitter
        emit_[partition_id].Flush();
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const size_t& partition_id, const KeyValuePair& p) {
        emit_stats_[partition_id]++;
        emit_impl_.EmitElement(p, partition_id, emit_);
    }

    //! Returns the neutral element.
    Value NeutralElement() const { return neutral_element_; }

    /*!
    * Closes all emitter.
    */
    void CloseEmitter() {
        sLOG << "emit stats: ";
        size_t i = 0;
        for (auto& e : emit_) {
            e.Close();
            sLOG << "emitter " << i << " pushed " << emit_stats_[i++];
        }
    }

private:
    using Super::num_partitions_;
    using Super::partition_files_;

    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& emit_;

    //! Comparator function for keys.
    FlushFunction flush_function_;

    //! Emitter stats.
    std::vector<int> emit_stats_;

    //! Neutral element (reduce to index).
    Value neutral_element_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

/******************************************************************************/
