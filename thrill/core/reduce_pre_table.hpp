/*******************************************************************************
 * thrill/core/reduce_pre_table.hpp
 *
 * Hash table with support for reduce and partitions.
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
#ifndef THRILL_CORE_REDUCE_PRE_TABLE_HEADER
#define THRILL_CORE_REDUCE_PRE_TABLE_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/post_reduce_flush.hpp>
#include <thrill/core/post_reduce_flush_to_index.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_probing_hash_table.hpp>
#include <thrill/data/block_writer.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
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
struct PreEmitImpl;

template <typename Emitters, typename KeyValuePair>
struct PreEmitImpl<true, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p.second);
    }
};

template <typename Emitters, typename KeyValuePair>
struct PreEmitImpl<false, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p);
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey,
          typename IndexFunction,
          typename EqualToFunction,
          template <typename ValueType, typename Key, typename Value,
                    typename KeyExtractor, typename ReduceFunction,
                    const bool RobustKey,
                    typename IndexFunction,
                    typename EqualToFunction> class HashTable>
class ReducePreTable
{
    static const bool debug = false;

public:
    using Table = HashTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction,
              RobustKey,
              IndexFunction, EqualToFunction
              >;

    using KeyValuePair = std::pair<Key, Value>;

    using Emitters = std::vector<data::DynBlockWriter>;

    PreEmitImpl<RobustKey, Emitters, KeyValuePair> emit_impl_;

    /**
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     */
    ReducePreTable(Context& ctx,
                   size_t num_partitions,
                   KeyExtractor key_extractor,
                   ReduceFunction reduce_function,
                   std::vector<data::DynBlockWriter>& emit,
                   const IndexFunction& index_function,
                   const Key& /* sentinel */ = Key(),
                   const Value& neutral_element = Value(),
                   size_t limit_memory_bytes = 1024* 16,
                   double bucket_rate = 1.0,
                   double limit_partition_fill_rate = 0.6,
                   const EqualToFunction& equal_to_function = EqualToFunction())
        : table_(ctx,
                 key_extractor, reduce_function,
                 index_function, equal_to_function,
                 num_partitions,
                 limit_memory_bytes,
                 limit_partition_fill_rate, bucket_rate),
          emit_(emit),
          neutral_element_(neutral_element) {
        sLOG << "creating ReducePreTable with" << emit_.size() << "output emitters";

        assert(num_partitions == emit.size());

        for (size_t i = 0; i < emit_.size(); i++)
            emit_stats_.push_back(0);
    }

    ReducePreTable(Context& ctx, size_t num_partitions, KeyExtractor key_extractor,
                   ReduceFunction reduce_function,
                   std::vector<data::DynBlockWriter>& emit)
        : ReducePreTable(
              ctx, num_partitions, key_extractor, reduce_function, emit, IndexFunction()) { }

    //! non-copyable: delete copy-constructor
    ReducePreTable(const ReducePreTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreTable& operator = (const ReducePreTable&) = delete;

    void Insert(const Value& p) {
        return table_.Insert(p);
    }

    void Insert(const KeyValuePair& kv) {
        return table_.Insert(kv);
    }

    /*!
     * Flush.
     */
    void Flush(bool consume = true) {
        for (size_t id = 0; id < table_.num_partitions(); ++id) {
            FlushPartition(id, consume);
        }
    }

    /*!
     * Flushes all items of a partition.
     *
     * \param partition_id The id of the partition to be flushed.
     */
    void FlushPartition(size_t partition_id, bool consume) {

        table_.FlushPartitionE(
            partition_id, consume,
            [=](const size_t& partition_id, const KeyValuePair& p) {
                this->EmitAll(partition_id, p);
            });

        // flush elements pushed into emitter
        emit_[partition_id].Flush();
    }

    //! Emits element to all children
    void EmitAll(const size_t& partition_id, const KeyValuePair& p) {
        emit_stats_[partition_id]++;
        emit_impl_.EmitElement(p, partition_id, emit_);
    }

    //! Returns the neutral element.
    Value NeutralElement() const {
        return neutral_element_;
    }

    //! Closes all emitter
    void CloseEmitter() {
        sLOG << "emit stats: ";
        size_t i = 0;
        for (auto& e : emit_) {
            e.Close();
            sLOG << "emitter " << i << " pushed " << emit_stats_[i++];
        }
    }

    //! \name Accessors
    //! {

    //! Returns the total num of items in the table.
    size_t num_items() const { return table_.num_items(); }

    //! }

private:
    //! the first-level hash table implementation
    Table table_;

    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& emit_;

    //! Emitter stats.
    std::vector<size_t> emit_stats_;

    //! Neutral element (reduce to index).
    Value neutral_element_;
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename IndexFunction = PreReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
using ReducePreBucketTable = ReducePreTable<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          RobustKey,
          IndexFunction, EqualToFunction,
          ReduceBucketHashTable>;

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename IndexFunction = PreReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
using ReducePreProbingTable = ReducePreTable<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          RobustKey,
          IndexFunction, EqualToFunction,
          ReduceProbingHashTable>;

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_TABLE_HEADER

/******************************************************************************/
