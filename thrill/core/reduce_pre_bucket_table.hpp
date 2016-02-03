/*******************************************************************************
 * thrill/core/reduce_pre_bucket_table.hpp
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
#ifndef THRILL_CORE_REDUCE_PRE_BUCKET_TABLE_HEADER
#define THRILL_CORE_REDUCE_PRE_BUCKET_TABLE_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/post_reduce_flush.hpp>
#include <thrill/core/post_reduce_flush_to_index.hpp>
#include <thrill/core/reduce_bucket_hash_table.hpp>
#include <thrill/core/reduce_pre_probing_table.hpp>
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

template <bool, typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl;

template <typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl<true, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p.second);
    }
};

template <typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl<false, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p);
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename FlushFunction = PostReduceFlush<Key, Value, ReduceFunction>,
          typename IndexFunction = PreReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          size_t TargetBlockSize = 16*16,
          const bool FullPreReduce = false>
class ReducePreBucketTable
    : public ReduceBucketHashTable<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          RobustKey,
          IndexFunction, EqualToFunction, TargetBlockSize>
{
    static const bool debug = false;

    using Super = ReduceBucketHashTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction,
              RobustKey,
              IndexFunction, EqualToFunction, TargetBlockSize>;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using Emitters = std::vector<data::DynBlockWriter>;

    PreBucketEmitImpl<RobustKey, Emitters, KeyValuePair> emit_impl_;

    /**
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     *
     * \param num_partitions The number of partitions.
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param limit_memory_bytes Maximal size of the table in byte. In case size of table exceeds that value, items
     *                  are flushed.
     * \param bucket_rate Ratio of number of blocks to number of buckets in the table.
     * \param max_partition_fill_rate Maximal number of blocks per partition relative to number of slots allowed
     *                                to be filled. It the rate is exceeded, items get flushed.
     * \param index_function Function to be used for computing the bucket the item to be inserted.
     * \param equal_to_function Function for checking equality fo two keys.
     */
    ReducePreBucketTable(Context& ctx,
                         size_t num_partitions,
                         KeyExtractor key_extractor,
                         ReduceFunction reduce_function,
                         std::vector<data::DynBlockWriter>& emit,
                         const IndexFunction& index_function,
                         const FlushFunction& flush_function,
                         const Key& /* sentinel */ = Key(),
                         const Value& neutral_element = Value(),
                         size_t limit_memory_bytes = 1024* 16,
                         double bucket_rate = 1.0,
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
                bucket_rate),
          emit_(emit),
          flush_function_(flush_function),
          neutral_element_(neutral_element) {
        sLOG << "creating ReducePreBucketTable with" << emit_.size() << "output emitters";

        assert(num_partitions == emit.size());

        total_items_per_partition_.resize(num_partitions_, 0);

        for (size_t i = 0; i < emit_.size(); i++)
            emit_stats_.push_back(0);
    }

    ReducePreBucketTable(Context& ctx, size_t num_partitions, KeyExtractor key_extractor,
                         ReduceFunction reduce_function,
                         std::vector<data::DynBlockWriter>& emit)
        : ReducePreBucketTable(
              ctx, num_partitions, key_extractor, reduce_function, emit, IndexFunction(),
              FlushFunction(reduce_function)) { }

    //! non-copyable: delete copy-constructor
    ReducePreBucketTable(const ReducePreBucketTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreBucketTable& operator = (const ReducePreBucketTable&) = delete;

    /*!
     * Flush.
     */
    void Flush(bool consume = true) {

        if (FullPreReduce) {
            flush_function_.FlushTable(consume, *this);
        }
        else {
            for (size_t id = 0; id < partition_files_.size(); ++id) {
                FlushPartition(id);
            }
        }
    }

    /*!
     * Flushes all items of a partition.
     *
     * \param partition_id The id of the partition to be flushed.
     */
    void FlushPartition(size_t partition_id) {

        Super::FlushPartitionE(
            partition_id, true,
            [=](const size_t& partition_id, const KeyValuePair& p) {
                this->EmitAll(partition_id, p);
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

    /*!
     * Returns the total num of items in the table.
     *
     * \return Number of items in the table.
     */
    size_t NumItemsPerTable() const {
        size_t total_num_items = 0;
        for (size_t num_items : items_per_partition_) {
            total_num_items += num_items;
        }

        return total_num_items;
    }

    /*!
     * Returns the number of block in the table.
     *
     * \return Number of blocks in the table.
     */
    size_t NumBlocksPerTable() const {
        return num_blocks_;
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
    * Returns the local index range.
    *
    * \return Begin local index.
    */
    common::Range LocalIndex() const {
        return common::Range(0, num_buckets_ - 1);
    }

    /*!
     * Closes all emitter
     */
    void CloseEmitter() {
        sLOG << "emit stats: ";
        unsigned int i = 0;
        for (auto& e : emit_) {
            e.Close();
            sLOG << "emitter " << i << " pushed " << emit_stats_[i++];
        }
    }

private:
    using Super::buckets_;
    using Super::key_extractor_;
    using Super::reduce_function_;
    using Super::index_function_;
    using Super::equal_to_function_;
    using Super::block_pool_;
    using Super::num_partitions_;
    using Super::num_buckets_;
    using Super::num_buckets_per_partition_;
    using Super::max_items_per_partition_;
    using Super::max_blocks_per_partition_;
    using Super::num_blocks_;
    using Super::limit_blocks_;
    using Super::items_per_partition_;
    using Super::limit_items_per_partition_;
    using Super::partition_files_;
    using Super::ctx_;

    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& emit_;

    //! Flush function.
    FlushFunction flush_function_;

    //! Number of items per partition.
    std::vector<size_t> total_items_per_partition_;

    //! Emitter stats.
    std::vector<size_t> emit_stats_;

    //! Neutral element (reduce to index).
    Value neutral_element_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_BUCKET_TABLE_HEADER

/******************************************************************************/
