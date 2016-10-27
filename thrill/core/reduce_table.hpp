/*******************************************************************************
 * thrill/core/reduce_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_TABLE_HEADER
#define THRILL_CORE_REDUCE_TABLE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/core/reduce_functional.hpp>

#include <algorithm>
#include <functional>
#include <limits>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

//! Enum class to select a hash table implementation.
enum class ReduceTableImpl {
    PROBING, OLD_PROBING, BUCKET
};

/*!
 * Configuration class to define operational parameters of reduce hash tables
 * and reduce phases. Most members can be defined static constexpr or be mutable
 * variables. Not all members need to be used by all implementations.
 */
class DefaultReduceConfig
{
public:
    //! limit on the fill rate of a reduce table partition prior to triggering a
    //! flush.
    double limit_partition_fill_rate_ = 0.5;

    //! only for BucketHashTable: ratio of number of buckets in a partition
    //! relative to the maximum possible number.
    double bucket_rate_ = 0.6;

    //! select the hash table in the reduce phase by enum
    static constexpr ReduceTableImpl table_impl_ = ReduceTableImpl::PROBING;

    //! only for growing ProbingHashTable: items initially in a partition.
    static constexpr size_t initial_items_per_partition_ = 512;

    //! only for BucketHashTable: size of a block in the bucket chain in bytes
    //! (must be a static constexpr)
    static constexpr size_t bucket_block_size_ = 512;

    //! use MixStream instead of CatStream in ReduceNodes: this makes the order
    //! of items delivered in the ReduceFunction arbitrary.
    static constexpr bool use_mix_stream_ = true;

    //! use an additional thread in ReduceNode and ReduceToIndexNode to process
    //! the pre and post phases simultaneously.
    static constexpr bool use_post_thread_ = false;

    //! \name Accessors
    //! \{

    //! Returns limit_partition_fill_rate_
    double limit_partition_fill_rate() const
    { return limit_partition_fill_rate_; }

    //! Returns bucket_rate_
    double bucket_rate() const { return bucket_rate_; }

    //! \}
};

/*!
 * DefaultReduceConfig with implementation type selection
 */
template <ReduceTableImpl table_impl>
class DefaultReduceConfigSelect : public DefaultReduceConfig
{
public:
    //! select the hash table in the reduce phase by enum
    static constexpr ReduceTableImpl table_impl_ = table_impl;
};

/*!
 * Common super-class for bucket and linear-probing hash/reduce tables. It
 * contains partitioning parameters, statistics, and the output files.
 */
template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool VolatileKey,
          typename ReduceConfig_, typename IndexFunction,
          typename KeyEqualFunction = std::equal_to<Key> >
class ReduceTable
{
public:
    static constexpr bool debug = false;

    using ReduceConfig = ReduceConfig_;
    using TableItem =
              typename common::If<
                  VolatileKey, std::pair<Key, Value>, Value>::type;
    using MakeTableItem = ReduceMakeTableItem<Value, TableItem, VolatileKey>;

    ReduceTable(
        Context& ctx, size_t dia_id,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        Emitter& emitter,
        size_t num_partitions,
        const ReduceConfig& config,
        bool immediate_flush,
        const IndexFunction& index_function,
        const KeyEqualFunction& key_equal_function)
        : ctx_(ctx), dia_id_(dia_id),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emitter_(emitter),
          index_function_(index_function),
          key_equal_function_(key_equal_function),
          num_partitions_(num_partitions),
          config_(config),
          immediate_flush_(immediate_flush),
          items_per_partition_(num_partitions_, 0) {

        assert(num_partitions > 0);

        // allocate Files for each partition to spill into. TODO(tb): switch to
        // FilePtr ondemand

        if (!immediate_flush_) {
            for (size_t i = 0; i < num_partitions_; i++) {
                partition_files_.push_back(ctx.GetFile(dia_id_));
            }
        }
    }

    //! non-copyable: delete copy-constructor
    ReduceTable(const ReduceTable&) = delete;
    //! non-copyable: delete assignment operator
    ReduceTable& operator = (const ReduceTable&) = delete;

    //! Deallocate memory
    void Dispose() {
        std::vector<data::File>().swap(partition_files_);
        std::vector<size_t>().swap(items_per_partition_);
    }

    //! \name Accessors
    //! \{

    //! Returns the context
    Context& ctx() const { return ctx_; }

    //! Returns dia_id_
    size_t dia_id() const { return dia_id_; }

    //! Returns the key_extractor
    const KeyExtractor& key_extractor() const { return key_extractor_; }

    //! Returns the reduce_function
    const ReduceFunction& reduce_function() const { return reduce_function_; }

    //! Returns emitter_
    const Emitter& emitter() const { return emitter_; }

    //! Returns index_function_
    const IndexFunction& index_function() const { return index_function_; }

    //! Returns index_function_ (mutable)
    IndexFunction& index_function() { return index_function_; }

    //! Returns key_equal_function_
    const KeyEqualFunction& key_equal_function() const
    { return key_equal_function_; }

    //! Returns the vector of partition files.
    std::vector<data::File>& partition_files() { return partition_files_; }

    //! Returns the number of partitions
    size_t num_partitions() { return num_partitions_; }

    //! Returns num_buckets_
    size_t num_buckets() const { return num_buckets_; }

    //! Returns num_buckets_per_partition_
    size_t num_buckets_per_partition() const
    { return num_buckets_per_partition_; }

    //! Returns limit_memory_bytes_
    size_t limit_memory_bytes() const { return limit_memory_bytes_; }

    //! Returns limit_items_per_partition_
    size_t limit_items_per_partition() const
    { return limit_items_per_partition_; }

    //! Returns items_per_partition_
    size_t items_per_partition(size_t id) const {
        assert(id < items_per_partition_.size());
        return items_per_partition_[id];
    }

    //! Returns the total num of items in the table.
    size_t num_items() const {
        return num_items_;
    }

    //! Returns the total num of items in the table.
    size_t num_items_calc() const {
        size_t total_num_items = 0;
        for (size_t num_items : items_per_partition_) {
            total_num_items += num_items;
        }

        return total_num_items;
    }

    //! calculate key range for the given output partition
    common::Range key_range(size_t partition_id) {
        return index_function().inverse_range(
            partition_id, num_buckets_per_partition_, num_buckets_);
    }

    //! returns whether and partition has spilled data into external memory.
    bool has_spilled_data() const {
        for (const data::File& file : partition_files_) {
            if (file.num_items()) return true;
        }
        return false;
    }

    //! \}

    //! \name Switches for VolatileKey
    //! \{

    Key key(const TableItem& t) {
        return MakeTableItem::GetKey(t, key_extractor_);
    }

    TableItem reduce(const TableItem& a, const TableItem& b) {
        return MakeTableItem::Reduce(a, b, reduce_function_);
    }

    //! \}

protected:
    //! Context
    Context& ctx_;

    //! Associated DIA id
    size_t dia_id_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Emitter object to receive items outputted to next phase.
    Emitter& emitter_;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    KeyEqualFunction key_equal_function_;

    //! Store the files for partitions.
    std::vector<data::File> partition_files_;

    //! \name Fixed Operational Parameters
    //! \{

    //! Number of partitions
    const size_t num_partitions_;

    //! config of reduce table
    ReduceConfig config_;

    //! Size of the table, which is the number of slots / buckets / entries
    //! available for items or chains of items.
    size_t num_buckets_;

    //! Partition size, the number of buckets per partition.
    size_t num_buckets_per_partition_;

    //! Size of the table in bytes
    size_t limit_memory_bytes_ = 0;

    //! Number of items in a partition before the partition is spilled.
    size_t limit_items_per_partition_;

    //! Whether to spill overfull partitions to disk or to immediately flush to
    //! next phase.
    bool immediate_flush_;

    //! \}

    //! \name Current Statistical Parameters
    //! \{

    //! Current number of items.
    size_t num_items_ = 0;

    //! Current number of items per partition.
    std::vector<size_t> items_per_partition_;

    //! \}
};

//! Type selection via ReduceTableImpl enum
template <ReduceTableImpl ImplSelect,
          typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          typename Emitter,
          const bool VolatileKey = false,
          typename ReduceConfig = DefaultReduceConfig,
          typename IndexFunction = ReduceByHash<Key>,
          typename KeyEqualFunction = std::equal_to<Key> >
class ReduceTableSelect;

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_TABLE_HEADER

/******************************************************************************/
