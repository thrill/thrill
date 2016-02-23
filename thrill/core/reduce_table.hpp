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

#include <algorithm>
#include <functional>
#include <limits>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

/*!
 * Configuration class to define operational parameters of reduce hash tables
 * and reduce stages. Most members can be defined static const or be mutable
 * variables. Not all members need to be used by all implementations.
 */
class DefaultReduceTableConfig
{
public:
    //! limit on the amount of memory used by the reduce table
    size_t limit_memory_bytes_ = 32 * 1024 * 1024llu;

    //! limit on the fill rate of a reduce table partition prior to triggering a
    //! flush.
    double limit_partition_fill_rate_ = 0.8;

    //! only for BucketHashTable: ratio of number of buckets in a partition
    //! relative to the maximum possible number.
    double bucket_rate_ = 0.5;

    //! only for BucketHashTable: size of a block in the bucket chain in bytes
    //! (must be a static constexpr)
    static constexpr size_t bucket_block_size = 256;

    //! \name Accessors
    //! {

    //! Returns limit_memory_bytes_
    size_t limit_memory_bytes() const { return limit_memory_bytes_; }

    //! Returns limit_partition_fill_rate_
    double limit_partition_fill_rate() const
    { return limit_partition_fill_rate_; }

    //! Returns bucket_rate_
    double bucket_rate() const { return bucket_rate_; }

    //! }
};

/*!
 * Common super-class for bucket and linear-probing hash/reduce tables. It
 * contains partitioning parameters, statistics, and the output files.
 */
template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool RobustKey,
          typename IndexFunction,
          typename ReduceStageConfig = DefaultReduceTableConfig,
          typename EqualToFunction = std::equal_to<Key> >
class ReduceTable
{
public:
    static const bool debug = false;

    using KeyValuePair = std::pair<Key, Value>;

    ReduceTable(
        Context& ctx,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        Emitter& emitter,
        size_t num_partitions,
        const ReduceStageConfig& config,
        bool immediate_flush,
        const IndexFunction& index_function,
        const EqualToFunction& equal_to_function)
        : ctx_(ctx),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emitter_(emitter),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          num_partitions_(num_partitions),
          limit_memory_bytes_(config.limit_memory_bytes()),
          immediate_flush_(immediate_flush),
          items_per_partition_(num_partitions_, 0) {

        assert(num_partitions > 0);

        // allocate Files for each partition to spill into. TODO(tb): switch to
        // FilePtr ondemand

        if (!immediate_flush_) {
            for (size_t i = 0; i < num_partitions_; i++) {
                partition_files_.push_back(ctx.GetFile());
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
    Context & ctx() const { return ctx_; }

    //! Returns the key_extractor
    const KeyExtractor & key_extractor() const { return key_extractor_; }

    //! Returns the reduce_function
    const ReduceFunction & reduce_function() const { return reduce_function_; }

    //! Returns emitter_
    const Emitter & emitter() const { return emitter_; }

    //! Returns index_function_
    const IndexFunction & index_function() const { return index_function_; }

    //! Returns index_function_ (mutable)
    IndexFunction & index_function() { return index_function_; }

    //! Returns equal_to_function_
    const EqualToFunction & equal_to_function() const
    { return equal_to_function_; }

    //! Returns the vector of partition files.
    std::vector<data::File> & partition_files() { return partition_files_; }

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

protected:
    //! Context
    Context& ctx_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Emitter object to receive items outputted to next stage.
    Emitter& emitter_;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Store the files for partitions.
    std::vector<data::File> partition_files_;

    //! \name Fixed Operational Parameters
    //! \{

    //! Number of partitions
    const size_t num_partitions_;

    //! Size of the table, which is the number of slots / buckets / entries
    //! available for items or chains of items.
    size_t num_buckets_;

    //! Partition size, the number of buckets per partition.
    size_t num_buckets_per_partition_;

    //! Size of the table in bytes
    const size_t limit_memory_bytes_;

    //! Number of items in a partition before the partition is spilled.
    size_t limit_items_per_partition_;

    //! Whether to spill overfull partitions to disk or to immediately flush to
    //! next stage.
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

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_TABLE_HEADER

/******************************************************************************/
