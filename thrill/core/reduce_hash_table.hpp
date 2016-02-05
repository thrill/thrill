/*******************************************************************************
 * thrill/core/reduce_hash_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_HASH_TABLE_HEADER
#define THRILL_CORE_REDUCE_HASH_TABLE_HEADER

#include <thrill/api/context.hpp>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

/*!
 * Common super-class for bucket and linear-probing hash/reduce tables. It
 * contains partitioning parameters, statistics, and the output files.
 */
template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool RobustKey,
          typename IndexFunction,
          typename EqualToFunction>
class ReduceHashTable
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    ReduceHashTable(
        Context& ctx,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        Emitter& emitter,
        size_t num_partitions,
        size_t limit_memory_bytes,
        bool immediate_flush,
        const Key& sentinel,
        const IndexFunction& index_function,
        const EqualToFunction& equal_to_function)
        : ctx_(ctx),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emitter_(emitter),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          num_partitions_(num_partitions),
          limit_memory_bytes_(limit_memory_bytes),
          immediate_flush_(immediate_flush),
          sentinel_(KeyValuePair(sentinel, Value())),
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
    ReduceHashTable(const ReduceHashTable&) = delete;
    //! non-copyable: delete assignment operator
    ReduceHashTable& operator = (const ReduceHashTable&) = delete;

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

    //! Returns equal_to_function_
    const EqualToFunction & equal_to_function() const { return equal_to_function_; }

    //! Returns the vector of partition files.
    std::vector<data::File> & partition_files() { return partition_files_; }

    //! Returns the number of partitions
    size_t num_partitions() { return num_partitions_; }

    //! Returns limit_memory_bytes_
    size_t limit_memory_bytes() const { return limit_memory_bytes_; }

    //! Returns limit_items_per_partition_
    size_t limit_items_per_partition() const
    { return limit_items_per_partition_; }

    //! Returns sentinel_
    const KeyValuePair & sentinel() const { return sentinel_; }

    //! Returns items_per_partition_
    size_t items_per_partition(size_t id) const {
        assert(id < items_per_partition_.size());
        return items_per_partition_[id];
    }

    //! Returns the total num of items in the table.
    size_t num_items() const {

        size_t total_num_items = 0;
        for (size_t num_items : items_per_partition_) {
            total_num_items += num_items;
        }

        return total_num_items;
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

    //! Size of the table in bytes
    const size_t limit_memory_bytes_;

    //! Number of items in a partition before the partition is spilled.
    size_t limit_items_per_partition_;

    //! Whether to spill overfull partitions to disk or to immediately flush to
    //! next stage.
    bool immediate_flush_;

    //! Sentinel element used to flag free slots.
    KeyValuePair sentinel_;

    //! \}

    //! \name Current Statistical Parameters
    //! \{

    //! Current number of items per partition.
    std::vector<size_t> items_per_partition_;

    //! \}
};

//! traits class for ReduceProbingHashTable, mainly to determine a good sentinel
//! (blank table entries) for standard types.
template <typename Type, class Enable = void>
class ProbingTableTraits
{
public:
    static Type Sentinel() {
        static bool warned = false;
        if (!warned) {
            LOG1 << "No good default sentinel for probing hash table "
                 << "could be determined. Please pass one manually.";
            warned = true;
        }
        return Type();
    }
};

//! traits class for all PODs -> use numeric limits.
template <typename T>
class ProbingTableTraits<
        T, typename std::enable_if<std::is_pod<T>::value>::type>
{
public:
    static T Sentinel() { return std::numeric_limits<T>::max(); }
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_HASH_TABLE_HEADER

/******************************************************************************/
