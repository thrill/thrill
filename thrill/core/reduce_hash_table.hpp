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

#include <thrill/core/bucket_block_pool.hpp>

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
          typename KeyExtractor, typename ReduceFunction,
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
        const IndexFunction& index_function,
        const EqualToFunction& equal_to_function,
        size_t num_partitions,
        size_t limit_memory_bytes)
        : ctx_(ctx),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          num_partitions_(num_partitions),
          limit_memory_bytes_(limit_memory_bytes),
          items_per_partition_(num_partitions_, 0) {

        assert(num_partitions > 0);

        // allocate Files for each partition to spill into. TODO(tb): switch to
        // FilePtr ondemand

        for (size_t i = 0; i < num_partitions_; i++) {
            partition_files_.push_back(ctx.GetFile());
        }
    }

    //! non-copyable: delete copy-constructor
    ReduceHashTable(const ReduceHashTable&) = delete;
    //! non-copyable: delete assignment operator
    ReduceHashTable& operator = (const ReduceHashTable&) = delete;

    //! \name Accessors
    //! \{

    //! Returns the vector of partition files.
    std::vector<data::File> & PartitionFiles() {
        return partition_files_;
    }

    //! \}

protected:
    //! Context
    Context& ctx_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

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

    //! \}

    //! \name Current Statistical Parameters
    //! \{

    //! Current number of items per partition.
    std::vector<size_t> items_per_partition_;

    //! \}
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_HASH_TABLE_HEADER

/******************************************************************************/
