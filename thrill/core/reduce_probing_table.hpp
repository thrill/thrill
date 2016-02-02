/*******************************************************************************
 * thrill/core/reduce_probing_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PROBING_TABLE_HEADER
#define THRILL_CORE_REDUCE_PROBING_TABLE_HEADER

namespace thrill {
namespace core {

#include <utility>
#include <vector>

/**
 * A data structure which takes an arbitrary value and extracts a key using a
 * key extractor function from that value. A key may also be provided initially
 * as part of a key/value pair, not requiring to extract a key.
 *
 * Afterwards, the key is hashed and the hash is used to assign that key/value
 * pair to some slot.
 *
 * In case a slot already has a key/value pair and the key of that value and the
 * key of the value to be inserted are them same, the values are reduced
 * according to some reduce function. No key/value is added to the data
 * structure.
 *
 * If the keys are different, the next slot (moving to the right) is considered.
 * If the slot is occupied, the same procedure happens again (know as linear
 * probing.)
 *
 * Finally, the key/value pair to be inserted may either:
 *
 * 1.) Be reduced with some other key/value pair, sharing the same key.
 * 2.) Inserted at a free slot.
 * 3.) Trigger a resize of the data structure in case there are no more free
 *     slots in the data structure.
 *
 * The following illustrations shows the general structure of the data
 * structure.  The set of slots is divided into 1..n partitions. Each key is
 * hashed into exactly one partition.
 *
 *
 *     Partition 0 Partition 1 Partition 2 Partition 3 Partition 4
 *     P00 P01 P02 P10 P11 P12 P20 P21 P22 P30 P31 P32 P40 P41 P42
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *    ||  |   |   ||  |   |   ||  |   |   ||  |   |   ||  |   |  ||
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *                <-   LI  ->
 *                     LI..Local Index
 *    <-        GI         ->
 *              GI..Global Index
 *         PI 0        PI 1        PI 2        PI 3        PI 4
 *         PI..Partition ID
 *
 */
template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey,
          typename IndexFunction,
          typename EqualToFunction,
          typename SubTable>
class ReduceProbingTable
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    ReduceProbingTable(
        Context& ctx,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const IndexFunction& index_function,
        const EqualToFunction& equal_to_function,
        size_t num_partitions,
        size_t limit_memory_bytes,
        double limit_partition_fill_rate,
        const Key& sentinel = Key())
        : ctx_(ctx),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          num_partitions_(num_partitions),
          limit_memory_bytes_(limit_memory_bytes),
          items_per_partition_(num_partitions_, 0) {

        assert(num_partitions > 0);

        // calculate partition_size_ from the memory limit and the number of
        // partitions required

        assert(limit_memory_bytes >= 0 &&
               "limit_memory_bytes must be greater than or equal to 0. "
               "A byte size of zero results in exactly one item per partition");

        partition_size_ = std::max<size_t>(
            1,
            (size_t)(limit_memory_bytes_
                     / static_cast<double>(sizeof(KeyValuePair))
                     / static_cast<double>(num_partitions_)));

        size_ = partition_size_ * num_partitions_;

        assert(partition_size_ > 0);
        assert(size_ > 0);

        // calculate limit on the number of items in a partition before these
        // are spilled to disk or flushed to network.

        assert(limit_partition_fill_rate >= 0.0 && limit_partition_fill_rate <= 1.0
               && "limit_partition_fill_rate must be between 0.0 and 1.0. "
               "with a fill rate of 0.0, items are immediately flushed.");

        limit_items_per_partition_ =
            (size_t)(partition_size_ * limit_partition_fill_rate);

        assert(limit_items_per_partition_ >= 0);

        // construct the hash table itself. fill it with sentinels

        sentinel_ = KeyValuePair(sentinel, Value());
        items_.resize(size_, sentinel_);

        // allocate Files for each partition to spill into. TODO(tb): switch to
        // FilePtr ondemand

        for (size_t i = 0; i < num_partitions_; i++) {
            partition_files_.push_back(ctx.GetFile());
        }
    }

    /*!
     * Inserts a value. Calls the key_extractor_, makes a key-value-pair and
     * inserts the pair via the Insert() function.
     */
    void Insert(const Value& p) {
        Insert(std::make_pair(key_extractor_(p), p));
    }

    /*!
     * Inserts a value into the table, potentially reducing it in case both the
     * key of the value already in the table and the key of the value to be
     * inserted are the same.
     *
     * An insert may trigger a partial flush of the partition with the most
     * items if the maximal number of items in the table (max_num_items_table)
     * is reached.
     *
     * Alternatively, it may trigger a resize of the table in case the maximal
     * fill ratio per partition is reached.
     *
     * \param kv Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {
        static const bool debug = false;

        typename IndexFunction::IndexResult h = index_function_(
            kv.first, num_partitions_, partition_size_, size_, 0);

        assert(h.partition_id < num_partitions_);
        assert(h.global_index < size_);

        KeyValuePair* initial = &items_[h.global_index];
        KeyValuePair* current = initial;
        KeyValuePair* last_item =
            &items_[(h.partition_id + 1) * partition_size_ - 1];

        while (!equal_to_function_(current->first, sentinel_.first))
        {
            if (equal_to_function_(current->first, kv.first))
            {
                LOG << "match of key: " << kv.first
                    << " and " << current->first << " ... reducing...";

                current->second = reduce_function_(current->second, kv.second);

                LOG << "...finished reduce!";
                return;
            }

            if (current == last_item) {
                current -= (partition_size_ - 1);
            }
            else {
                ++current;
            }

            // flush partition, if all slots are reserved
            if (current == initial) {

                SpillPartition(h.partition_id);

                *current = kv;

                // increase counter for partition
                items_per_partition_[h.partition_id]++;

                return;
            }
        }

        // insert new pair
        *current = kv;

        // increase counter for partition
        items_per_partition_[h.partition_id]++;

        if (items_per_partition_[h.partition_id] > limit_items_per_partition_)
        {
            SpillPartition(h.partition_id);
        }
    }

    //! \name Spilling Mechanisms to External Memory Files
    //! \{

    //! Spill all items of a partition into an external memory File.
    void SpillPartition(size_t partition_id) {
        LOG << "Spilling items of partition with id: " << partition_id;

        data::File::Writer writer = partition_files_[partition_id].GetWriter();

        for (size_t i = partition_id * partition_size_;
             i < (partition_id + 1) * partition_size_; i++)
        {
            KeyValuePair& current = items_[i];
            if (current.first != sentinel_.first)
            {
                writer.Put(current);
                items_[i] = sentinel_;
            }
        }

        // reset partition specific counter
        items_per_partition_[partition_id] = 0;

        LOG << "Spilled items of partition with id: " << partition_id;
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

    //! Storing the actual hash table.
    std::vector<KeyValuePair> items_;

    //! Sentinel element used to flag free slots.
    KeyValuePair sentinel_;

    //! Store the files for partitions.
    std::vector<data::File> partition_files_;

    //! \name Fixed Operational Parameters
    //! \{

    //! Number of partitions.
    size_t num_partitions_;

    //! Limit on the number of bytes used by the table in memory.
    size_t limit_memory_bytes_ = 0;

    //! Size of the table, which is the number of slots available for items.
    size_t size_;

    //! Number of items per partition before spilling items to EM.
    size_t limit_items_per_partition_;

    //! Partition size.
    size_t partition_size_;

    //! \}

    //! \name Current Statistical Parameters
    //! \{

    //! Current number of items per partition.
    std::vector<size_t> items_per_partition_;

    //! \}
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PROBING_TABLE_HEADER

/******************************************************************************/
