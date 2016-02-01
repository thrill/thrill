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
        size_t num_partitions,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const IndexFunction& index_function,
        const EqualToFunction& equal_to_function)
        : key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          num_partitions_(num_partitions),
          items_per_partition_(num_partitions_, 0) {
        assert(num_partitions > 0);
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

    void SpillPartition(size_t partition_id) {
        SubTable& t = *static_cast<SubTable*>(this);
        t.SpillPartition(partition_id);
    }

protected:
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

    //! \name Fixed Operational Parameters
    //! \{

    //! Number of partitions.
    size_t num_partitions_;

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
