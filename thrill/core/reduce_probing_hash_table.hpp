/*******************************************************************************
 * thrill/core/reduce_probing_hash_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PROBING_HASH_TABLE_HEADER
#define THRILL_CORE_REDUCE_PROBING_HASH_TABLE_HEADER

#include <thrill/core/reduce_table.hpp>

#include <functional>
#include <limits>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

/*!
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
          typename KeyExtractor, typename ReduceFunction, typename Emitter,
          const bool RobustKey,
          typename IndexFunction,
          typename ReduceStageConfig = DefaultReduceTableConfig,
          typename EqualToFunction = std::equal_to<Key> >
class ReduceProbingHashTable
    : public ReduceTable<ValueType, Key, Value,
                         KeyExtractor, ReduceFunction, Emitter,
                         RobustKey, IndexFunction,
                         ReduceStageConfig, EqualToFunction>
{
    static const bool debug = false;

    using Super = ReduceTable<ValueType, Key, Value,
                              KeyExtractor, ReduceFunction, Emitter,
                              RobustKey, IndexFunction,
                              ReduceStageConfig, EqualToFunction>;

public:
    using KeyValuePair = std::pair<Key, Value>;

    ReduceProbingHashTable(
        Context& ctx,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        Emitter& emitter,
        size_t num_partitions,
        const ReduceStageConfig& config = ReduceStageConfig(),
        bool immediate_flush = false,
        const Key& sentinel = ProbingTableTraits<Key>::Sentinel(),
        const IndexFunction& index_function = IndexFunction(),
        const EqualToFunction& equal_to_function = EqualToFunction())
        : Super(ctx,
                key_extractor, reduce_function, emitter,
                num_partitions, config, immediate_flush,
                sentinel, index_function, equal_to_function) {

        assert(num_partitions > 0);

        // calculate num_buckets_per_partition_ from the memory limit and the
        // number of partitions required

        assert(limit_memory_bytes_ >= 0 &&
               "limit_memory_bytes must be greater than or equal to 0. "
               "A byte size of zero results in exactly one item per partition");

        num_buckets_per_partition_ = std::max<size_t>(
            1,
            (size_t)(limit_memory_bytes_
                     / static_cast<double>(sizeof(KeyValuePair))
                     / static_cast<double>(num_partitions_)));

        num_buckets_ = num_buckets_per_partition_ * num_partitions_;

        assert(num_buckets_per_partition_ > 0);
        assert(num_buckets_ > 0);

        // calculate limit on the number of items in a partition before these
        // are spilled to disk or flushed to network.

        double limit_fill_rate = config.limit_partition_fill_rate();

        assert(limit_fill_rate >= 0.0 && limit_fill_rate <= 1.0
               && "limit_partition_fill_rate must be between 0.0 and 1.0. "
               "with a fill rate of 0.0, items are immediately flushed.");

        limit_items_per_partition_ =
            (size_t)(num_buckets_per_partition_ * limit_fill_rate);

        assert(limit_items_per_partition_ >= 0);

        // construct the hash table itself. fill it with sentinels

        items_.resize(num_buckets_, sentinel_);
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
            kv.first, num_partitions_,
            num_buckets_per_partition_, num_buckets_);

        assert(h.partition_id < num_partitions_);
        assert(h.global_index < num_buckets_);

        KeyValuePair* initial = &items_[h.global_index];
        KeyValuePair* current = initial;
        KeyValuePair* last_item =
            &items_[(h.partition_id + 1) * num_buckets_per_partition_ - 1];

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
                current -= (num_buckets_per_partition_ - 1);
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

        while (items_per_partition_[h.partition_id] > limit_items_per_partition_)
        {
            SpillPartition(h.partition_id);
        }
    }

    //! \name Spilling Mechanisms to External Memory Files
    //! \{

    //! Spill all items of a partition into an external memory File.
    void SpillPartition(size_t partition_id) {

        if (immediate_flush_)
            return FlushPartition(partition_id, true);

        LOG << "Spilling " << items_per_partition_[partition_id]
            << " items of partition with id: " << partition_id;

        if (items_per_partition_[partition_id] == 0)
            return;

        data::File::Writer writer = partition_files_[partition_id].GetWriter();

        for (size_t i = partition_id * num_buckets_per_partition_;
             i < (partition_id + 1) * num_buckets_per_partition_; i++)
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

    //! \name Flushing Mechanisms to Next Stage
    //! \{

    template <typename Emit>
    void FlushPartitionEmit(size_t partition_id, bool consume, Emit emit) {

        LOG << "Flushing " << items_per_partition_[partition_id]
            << " items of partition: " << partition_id;

        size_t begin = partition_id * num_buckets_per_partition_;
        size_t end = (partition_id + 1) * num_buckets_per_partition_;

        for (size_t i = begin; i != end; ++i)
        {
            KeyValuePair& current = items_[i];

            if (current.first != sentinel_.first) {
                emit(partition_id, current);

                if (consume)
                    items_[i] = sentinel_;
            }
        }

        if (consume) {
            // reset partition specific counter
            items_per_partition_[partition_id] = 0;
        }

        LOG << "Done flushed items of partition: " << partition_id;
    }

    void FlushPartition(size_t partition_id, bool consume) {
        FlushPartitionEmit(
            partition_id, consume,
            [this](const size_t& partition_id, const KeyValuePair& p) {
                this->emitter_.Emit(partition_id, p);
            });
    }

    void FlushAll() {
        for (size_t i = 0; i < num_partitions_; ++i) {
            FlushPartition(i, true);
        }
    }

    //! \}

private:
    using Super::equal_to_function_;
    using Super::immediate_flush_;
    using Super::index_function_;
    using Super::items_per_partition_;
    using Super::key_extractor_;
    using Super::limit_items_per_partition_;
    using Super::limit_memory_bytes_;
    using Super::num_buckets_;
    using Super::num_buckets_per_partition_;
    using Super::num_partitions_;
    using Super::partition_files_;
    using Super::reduce_function_;
    using Super::sentinel_;

    //! Storing the actual hash table.
    std::vector<KeyValuePair> items_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PROBING_HASH_TABLE_HEADER

/******************************************************************************/
