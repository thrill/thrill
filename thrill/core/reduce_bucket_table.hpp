/*******************************************************************************
 * thrill/core/reduce_bucket_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_BUCKET_TABLE_HEADER
#define THRILL_CORE_REDUCE_BUCKET_TABLE_HEADER

#include <thrill/core/bucket_block_pool.hpp>

namespace thrill {
namespace core {

/**
 * A data structure which takes an arbitrary value and extracts a key using a
 * key extractor function from that value. A key may also be provided initially
 * as part of a key/value pair, not requiring to extract a key.
 *
 * Afterwards, the key is hashed and the hash is used to assign that key/value
 * pair to some bucket. A bucket can have one or more slots to store
 * items. There are max_num_items_per_table_per_bucket slots in each bucket.
 *
 * In case a slot already has a key/value pair and the key of that value and the
 * key of the value to be inserted are them same, the values are reduced
 * according to some reduce function. No key/value is added to the current
 * bucket.
 *
 * If the keys are different, the next slot (moving down) is considered. If the
 * slot is occupied, the same procedure happens again. This prociedure may be
 * considered as linear probing within the scope of a bucket.
 *
 * Finally, the key/value pair to be inserted may either:
 *
 * 1.) Be reduced with some other key/value pair, sharing the same key.
 * 2.) Inserted at a free slot in the bucket.
 * 3.) Trigger a resize of the data structure in case there are no more free
 *     slots in the bucket.
 *
 * The following illustrations shows the general structure of the data
 * structure.  There are several buckets containing one or more slots. Each slot
 * may store a item.  In order to optimize I/O, slots are organized in bucket
 * blocks. Bucket blocks are connected by pointers. Key/value pairs are directly
 * stored in a bucket block, no pointers are required here.
 *
 *
 *     Partition 0 Partition 1 Partition 2 Partition 3 Partition 4
 *     B00 B01 B02 B10 B11 B12 B20 B21 B22 B30 B31 B32 B40 B41 B42
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *    ||  |   |   ||  |   |   ||  |   |   ||  |   |   ||  |   |  ||
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *      |   |   |   |   |   |   |   |   |   |   |   |   |   |   |
 *      V   V   V   V   V   V   V   V   V   V   V   V   V   V   >
 *    +---+       +---+
 *    |   |       |   |
 *    +---+       +---+         ...
 *    |   |       |   |
 *    +---+       +---+
 *      |           |
 *      V           V
 *    +---+       +---+
 *    |   |       |   |
 *    +---+       +---+         ...
 *    |   |       |   |
 *    +---+       +---+
 *
 */
template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey,
          typename IndexFunction,
          typename EqualToFunction,
          size_t TargetBlockSize,
          typename SubTable>
class ReduceBucketTable
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    //! calculate number of items such that each BucketBlock has about 1 MiB of
    //! size, or at least 8 items.
    static constexpr size_t block_size_ =
        common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));

    //! Block holding reduce key/value pairs.
    struct BucketBlock {
        //! number of _used_/constructed items in this block. next is unused if
        //! size != block_size.
        size_t       size;

        //! link of linked list to next block
        BucketBlock  * next;

        //! memory area of items
        KeyValuePair items[block_size_]; // NOLINT

        //! helper to destroy all allocated items
        void         destroy_items() {
            for (KeyValuePair* i = items; i != items + size; ++i) {
                i->~KeyValuePair();
            }
        }
    };

    ReduceBucketTable(
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
          num_items_per_partition_(num_partitions_, 0) {
        assert(num_partitions > 0);
    }

    //! non-copyable: delete copy-constructor
    ReduceBucketTable(const ReduceBucketTable&) = delete;
    //! non-copyable: delete assignment operator
    ReduceBucketTable& operator = (const ReduceBucketTable&) = delete;

    ~ReduceBucketTable() {
        // destroy all block chains
        for (BucketBlock* b_block : buckets_)
        {
            BucketBlock* current = b_block;
            while (current != nullptr)
            {
                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }
        }
        block_pool_.Destroy();
    }

    /*!
     * Inserts a value. Calls the key_extractor_, makes a key-value-pair and
     * inserts the pair into the hashtable.
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
     * items if the maximal number of items in the table
     * (max_items_per_table_table) is reached.
     *
     * Alternatively, it may trigger a resize of table in case maximal number of
     * items per bucket is reached.
     *
     * \param p Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {

        typename IndexFunction::IndexResult h = index_function_(
            kv.first, num_partitions_,
            num_buckets_per_partition_, num_buckets_, 0);

        assert(h.partition_id < num_partitions_);
        assert(h.global_index < num_buckets_);

        LOG << "key: " << kv.first << " to bucket id: " << h.global_index;

        BucketBlock* current = buckets_[h.global_index];

        while (current != nullptr)
        {
            // iterate over valid items in a block
            for (KeyValuePair* bi = current->items;
                 bi != current->items + current->size; ++bi)
            {
                // if item and key equals, then reduce.
                if (equal_to_function_(kv.first, bi->first))
                {
                    LOG << "match of key: " << kv.first
                        << " and " << bi->first << " ... reducing...";

                    bi->second = reduce_function_(bi->second, kv.second);

                    LOG << "...finished reduce!";
                    return;
                }
            }

            current = current->next;
        }

        //////
        // have an item that needs to be added.
        //////

        current = buckets_[h.global_index];

        if (current == nullptr || current->size == block_size_)
        {
            //////
            // new block needed.
            //////

            // flush largest partition if max number of blocks reached
            if (num_blocks_ == limit_blocks_)
            {
                SpillAnyPartition(h.partition_id);
            }

            // allocate a new block of uninitialized items, prepend to bucket
            current = block_pool_.GetBlock();
            current->next = buckets_[h.global_index];
            buckets_[h.global_index] = current;

            // Total number of blocks
            num_blocks_++;
        }

        // in-place construct/insert new item in current bucket block
        new (current->items + current->size++)KeyValuePair(kv);

        sLOG << "num_items_per_partition_.size()" << num_items_per_partition_.size();
        sLOG << "h.partition_id" << h.partition_id;

        // Increase partition item count
        num_items_per_partition_[h.partition_id]++;

        // flush current partition if max partition fill rate reached
        if (num_items_per_partition_[h.partition_id] > limit_items_per_partition_)
        {
            SpillPartition(h.partition_id);
        }
    }

    void SpillAnyPartition(size_t current_id) {
        SubTable& t = *static_cast<SubTable*>(this);
        t.SpillAnyPartition(current_id);
    }

    void SpillPartition(size_t partition_id) {
        SubTable& t = *static_cast<SubTable*>(this);
        t.SpillPartition(partition_id);
    }

protected:
    //! Storing the items.
    std::vector<BucketBlock*> buckets_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Bucket block pool.
    BucketBlockPool<BucketBlock> block_pool_;

    //! \name Fixed Operational Parameters
    //! \{

    //! Number of partitions
    size_t num_partitions_;

    //! Number of buckets in the table.
    size_t num_buckets_;

    //! Number of buckets per partition.
    size_t num_buckets_per_partition_;

    //! Number of blocks in the table before some items are spilled.
    size_t limit_blocks_;

    //! Maximal number of items per partition.
    size_t max_items_per_partition_;

    //! Maximal number of blocks per partition.
    size_t max_blocks_per_partition_;

    //! Number of items in a partition before the partition is spilled.
    size_t limit_items_per_partition_;

    //! \}

    //! \name Current Statistical Parameters
    //! \{

    //! Total number of blocks in the table.
    size_t num_blocks_ = 0;

    //! Number of blocks per partition.
    std::vector<size_t> num_items_per_partition_;

    //! \}
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_BUCKET_TABLE_HEADER

/******************************************************************************/
