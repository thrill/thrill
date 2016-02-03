/*******************************************************************************
 * thrill/core/reduce_bucket_hash_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_BUCKET_HASH_TABLE_HEADER
#define THRILL_CORE_REDUCE_BUCKET_HASH_TABLE_HEADER

#include <thrill/core/bucket_block_pool.hpp>
#include <thrill/core/reduce_hash_table.hpp>

#include <algorithm>
#include <limits>
#include <utility>
#include <vector>

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
          size_t TargetBlockSize>
class ReduceBucketHashTable
    : public ReduceHashTable<ValueType, Key, Value,
                             KeyExtractor, ReduceFunction,
                             RobustKey, IndexFunction, EqualToFunction>
{
    static const bool debug = false;

    using Super = ReduceHashTable<ValueType, Key, Value,
                                  KeyExtractor, ReduceFunction,
                                  RobustKey, IndexFunction, EqualToFunction>;

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

    ReduceBucketHashTable(
        Context& ctx,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        const IndexFunction& index_function,
        const EqualToFunction& equal_to_function,
        size_t num_partitions,
        size_t limit_memory_bytes,
        double limit_partition_fill_rate,
        double bucket_rate)
        : Super(ctx,
                key_extractor, reduce_function,
                index_function, equal_to_function,
                num_partitions, limit_memory_bytes) {

        assert(num_partitions > 0);

        // calculate maximum number of blocks allowed in a partition due to the
        // memory limit.

        assert(limit_memory_bytes >= 0 &&
               "limit_memory_bytes must be greater than or equal to 0. "
               "a byte size of zero results in exactly one item per partition");

        max_blocks_per_partition_ = std::max<size_t>(
            1,
            (size_t)(limit_memory_bytes_
                     / static_cast<double>(num_partitions_)
                     / static_cast<double>(sizeof(BucketBlock))));

        assert(max_blocks_per_partition_ > 0);

        // calculate limit on the number of _items_ in a partition before these
        // are spilled to disk or flushed to network.

        assert(limit_partition_fill_rate >= 0.0 && limit_partition_fill_rate <= 1.0
               && "limit_partition_fill_rate must be between 0.0 and 1.0. "
               "with a fill rate of 0.0, items are immediately flushed.");

        max_items_per_partition_ = max_blocks_per_partition_ * block_size_;

        limit_items_per_partition_ = (size_t)(
            static_cast<double>(max_items_per_partition_)
            * limit_partition_fill_rate);

        assert(max_items_per_partition_ > 0);
        assert(limit_items_per_partition_ >= 0);

        // calculate number of slots in a partition of the bucket table, i.e.,
        // the number of bucket pointers per partition

        assert(bucket_rate >= 0.0 &&
               "bucket_rate must be greater than or equal 0. "
               "a bucket rate of 0.0 causes exactly 1 bucket per partition.");

        num_buckets_per_partition_ = std::max<size_t>(
            1,
            (size_t)(static_cast<double>(max_blocks_per_partition_)
                     * bucket_rate));

        assert(num_buckets_per_partition_ > 0);

        // reduce max number of blocks per partition to cope for the memory
        // needed for pointers

        max_blocks_per_partition_ -= std::max<size_t>(
            0,
            (size_t)(std::ceil(
                         static_cast<double>(
                             num_buckets_per_partition_ * sizeof(BucketBlock*))
                         / static_cast<double>(sizeof(BucketBlock)))));

        max_blocks_per_partition_ = std::max<size_t>(max_blocks_per_partition_, 1);

        // finally, calculate number of buckets and allocate the table

        num_buckets_ = num_buckets_per_partition_ * num_partitions_;
        limit_blocks_ = max_blocks_per_partition_ * num_partitions_;

        assert(num_buckets_ > 0);
        assert(limit_blocks_ > 0);

        buckets_.resize(num_buckets_, nullptr);

        // allocate Files for each partition to spill into. TODO(tb): switch to
        // FilePtr ondemand

        for (size_t i = 0; i < num_partitions_; i++) {
            partition_files_.push_back(ctx.GetFile());
        }
    }

    //! non-copyable: delete copy-constructor
    ReduceBucketHashTable(const ReduceBucketHashTable&) = delete;
    //! non-copyable: delete assignment operator
    ReduceBucketHashTable& operator = (const ReduceBucketHashTable&) = delete;

    ~ReduceBucketHashTable() {
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
            while (num_blocks_ > limit_blocks_)
                SpillAnyPartition();

            // allocate a new block of uninitialized items, prepend to bucket
            current = block_pool_.GetBlock();
            current->next = buckets_[h.global_index];
            buckets_[h.global_index] = current;

            // Total number of blocks
            num_blocks_++;
        }

        // in-place construct/insert new item in current bucket block
        new (current->items + current->size++)KeyValuePair(kv);

        sLOG << "items_per_partition_.size()" << items_per_partition_.size();
        sLOG << "h.partition_id" << h.partition_id;

        // Increase partition item count
        items_per_partition_[h.partition_id]++;

        // flush current partition if max partition fill rate reached
        while (items_per_partition_[h.partition_id] > limit_items_per_partition_)
            SpillPartition(h.partition_id);
    }

    //! \name Spilling Mechanisms to External Memory Files
    //! \{

    //! Spill all items of an arbitrary partition into an external memory File.
    void SpillAnyPartition() {
        // maybe make a policy later -tb
        return SpillLargestPartition();
    }

    //! Spill all items of a partition into an external memory File.
    void SpillPartition(size_t partition_id) {

        data::File::Writer writer = partition_files_[partition_id].GetWriter();

        for (size_t i = partition_id * num_buckets_per_partition_;
             i < (partition_id + 1) * num_buckets_per_partition_; i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != nullptr)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    writer.Put(*bi);
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                block_pool_.Deallocate(current);
                current = next;
            }

            buckets_[i] = nullptr;
        }

        // reset partition specific counter
        items_per_partition_[partition_id] = 0;
    }

    //! Spill all items of the largest partition into an external memory File.
    void SpillLargestPartition() {
        // get partition with max size
        size_t size_max = 0, index = 0;

        for (size_t i = 0; i < num_partitions_; i++)
        {
            if (items_per_partition_[i] > size_max)
            {
                size_max = items_per_partition_[i];
                index = i;
            }
        }

        if (size_max == 0) {
            return;
        }

        return SpillPartition(index);
    }

    //! Spill all items of the smallest non-empty partition into an external
    //! memory File.
    void SpillSmallestPartition() {
        // get partition with min size
        size_t size_min = std::numeric_limits<size_t>::max(), index = 0;

        for (size_t i = 0; i < num_partitions_; i++)
        {
            if (items_per_partition_[i] < size_min
                && items_per_partition_[i] != 0)
            {
                size_min = items_per_partition_[i];
                index = i;
            }
        }

        if (size_min == 0 || size_min == std::numeric_limits<size_t>::max()) {
            return;
        }

        return SpillPartition(index);
    }

    //! \}

    //! \name Flushing Mechanisms to Next Stage
    //! \{

    template <typename Emit>
    void FlushPartitionE(size_t partition_id, bool consume, Emit emit) {
        LOG << "Flushing items of partition: " << partition_id;

        size_t begin = partition_id * num_buckets_per_partition_;
        size_t end = (partition_id + 1) * num_buckets_per_partition_;

        for (size_t i = begin; i != end; i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != nullptr)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    emit(partition_id, *bi);
                }

                if (consume) {
                    // destroy block and advance to next
                    BucketBlock* next = current->next;
                    block_pool_.Deallocate(current);
                    current = next;
                }
                else {
                    // advance to next
                    current = current->next;
                }
            }

            if (consume)
                buckets_[i] = nullptr;
        }

        if (consume) {
            // reset partition specific counter
            items_per_partition_[partition_id] = 0;
        }

        // flush elements pushed into emitter
        // emit_[partition_id].Flush();

        LOG << "Done flushing items of partition:" << partition_id;
    }

    //! \}

protected:
    using Super::equal_to_function_;
    using Super::index_function_;
    using Super::key_extractor_;
    using Super::limit_items_per_partition_;
    using Super::limit_memory_bytes_;
    using Super::items_per_partition_;
    using Super::num_partitions_;
    using Super::partition_files_;
    using Super::reduce_function_;

    //! Storing the items.
    std::vector<BucketBlock*> buckets_;

    //! Bucket block pool.
    BucketBlockPool<BucketBlock> block_pool_;

    //! \name Fixed Operational Parameters
    //! \{

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

    //! \}

    //! \name Current Statistical Parameters
    //! \{

    //! Total number of blocks in the table.
    size_t num_blocks_ = 0;

    //! \}
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_BUCKET_HASH_TABLE_HEADER

/******************************************************************************/
