/*******************************************************************************
 * thrill/core/reduce_pre_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PRE_TABLE_HEADER
#define THRILL_CORE_REDUCE_PRE_TABLE_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/core/bucket_block_pool.hpp>
#include <thrill/core/pre_bucket_reduce_by_hash_key.hpp>
#include <thrill/core/pre_bucket_reduce_by_index.hpp>
#include <thrill/core/pre_bucket_reduce_flush_to_index.hpp>
#include <thrill/core/post_bucket_reduce_flush.hpp>


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

/**
 *
 * A data structure which takes an arbitrary value and extracts a key using
 * a key extractor function from that value. A key may also be provided initially
 * as part of a key/value pair, not requiring to extract a key.
 *
 * Afterwards, the key is hashed and the hash is used to assign that key/value pair
 * to some bucket. A bucket can have one or more slots to store items. There are
 * max_num_items_per_table_per_bucket slots in each bucket.
 *
 * In case a slot already has a key/value pair and the key of that value and the key of
 * the value to be inserted are them same, the values are reduced according to
 * some reduce function. No key/value is added to the current bucket.
 *
 * If the keys are different, the next slot (moving down) is considered. If the
 * slot is occupied, the same procedure happens again. This prociedure may be considered
 * as linear probing within the scope of a bucket.
 *
 * Finally, the key/value pair to be inserted may either:
 *
 * 1.) Be reduced with some other key/value pair, sharing the same key.
 * 2.) Inserted at a free slot in the bucket.
 * 3.) Trigger a resize of the data structure in case there are no more free slots
 *     in the bucket.
 *
 * The following illustrations shows the general structure of the data structure.
 * There are several buckets containing one or more slots. Each slot may store a item.
 * In order to optimize I/O, slots are organized in bucket blocks. Bucket blocks are
 * connected by pointers. Key/value pairs are directly stored in a bucket block, no
 * pointers are required here.
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
template <bool, typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl;

template <typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl<true, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id](p.second);
    }
};

template <typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl<false, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id](p);
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename FlushFunction = PostBucketReduceFlush<Key, Value, ReduceFunction>,
          typename IndexFunction = PreBucketReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          size_t TargetBlockSize = 16*16,
          const bool FullPreReduce = false>
class ReducePreTable
{
    static const bool debug = false;

    static const bool bench = true;

    static const bool emit = false;

    static const size_t flush_mode = 4; // 0... 1-factor, 1... fullest, 2... LRU, 3... LFU, 4... random

public:
    using KeyValuePair = std::pair<Key, Value>;

    using Emitters = std::vector<data::DynBlockWriter>;

    PreBucketEmitImpl<RobustKey, Emitters, KeyValuePair> emit_impl_;

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
        void destroy_items() {
            for (KeyValuePair* i = items; i != items + size; ++i) {
                i->~KeyValuePair();
            }
        }
    };

    /**
     * A data structure which takes an arbitrary value and extracts a key using a key extractor
     * function from that value. Afterwards, the value is hashed based on the key into some slot.
     *
     * \param num_partitions The number of partitions.
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param byte_size Maximal size of the table in byte. In case size of table exceeds that value, items
     *                  are flushed.
     * \param bucket_rate Ratio of number of blocks to number of buckets in the table.
     * \param max_partition_fill_rate Maximal number of blocks per partition relative to number of slots allowed
     *                                to be filled. It the rate is exceeded, items get flushed.
     * \param index_function Function to be used for computing the bucket the item to be inserted.
     * \param equal_to_function Function for checking equality fo two keys.
     */
    ReducePreTable(Context& ctx,
                   size_t num_partitions,
                   KeyExtractor key_extractor,
                   ReduceFunction reduce_function,
                   std::vector<data::DynBlockWriter>& emit,
                   const IndexFunction& index_function,
                   const FlushFunction& flush_function,
                   const Value& neutral_element = Value(),
                   size_t byte_size = 1024 * 16,
                   double bucket_rate = 1.0,
                   double max_partition_fill_rate = 0.5,
                   const EqualToFunction& equal_to_function = EqualToFunction(),
                   double table_rate_multiplier = 1.05)
        : ctx_(ctx),
          num_partitions_(num_partitions),
          max_partition_fill_rate_(max_partition_fill_rate),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit),
          byte_size_(byte_size),
          index_function_(index_function),
          flush_function_(flush_function),
          equal_to_function_(equal_to_function),
          neutral_element_(neutral_element)
    {
        sLOG << "creating ReducePreTable with" << emit_.size() << "output emitters";

        assert(num_partitions > 0);
        assert(num_partitions == emit.size());
        assert(byte_size >= 0 && "byte_size must be greater than or equal to 0. "
                "a byte size of zero results in exactly one item per partition");
        assert(max_partition_fill_rate >= 0.0 && max_partition_fill_rate <= 1.0 && "max_partition_fill_rate "
                "must be between 0.0 and 1.0. with a fill rate of 0.0, items are immediately flushed.");
        assert(bucket_rate >= 0.0 && "bucket_rate must be greater than or equal 0. "
                                             "a bucket rate of 0.0 causes exacty 1 bucket per partition.");

        table_rate_ = table_rate_multiplier * (1.0 / static_cast<double>(num_partitions_));

        max_num_blocks_per_partition_ =
                std::max<size_t>((size_t)(((byte_size_ * (1 - table_rate_)) / num_partitions_)
                                          / static_cast<double>(sizeof(BucketBlock))), 1);

        max_num_items_per_partition_ = max_num_blocks_per_partition_ * block_size_;

        fill_rate_num_items_per_partition_ = (size_t)(static_cast<double>(max_num_items_per_partition_)
                                                      * max_partition_fill_rate_);

        num_buckets_per_partition_ =
            std::max<size_t>((size_t)(static_cast<double>(max_num_blocks_per_partition_)
                                      * bucket_rate), 1);

        // reduce max number of blocks per partition to cope for the memory needed for pointers
        max_num_blocks_per_partition_ -=
            std::max<size_t>((size_t)(std::ceil(
                                          static_cast<double>(num_buckets_per_partition_ * sizeof(BucketBlock*))
                                          / static_cast<double>(sizeof(BucketBlock)))), 0);

        max_num_blocks_per_partition_ = std::max<size_t>(max_num_blocks_per_partition_, 1);

        num_buckets_per_table_ = num_buckets_per_partition_ * num_partitions_;
        max_num_blocks_per_table_ = max_num_blocks_per_partition_ * num_partitions_;

        assert(max_num_blocks_per_partition_ > 0);
        assert(max_num_items_per_partition_ > 0);
        assert(fill_rate_num_items_per_partition_ >= 0);
        assert(num_buckets_per_partition_ > 0);
        assert(num_buckets_per_table_ > 0);
        assert(max_num_blocks_per_table_ > 0);

        buckets_.resize(num_buckets_per_table_, nullptr);
        buckets_length_.resize(num_buckets_per_table_, 0);

        num_items_per_partition_.resize(num_partitions_, 0);

        total_items_per_partition_.resize(num_partitions, 0);

        for (size_t i = 0; i < num_partitions_; i++) {
            partition_files_.push_back(ctx.GetFile());
        }
        for (size_t i = 0; i < num_partitions_; i++) {
            partition_writers_.push_back(partition_files_[i].GetWriter());
        }

        // set up second table
        max_num_blocks_second_reduce_ = std::max<size_t>((size_t)((byte_size_ * table_rate_)
                                                                  / static_cast<double>(sizeof(BucketBlock))), 0);

        max_num_blocks_second_reduce_ = std::max<size_t>(max_num_blocks_second_reduce_, 1);

        max_num_items_second_reduce_ = max_num_blocks_second_reduce_ * block_size_;

        fill_rate_num_items_second_reduce_ = (size_t)(max_num_items_second_reduce_ * max_partition_fill_rate_);

        second_table_size_ = std::max<size_t>((size_t)(static_cast<double>(max_num_blocks_second_reduce_)
                                                       * bucket_rate), 1);

        // ensure size of second table is even, in order to be able to split by half for spilling
        if (second_table_size_ % 2 != 0) {
            second_table_size_--;
        }
        second_table_size_ = std::max<size_t>(2, second_table_size_);
        // reduce max number of blocks of second table to cope for the memory needed for pointers
        max_num_blocks_second_reduce_ -= std::max<size_t>((size_t)(std::ceil(
                static_cast<double>(second_table_size_ * sizeof(BucketBlock*))
                / static_cast<double>(sizeof(BucketBlock)))), 0);
        max_num_blocks_second_reduce_ = std::max<size_t>(max_num_blocks_second_reduce_, 1);

        assert(max_num_blocks_second_reduce_ > 0);
        assert(max_num_items_second_reduce_ > 0);
        assert(fill_rate_num_items_second_reduce_ >= 0);
        assert(second_table_size_ > 0);

        second_table_.resize(second_table_size_, nullptr);

        for (size_t i = 0; i < emit_.size(); i++)
            emit_stats_.push_back(0);

        frame_sequence_.resize(num_partitions_, 0);
        if (flush_mode == 0)
        {
            ComputeOneFactor(num_partitions_, ctx_.my_rank());
        }
        else if (flush_mode == 4)
        {
            size_t idx = 0;
            for (size_t i=0; i<num_partitions_; i++)
            {
                if (i != ctx_.my_rank()) {
                    frame_sequence_[idx++] = i;
                }
            }
            std::random_shuffle(frame_sequence_.begin(), frame_sequence_.end()-1);
            frame_sequence_[num_partitions_-1] = ctx_.my_rank();
        }
    }

    ReducePreTable(Context& ctx, size_t num_partitions, KeyExtractor key_extractor,
            ReduceFunction reduce_function, std::vector<data::DynBlockWriter>& emit)
    : ReducePreTable(ctx, num_partitions, key_extractor, reduce_function, emit, IndexFunction(),
            FlushFunction(reduce_function)) { }

    //! non-copyable: delete copy-constructor
    ReducePreTable(const ReducePreTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreTable& operator = (const ReducePreTable&) = delete;

    ~ReducePreTable() {
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
        block_pool.Destroy();
    }

    /*!
     * Inserts a value. Calls the key_extractor_, makes a key-value-pair and
     * inserts the pair into the hashtable.
     */
    void Insert(const Value& p) {
        Insert(std::make_pair(key_extractor_(p), p));
    }

    /*!
     * Inserts a value into the table, potentially reducing it in case both the key of the value
     * already in the table and the key of the value to be inserted are the same.
     *
     * An insert may trigger a partial flush of the partition with the most items if the maximal
     * number of items in the table (max_num_items_per_table_table) is reached.
     *
     * Alternatively, it may trigger a resize of table in case maximal number of items per
     * bucket is reached.
     *
     * \param p Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {

        typename IndexFunction::IndexResult h = index_function_(kv.first, this);

        assert(h.partition_id >= 0 && h.partition_id < num_partitions_);
        assert(h.global_index >= 0 && h.global_index < num_buckets_per_table_);

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

                if (bench) {
                    num_collisions_++;
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
            if (num_blocks_per_table_ == max_num_blocks_per_table_)
            {
                if (FullPreReduce) {
                    SpillPartition(h.partition_id);
                } else {
                    FlushPartition(h.partition_id);
                }
            }

            // allocate a new block of uninitialized items, prepend to bucket
            current = block_pool.GetBlock();
            current->next = buckets_[h.global_index];
            buckets_[h.global_index] = current;

            // Total number of blocks
            num_blocks_per_table_++;

            if (bench) {
                buckets_length_[h.global_index]++;
            }
        }

        // in-place construct/insert new item in current bucket block
        new (current->items + current->size++)KeyValuePair(kv);

        // Increase partition item count
        num_items_per_partition_[h.partition_id]++;

        // flush current partition if max partition fill rate reached
        if (num_items_per_partition_[h.partition_id] > fill_rate_num_items_per_partition_)
        {
            if (FullPreReduce) {
                SpillPartition(h.partition_id);
            } else {
                FlushPartition(h.partition_id);
            }
        }
    }

    /*!
     * Spills all items of a partition.
     *
     * \param partition_id The id of the partition to be flushed.
     */
    void SpillPartition(size_t partition_id) {

        data::File::Writer& writer = partition_writers_[partition_id];

        for (size_t i = partition_id * num_buckets_per_partition_;
             i < (partition_id + 1) * num_buckets_per_partition_; i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != nullptr)
            {
           
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    writer.PutItem(*bi);
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                block_pool.Deallocate(current);
                current = next;
            }

            if (bench) {
                if (buckets_[i] != nullptr) {
                    buckets_length_[i] = 0;
                }
            }

            buckets_[i] = nullptr;
        }

        if (flush_mode == 1)
        {
            total_items_per_partition_[partition_id] += num_items_per_partition_[partition_id];
        }

        // reset partition specific counter
        num_items_per_partition_[partition_id] = 0;

        if (bench) {
            // debug: increase flush counter
            num_spills_++;
        }
    }

    /*!
     * Flush.
     */
    void Flush(bool consume = true) {

        if (flush_mode == 1) {
            size_t idx = 0;
            for (size_t i = 0; i != num_partitions_; i++) {
                if (i != ctx_.my_rank())
                    frame_sequence_[idx++] = i;
            }

            if (FullPreReduce) {
                std::vector<size_t> sum_items_per_partition_;
                sum_items_per_partition_.resize(num_partitions_, 0);
                for (size_t i = 0; i != num_partitions_; ++i) {
                    sum_items_per_partition_[i] += num_items_per_partition_[i];
                    sum_items_per_partition_[i] += total_items_per_partition_[i];
                    if (consume)
                        total_items_per_partition_[i] = 0;
                }
                std::sort(frame_sequence_.begin(), frame_sequence_.end() - 1,
                          [&](size_t i1, size_t i2) {
                              return sum_items_per_partition_[i1] < sum_items_per_partition_[i2];
                          });

            } else {
                std::sort(frame_sequence_.begin(), frame_sequence_.end() - 1,
                          [&](size_t i1, size_t i2) {
                              return num_items_per_partition_[i1] < num_items_per_partition_[i2];
                          });
            }

            frame_sequence_[num_partitions_-1] = ctx_.my_rank();
        }

        if (FullPreReduce) {
            flush_function_(consume, this);

        } else {

            for (size_t i : frame_sequence_)
            {
                FlushPartition(i);
            }
        }
    }

    /*!
     * Retrieves all items belonging to the partition
     * having the most items. Retrieved items are then pushed
     * to the provided emitter.
     */
    void FlushLargestPartition() {
        LOG << "Flushing items of largest partition";

        // get partition with max size
        size_t p_size_max = 0;
        size_t p_idx = 0;
        for (size_t i = 0; i < num_partitions_; i++)
        {
            if (num_items_per_partition_[i] > p_size_max)
            {
                p_size_max = num_items_per_partition_[i];
                p_idx = i;
            }
        }

        LOG << "currMax: "
            << p_size_max
            << " currentIdx: "
            << p_idx
            << " currentIdx*p_size: "
            << p_idx * num_buckets_per_partition_
            << " CurrentIdx*p_size+p_size-1 "
            << p_idx * num_buckets_per_partition_ + num_buckets_per_partition_ - 1;

        LOG << "Largest patition id: "
            << p_idx;

        if (p_size_max == 0) {
            return;
        }

        FlushPartition(p_idx);

        LOG << "Flushed items of largest partition";
    }

    /*!
     * Flushes all items of a partition.
     *
     * \param partition_id The id of the partition to be flushed.
     */
    void FlushPartition(size_t partition_id) {

        LOG << "Flushing items of partition with id: "
            << partition_id;
        
        for (size_t i = partition_id * num_buckets_per_partition_;
             i < (partition_id + 1) * num_buckets_per_partition_; i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != nullptr)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    if (emit) {
                        EmitAll(*bi, partition_id);
                    }
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                block_pool.Deallocate(current);
                current = next;
            }

            buckets_[i] = nullptr;

            if (bench) {
                buckets_length_[i] = 0;
            }
        }

        if (flush_mode == 1)
        {
            total_items_per_partition_[partition_id] -= num_items_per_partition_[partition_id];
        }

        // reset partition specific counter
        num_items_per_partition_[partition_id] = 0;
        // flush elements pushed into emitter
        emit_[partition_id].Flush();

        if (bench) {
            // debug: increase flush counter
            num_flushes_++;
        }

        LOG << "Flushed items of partition with id: " << partition_id;
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const KeyValuePair& p, const size_t& partition_id) {
        emit_stats_[partition_id]++;
        emit_impl_.EmitElement(p, partition_id, emit_);
    }

    /*!
     * Returns the total num of buckets in the table.
     *
     * \return Number of buckets in the table.
     */
    size_t NumBucketsPerTable() const {
        return num_buckets_per_table_;
    }

    /*!
     * Sets the num of blocks in the table.
     */
    void SetNumBlocksPerTable(const size_t num_blocks) {
        num_blocks_per_table_ = num_blocks;
    }

    /*!
     * Returns the total num of items in the table.
     *
     * \return Number of items in the table.
     */
    size_t NumItemsPerTable() const {
        size_t total_num_items = 0;
        for (size_t num_items : num_items_per_partition_) {
            total_num_items += num_items;
        }

        return total_num_items;
    }

    /*!
     * Returns the number of buckets per partition.
     *
     * \return Number of buckets per partition.
     */
    size_t NumBucketsPerFrame() const {
        return num_buckets_per_partition_;
    }

    /*!
     * Returns the number of partitions.
     *
     * \return The number of partitions.
     */
    size_t NumFrames() const {
        return num_partitions_;
    }

    /*!
     * Returns the number of flushes.
     *
     * \return Number of flushes.
     */
    size_t NumFlushes() const {
        return num_flushes_;
    }

    /*!
     * Returns the number of collisions.
     *
     * \return Number of collisions.
     */
    size_t NumCollisions() const {
        return num_collisions_;
    }

    /*!
     * Returns the vector of bucket blocks.
     *
     * \return Vector of bucket blocks.
     */
    std::vector<BucketBlock*> & Items() {
        return buckets_;
    }

    /*!
     * Returns the vector of frame files.
     *
     * \return Vector of frame files.
     */
    std::vector<data::File> & FrameFiles() {
        return partition_files_;
    }

    /*!
     * Returns the vector of frame writers.
     *
     * \return Vector of frame writers.
     */
    std::vector<data::File::Writer> & FrameWriters() {
        return partition_writers_;
    }

    /*!
     * Returns the number of items of a partition.
     *
     * \param partition_id The id of the partition the number of
     *                  blocks to be returned..
     * \return The number of items in the partitions.
     */
    size_t NumItemsPerPartition(size_t partition_id) {
        return num_items_per_partition_[partition_id];
    }

    /*!
     * Returns the vector of number of items per frame in internal memory.
     *
     * \return Vector of number of items per frame in internal memory.
     */
    std::vector<size_t> & NumItemsMemPerFrame() {
        return num_items_per_partition_;
    }

    /*!
     * Returns the number of spills.
     *
     * \return Number of spills.
     */
    size_t NumSpills() const {
        return num_spills_;
    }

    /*!
     * Returns the block size.
     *
     * \return Block size.
     */
    double BlockSize() const {
        return block_size_;
    }

    /*!
     * Returns the block size.
     *
     * \return Block size.
     */
    BucketBlockPool<BucketBlock> & BlockPool() {
        return block_pool;
    }

    /*!
     * Returns the vector of key/value pairs.
     *
     * \return Vector of key/value pairs.
     */
    std::vector<BucketBlock*> & SecondTable() {
        return second_table_;
    }

    /*!
     * Returns the vector of key/value pairs.s
     *
     * \return Vector of key/value pairs.
     */
    Context& Ctx() {
        return ctx_;
    }

    /*!
     * Returns the number of spills.
     *
     * \return Number of spills.
     */
    size_t MaxNumItemsSecondReduce() const {
        return max_num_items_second_reduce_;
    }

    /*!
     * Returns the number of spills.
     *
     * \return Number of spills.
     */
    size_t MaxNumBlocksSecondReduce() const {
        return max_num_blocks_second_reduce_;
    }

    /*!
     * Returns the number of block in the table.
     *
     * \return Number of blocks in the table.
     */
    size_t NumBlocksPerTable() const {
        return num_blocks_per_table_;
    }

    /*!
    * Returns the neutral element.
    *
    * \return Neutral element.
    */
    Value NeutralElement() const {
        return neutral_element_;
    }

    void incrRecursiveSpills() {
        num_recursive_spills_++;
    }

    size_t RecursiveSpills() {
        return num_recursive_spills_;
    }

    double BucketLengthMedian() {
        double sum = std::accumulate(buckets_length_.begin(), buckets_length_.end(), 0.0);
        return sum / buckets_length_.size();
    }

    double BucketLengthStdv() {
        double sum = std::accumulate(buckets_length_.begin(), buckets_length_.end(), 0.0);
        double mean = sum / buckets_length_.size();
        double sq_sum = std::inner_product(buckets_length_.begin(), buckets_length_.end(), buckets_length_.begin(), 0.0);
        return std::sqrt(sq_sum / buckets_length_.size() - mean * mean);
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

    /*!
     * Computes the one 1-factor sequence
     */
    void ComputeOneFactor(const size_t& p_raw,
                          const size_t& j)
    {
        assert(p_raw > 0);
        assert(j >= 0);
        assert(j < p_raw);

        size_t p = (p_raw % 2 == 0) ? p_raw-1 : p_raw;
        size_t p_i[p];

        for (size_t i=0; i<p; i++) {
            if (i == 0) {
                p_i[i] = 0;
                continue;
            }
            p_i[i] = p-i;
        }

        size_t a = 0;
        for (size_t i=0; i<p; i++) {
            if (p != p_raw && j == p) {
                frame_sequence_[i] = ((p_raw/2)*i) % (p_raw-1);
                continue;
            }

            int idx = j-i;
            if (idx < 0) {
                idx = p+(j-i);
            }
            if (p_i[idx] == j) {
                if (p == p_raw) {
                    continue;
                } else {
                    frame_sequence_[a++] = p;
                    continue;
                }
            }
            frame_sequence_[a++] = p_i[idx];
        }

        frame_sequence_[p_raw-1] = j;
    }

    /*!
     * Returns the sequence of frame ids to
     * be processed on flush.
     */
    std::vector<size_t>& FrameSequence() {
        return frame_sequence_;
    }

private:
    //! Context
    Context& ctx_;

    //! Number of partitions
    size_t num_partitions_ = 1;

    //! Number of buckets per partition.
    size_t num_buckets_per_partition_ = 0;

    // Fill rate for partition.
    double max_partition_fill_rate_ = 1.0;

    //! Maximal number of blocks.
    size_t max_num_blocks_per_table_ = 0;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& emit_;

    //! Size of the table in bytes
    size_t byte_size_ = 0;

    //! Bucket rate.
    double bucket_rate_ = 0.0;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Flush function.
    FlushFunction flush_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Number of buckets in teh table.
    size_t num_buckets_per_table_ = 0;

    //! Total number of blocks in the table.
    size_t num_blocks_per_table_ = 0;

    //! Number of blocks per partition.
    std::vector<size_t> num_items_per_partition_;

    //! Number of items per partition.
    std::vector<size_t> total_items_per_partition_;

    //! Emitter stats.
    std::vector<size_t> emit_stats_;

    //! Storing the items.
    std::vector<BucketBlock*> buckets_;

    //! Store the files for frames.
    std::vector<data::File> partition_files_;

    //! Store the writers for frames.
    std::vector<data::File::Writer> partition_writers_;

    //! Maximal number of items per partition.
    size_t max_num_items_per_partition_ = 0;

    //! Maximal number of blocks per partition.
    size_t max_num_blocks_per_partition_ = 0;

    //! Number of flushes.
    size_t num_flushes_ = 0;

    //! Number of collisions.
    size_t num_collisions_ = 0;

    //! Number of spills.
    size_t num_spills_ = 0;

    //! Bucket block pool.
    BucketBlockPool<BucketBlock> block_pool;

    //! Number of items per partition considering fill rate.
    size_t fill_rate_num_items_per_partition_ = 0;

    //! Rate of sizes of primary to secondary table.
    double table_rate_ = 0.0;

    //! Storing the secondary table.
    std::vector<BucketBlock*> second_table_;

    size_t max_num_items_second_reduce_;

    size_t second_table_size_;

    size_t max_num_blocks_second_reduce_;

    size_t fill_rate_num_items_second_reduce_;

    //! Neutral element (reduce to index).
    Value neutral_element_;

    //! Frame Sequence.
    std::vector<size_t> frame_sequence_;

    //! Number of recursive spills.
    size_t num_recursive_spills_ = 0;

    //! Storing the items.
    std::vector<size_t> buckets_length_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_TABLE_HEADER

/******************************************************************************/
