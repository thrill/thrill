/*******************************************************************************
 * thrill/core/reduce_post_table.hpp
 *
 * Hash table with support for reduce.
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
#ifndef THRILL_CORE_REDUCE_POST_BUCKET_TABLE_HEADER
#define THRILL_CORE_REDUCE_POST_BUCKET_TABLE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/core/bucket_block_pool.hpp>
#include <thrill/core/post_bucket_reduce_flush_to_index.hpp>
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
#include <limits.h>
#include <stddef.h>

namespace thrill {
namespace core {

template <typename Key, typename HashFunction = std::hash<Key> >
class PostBucketReduceByHashKey
{
public:
    explicit PostBucketReduceByHashKey(const HashFunction& hash_function = HashFunction())
            : hash_function_(hash_function)
    { }

    template <typename Table>
    size_t
    operator () (const Key& k, Table* ht, const size_t& size) const {

        (void)ht;

        size_t hashed = hash_function_(k);

        return hashed % size;
    }

private:
    HashFunction hash_function_;
};


class PostBucketReduceByIndex
{
public:
    PostBucketReduceByIndex() { }

    template <typename Table>
    size_t
    operator () (const size_t& k, Table* ht, const size_t& size) const {

        return (k - ht->BeginLocalIndex()) % size;
    }
};

/**
 *
 * A data structure which takes an arbitrary value and extracts a key using
 * a key extractor function from that value. A key may also be provided initially
 * as part of a key/value pair, not requiring to extract a key.
 *
 * Afterwards, the key is hashed and the hash is used to assign that key/value pair
 * to some bucket. A bucket can have one or more slots to store items. There are
 * max_num_items_per_bucket slots in each bucket.
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
template <bool, typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostBucketEmitImpl;

template <typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostBucketEmitImpl<true, EmitterFunction, KeyValuePair, SendType>{
    void EmitElement(const KeyValuePair& p, EmitterFunction emit) {
        emit(p);
    }
};

template <typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostBucketEmitImpl<false, EmitterFunction, KeyValuePair, SendType>{
    void EmitElement(const KeyValuePair& p, EmitterFunction emit) {
        emit(p.second);
    }
};

template <typename ValueType, typename Key, typename Value, // TODO: dont need both ValueType and Value
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename FlushFunction = PostBucketReduceFlush<Key, Value, ReduceFunction>,
          typename IndexFunction = PostBucketReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          size_t TargetBlockSize = 16*16
          >
class ReducePostTable
{
    static const bool debug = false;

    static const bool bench = true;

    static const bool emit = true;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const ValueType&)>;

    PostBucketEmitImpl<SendPair, EmitterFunction, KeyValuePair, ValueType> emit_impl_;

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
     * \param context Context.
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param index_function Function to be used for computing the bucket the item to be inserted.
     * \param flush_function Function to be used for flushing all items in the table.
     * \param begin_local_index Begin index for reduce to index.
     * \param end_local_index End index for reduce to index.
     * \param neutral element Neutral element for reduce to index.
     * \param byte_size Maximal size of the table in byte. In case size of table exceeds that value, items
     *                  are flushed.
     * \param bucket_rate Ratio of number of blocks to number of buckets in the table.
     * \param max_frame_fill_rate Maximal number of items relative to maximal number of items in a frame.
     *        It the number is exceeded, no more blocks are added to a bucket, instead, items get spilled to disk.
     * \param frame_rate Rate of number of buckets to number of frames. There is one file writer per frame.
     * \param equal_to_function Function for checking equality of two keys.
     */
    ReducePostTable(Context& ctx,
                    const KeyExtractor& key_extractor,
                    const ReduceFunction& reduce_function,
                    const EmitterFunction& emit,
                    const IndexFunction& index_function,
                    const FlushFunction& flush_function,
                    size_t begin_local_index = 0,
                    size_t end_local_index = 0,
                    const Value& neutral_element = Value(),
                    size_t byte_size = 1024 * 16,
                    double bucket_rate = 1.0,
                    double max_frame_fill_rate = 0.5,
                    double frame_rate = 0.01,
                    const EqualToFunction& equal_to_function = EqualToFunction(),
                    double table_rate_multiplier = 1.1)
        : ctx_(ctx),
          max_frame_fill_rate_(max_frame_fill_rate),
          emit_(emit),
          byte_size_(byte_size),
          begin_local_index_(begin_local_index),
          end_local_index_(end_local_index),
          neutral_element_(neutral_element),
          key_extractor_(key_extractor),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          flush_function_(flush_function),
          reduce_function_(reduce_function) {

        assert(byte_size >= 0 && "byte_size must be greater than or equal to 0. "
                "a byte size of zero results in exactly one item per partition");
        assert(max_frame_fill_rate >= 0.0 && max_frame_fill_rate <= 1.0 && "max_partition_fill_rate "
                "must be between 0.0 and 1.0. with a fill rate of 0.0, items are immediately flushed.");
        assert(frame_rate > 0.0 && frame_rate <= 1.0 && "a frame rate of 1.0 causes exactly one frame.");
        assert(bucket_rate >= 0.0 && "bucket_rate must be greater than or equal 0. "
                "a bucket rate of 0.0 causes exacty 1 bucket per partition.");
        assert(begin_local_index >= 0);
        assert(end_local_index >= 0);

        num_frames_ = std::max<size_t>((size_t)(1.0 / frame_rate), 1);

        table_rate_ = table_rate_multiplier * std::min<double>(1.0 / static_cast<double>(num_frames_), 0.5);

        max_num_blocks_mem_per_frame_ =
                std::max<size_t>((size_t)(((byte_size_ * (1 - table_rate_)) / num_frames_)
                                          / static_cast<double>(sizeof(BucketBlock))), 1);

        max_num_items_mem_per_frame_ = max_num_blocks_mem_per_frame_ * block_size_;

        fill_rate_num_items_mem_per_frame_ = (size_t)(max_num_items_mem_per_frame_ * max_frame_fill_rate_);

        num_buckets_per_frame_ =
                std::max<size_t>((size_t)(static_cast<double>(max_num_blocks_mem_per_frame_)
                                          * bucket_rate), 1);

        // reduce max number of blocks per frame to cope for the memory needed for pointers
        max_num_blocks_mem_per_frame_ -= std::max<size_t>((size_t)(std::ceil(
                                                                   static_cast<double>(num_buckets_per_frame_ * sizeof(BucketBlock*))
                                                                   / static_cast<double>(sizeof(BucketBlock)))), 0);

        max_num_blocks_mem_per_frame_ = std::max<size_t>(max_num_blocks_mem_per_frame_, 1);

        num_buckets_per_table_ = num_buckets_per_frame_ * num_frames_;
        max_num_blocks_per_table_ = max_num_blocks_mem_per_frame_ * num_frames_;

        assert(num_frames_ > 0);
        assert(max_num_blocks_mem_per_frame_ > 0);
        assert(max_num_items_mem_per_frame_ > 0);
        assert(fill_rate_num_items_mem_per_frame_ >= 0);
        assert(num_buckets_per_frame_ > 0);
        assert(num_buckets_per_table_ > 0);
        assert(max_num_blocks_per_table_ > 0);

        buckets_.resize(num_buckets_per_table_, nullptr);
        num_items_mem_per_frame_.resize(num_frames_, 0);

        for (size_t i = 0; i < num_frames_; i++) {
            frame_files_.push_back(ctx.GetFile());
        }
        for (size_t i = 0; i < num_frames_; i++) {
            frame_writers_.push_back(frame_files_[i].GetWriter());
        }

        // set up second table
        max_num_blocks_second_reduce_ = std::max<size_t>((size_t)((byte_size_ * table_rate_)
                                                              / static_cast<double>(sizeof(BucketBlock))), 0);

        max_num_blocks_second_reduce_ = std::max<size_t>(max_num_blocks_second_reduce_, 1);

        max_num_items_second_reduce_ = max_num_blocks_second_reduce_ * block_size_;

        fill_rate_num_items_second_reduce_ = (size_t)(max_num_items_second_reduce_ * max_frame_fill_rate_);

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

        frame_sequence_.resize(num_frames_, 0);
        size_t idx = 0;
        for (size_t i=0; i<num_frames_; i++)
        {
            frame_sequence_[idx++] = i;
        }
    }

    ReducePostTable(Context& ctx, KeyExtractor key_extractor,
                    ReduceFunction reduce_function, EmitterFunction emit)
        : ReducePostTable(ctx, key_extractor, reduce_function, emit, IndexFunction(),
                          FlushFunction(reduce_function)) { }

    //! non-copyable: delete copy-constructor
    ReducePostTable(const ReducePostTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePostTable& operator = (const ReducePostTable&) = delete;

    ~ReducePostTable() {
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
     * number of items in the table (max_num_items_table) is reached.
     *
     * Alternatively, it may trigger a resize of table in case maximal number of items per
     * bucket is reached.
     *
     * \param p Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {

        size_t global_index = index_function_(kv.first, this, num_buckets_per_table_);

        assert(global_index >= 0 && global_index < num_buckets_per_table_);

        size_t frame_id = global_index / num_buckets_per_frame_;

        LOG << "key: " << kv.first << " to bucket id: " << global_index;

        BucketBlock* current = buckets_[global_index];

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

        current = buckets_[global_index];

        // have an item that needs to be added.
        if (current == nullptr || current->size == block_size_)
        {
            //////
            // new block needed.
            //////

            // spill largest frame if max number of blocks reached
            if (num_blocks_per_table_ == max_num_blocks_per_table_)
            {
                SpillLargestFrame();
            }

            // allocate a new block of uninitialized items, prepend to bucket
            current = block_pool.GetBlock();
            current->next = buckets_[global_index];
            buckets_[global_index] = current;

            // Total number of blocks
            num_blocks_per_table_++;
        }

        // in-place construct/insert new item in current bucket block
        new (current->items + current->size++)KeyValuePair(kv);

        // Number of items per frame.
        num_items_mem_per_frame_[frame_id]++;

        if (num_items_mem_per_frame_[frame_id] > fill_rate_num_items_mem_per_frame_)
        {
            SpillFrame(frame_id);
        }
    }

    /*!
    * Flushes all items in the whole table.
    */
    void Flush(bool consume = false) {
        LOG << "Flushing items";

        flush_function_(consume, this);

        LOG << "Flushed items";
    }

    /*!
     * Retrieve all items belonging to the frame
     * having the most items. Retrieved items are then spilled
     * to the provided file.
     */
    void SpillLargestFrame() {
        // get frame with max size
        size_t p_size_max = 0;
        size_t p_idx = 0;
        for (size_t i = 0; i < num_frames_; i++)
        {
            if (num_items_mem_per_frame_[i] > p_size_max)
            {
                p_size_max = num_items_mem_per_frame_[i];
                p_idx = i;
            }
        }

        if (p_size_max == 0) {
            return;
        }

        SpillFrame(p_idx);
    }

    /*!
     * Retrieve all items belonging to the frame
     * having the most items. Retrieved items are then spilled
     * to the provided file.
     */
    void SpillSmallestFrame() {
        // get frame with min size
        size_t p_size_min = ULONG_MAX;
        size_t p_idx = 0;
        for (size_t i = 0; i < num_frames_; i++)
        {
            if (num_items_mem_per_frame_[i] < p_size_min
                && num_items_mem_per_frame_[i] != 0)
            {
                p_size_min = num_items_mem_per_frame_[i];
                p_idx = i;
            }
        }

        if (p_size_min == 0
            || p_size_min == ULONG_MAX) {
            return;
        }

        SpillFrame(p_idx);
    }

    /*!
     * Spills all items of a frame.
     *
     * \param frame_id The id of the frame to be spilled.
     */
    void SpillFrame(size_t frame_id) {
        data::File::Writer& writer = frame_writers_[frame_id];

        for (size_t i = frame_id * num_buckets_per_frame_;
             i < (frame_id + 1) * num_buckets_per_frame_; i++)
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

            buckets_[i] = nullptr;
        }

        // reset number of blocks in external memory
        num_items_mem_per_frame_[frame_id] = 0;
        if (bench) {
            // increase spill counter
            num_spills_++;
        }
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const KeyValuePair& p, const size_t& partition_id) {
        (void)partition_id;
        emit_impl_.EmitElement(p, emit_);
    }

    /*!
     * Returns the num of buckets in a frame.
     *
     * \return Number of buckets in a frame.
     */
    size_t NumBucketsPerFrame() const {
        return num_buckets_per_frame_;
    }

    /*!
     * Sets the num of blocks in the table.
     */
    void SetNumBlocksPerTable(const size_t num_blocks) {
        num_blocks_per_table_ = num_blocks;
    }

    /*!
     * Returns the number of items in the table.
     *
     * \return Number of items in the table.
     */
    size_t NumItemsPerTable() const {

        size_t total_num_items = 0;
        for (size_t num_items : num_items_mem_per_frame_) {
            total_num_items += num_items;
        }

        return total_num_items;
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
     * Returns the begin local index.
     *
     * \return Begin local index.
     */
    size_t BeginLocalIndex() const {
        return begin_local_index_;
    }

    /*!
     * Returns the end local index.
     *
     * \return End local index.
     */
    size_t EndLocalIndex() const {
        return end_local_index_;
    }

    /*!
     * Returns the neutral element.
     *
     * \return Neutral element.
     */
    Value NeutralElement() const {
        return neutral_element_;
    }

    /*!
     * Returns the vector of frame files.
     *
     * \return Vector of frame files.
     */
    std::vector<data::File> & FrameFiles() {
        return frame_files_;
    }

    /*!
     * Returns the vector of frame writers.
     *
     * \return Vector of frame writers.
     */
    std::vector<data::File::Writer> & FrameWriters() {
        return frame_writers_;
    }

    /*!
     * Returns the vector of number of items per frame in internal memory.
     *
     * \return Vector of number of items per frame in internal memory.
     */
    std::vector<size_t> & NumItemsMemPerFrame() {
        return num_items_mem_per_frame_;
    }

    /*!
     * Returns the number of frames.
     *
     * \return Number of frames.
     */
    size_t NumFrames() const {
        return num_frames_;
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
     * Returns the sequence of frame ids to
     * be processed on flush.
     */
    std::vector<size_t>& FrameSequence() {
        return frame_sequence_;
    }

    void incrRecursiveSpills() {
        num_recursive_spills_++;
    }

private:
    //! Context
    Context& ctx_;

    //! Number of buckets per table.
    size_t num_buckets_per_table_ = 0;

    // Maximal frame fill rate.
    double max_frame_fill_rate_ = 1.0;

    //! Maximal number of blocks in the table before some items
    //! are spilled.
    size_t max_num_blocks_per_table_ = 0;

    //! Total number of blocks in the table.
    size_t num_blocks_per_table_ = 0;

    //! Emitter function.
    EmitterFunction emit_;

    //! Size of the table in bytes.
    size_t byte_size_ = 0;

    //! Store the items.
    std::vector<BucketBlock*> buckets_;

    //! Store the files for frames.
    std::vector<data::File> frame_files_;

    //! Store the writers for frames.
    std::vector<data::File::Writer> frame_writers_;

    //! Begin local index (reduce to index).
    size_t begin_local_index_ = 0;

    //! End local index (reduce to index).
    size_t end_local_index_ = 0;

    //! Neutral element (reduce to index).
    Value neutral_element_;

    //! Number of frames.
    size_t num_frames_ = 0;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Flush function.
    FlushFunction flush_function_;

    //! Number of items per frame in internal memory.
    std::vector<size_t> num_items_mem_per_frame_;

    //! Maximal number of items per partition.
    size_t max_num_items_mem_per_frame_ = 0;

    //! Number of spills.
    size_t num_spills_ = 0;

    //! Number of buckets per frame.
    size_t num_buckets_per_frame_ = 0;

    //! Maximal number of blocks per frame.
    size_t max_num_blocks_mem_per_frame_ = 0;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Bucket block pool.
    BucketBlockPool<BucketBlock> block_pool;

    //! Rate of sizes of primary to secondary table.
    double table_rate_ = 0.0;

    //! Storing the secondary table.
    std::vector<BucketBlock*> second_table_;

    size_t max_num_items_second_reduce_;

    //! Number of items per frame considering fill rate.
    size_t fill_rate_num_items_mem_per_frame_ = 0;

    size_t second_table_size_;

    size_t max_num_blocks_second_reduce_;

    size_t fill_rate_num_items_second_reduce_;

    //! Frame Sequence.
    std::vector<size_t> frame_sequence_;

    //! Number of recursive spills.
    size_t num_recursive_spills_ = 0;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_POST_BUCKET_TABLE_HEADER

/******************************************************************************/
