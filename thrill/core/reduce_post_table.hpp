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
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_POST_TABLE_HEADER
#define THRILL_CORE_REDUCE_POST_TABLE_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_pool.hpp>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <functional>
#include <string>
#include <utility>
#include <vector>
#include <iostream>
#include <cmath>

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
template <typename Key, typename HashFunction = std::hash<Key> >
class PostReduceByHashKey
{
public:
    explicit PostReduceByHashKey(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePostTable>
    size_t
    operator () (const Key& k, ReducePostTable* ht, const size_t& size) const {

        (*ht).NumBlocks();

        size_t hashed = hash_function_(k);

        return hashed % size;
    }

private:
    HashFunction hash_function_;
};

class PostReduceByIndex
{
public:
    PostReduceByIndex() { }

    template <typename ReducePostTable>
    size_t
    operator () (const size_t& k, ReducePostTable* ht, const size_t& size) const {

        return (k - ht->BeginLocalIndex()) % size;
    }
};

template <typename Key,
            typename ReduceFunction,
            typename IndexFunction = PostReduceByHashKey<Key>,
            typename EqualToFunction = std::equal_to<Key>>
class PostReduceFlushToDefault
{
public:
    PostReduceFlushToDefault(const IndexFunction& index_function = IndexFunction(),
                             const EqualToFunction& equal_to_function = EqualToFunction()) :
            index_function_(index_function),
            equal_to_function_(equal_to_function)
    {
    }

    template <typename ReducePostTable>
    void
    operator () (ReducePostTable* ht) const {

        using KeyValuePair = typename ReducePostTable::KeyValuePair;

        using BucketBlock = typename ReducePostTable::BucketBlock;

        auto& buckets_ = ht->Items();

        auto& frame_files = ht->FrameFiles();

        auto& frame_writers = ht->FrameWriters();

        auto& num_items_per_frame = ht->NumItemsPerFrame();

        //! Data structure for second reduce table
        std::vector<BucketBlock*> second_reduce;

        for (size_t frame_id = 0; frame_id < ht->NumFrames(); frame_id++) {

            // compute frame offset of current frame
            size_t offset = frame_id * ht->FrameSize();

            // get the actual reader from the file
            data::File& file = frame_files[frame_id];
            data::File::Writer& writer = frame_writers[frame_id];
            std::cout << "close" << std::endl;
            writer.Close();
            std::cout << "close 2" << std::endl;

            // only if items have been spilled,
            // process a second reduce
            if (file.NumItems() > 0)  {

                size_t frame_length = (size_t) std::ceil(static_cast<double>(file.NumItems())
                                                         / static_cast<double>(ht->block_size_)); // TODO

                // adjust size of second reduce table
                second_reduce.resize(frame_length, NULL);

                /////
                // reduce data from spilled files
                /////

                data::File::Reader reader = file.GetReader();

                // get the items and insert them in secondary
                // table
                while(reader.HasNext()) {

                    KeyValuePair kv = reader.Next<KeyValuePair>();

                    size_t global_index = index_function_(kv.first, ht, frame_length);

                    BucketBlock* current = second_reduce[global_index];
                    while (current != NULL)
                    {
                        // iterate over valid items in a block
                        for (KeyValuePair* bi = current->items;
                             bi != current->items + current->size; ++bi)
                        {
                            // if item and key equals, then reduce.
                            if (equal_to_function_(kv.first, bi->first))
                            {
                                bi->second = ht->reduce_function_(bi->second, kv.second);
                                return;
                            }
                        }

                        current = current->next;
                    }

                    current = second_reduce[global_index];

                    // have an item that needs to be added.
                    if (current == NULL ||
                        current->size == ht->block_size_)
                    {
                        // allocate a new block of uninitialized items, postpend to bucket
                        current = static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

                        current->size = 0;
                        current->next = second_reduce[global_index];
                        second_reduce[global_index] = current;
                    }

                    // in-place construct/insert new item in current bucket block
                    new (current->items + current->size++)KeyValuePair(kv.first, std::move(kv.second));
                }

                /////
                // reduce data from primary table
                /////
                for (size_t i = offset; i < offset + ht->FrameSize(); i++)
                {
                    BucketBlock* current = buckets_[i];

                    while (current != NULL)
                    {
                        for (KeyValuePair* from = current->items;
                             from != current->items + current->size; ++from)
                        {
                            // insert in second reduce table
                            size_t global_index = index_function_(from->first, ht, frame_length);
                            BucketBlock* current = second_reduce[global_index];
                            while (current != NULL)
                            {
                                // iterate over valid items in a block
                                for (KeyValuePair* bi = current->items;
                                     bi != current->items + current->size; ++bi)
                                {
                                    // if item and key equals, then reduce.
                                    if (equal_to_function_(from->first, bi->first))
                                    {
                                        bi->second = ht->reduce_function_(bi->second, from->second);
                                        return;
                                    }
                                }

                                current = current->next;
                            }

                            current = second_reduce[global_index];

                            // have an item that needs to be added.
                            if (current == NULL ||
                                current->size == ht->block_size_)
                            {
                                // allocate a new block of uninitialized items, postpend to bucket
                                current = static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

                                current->size = 0;
                                current->next = second_reduce[global_index];
                                second_reduce[global_index] = current;
                            }

                            // in-place construct/insert new item in current bucket block
                            new (current->items + current->size++)KeyValuePair(from->first, std::move(from->second));
                        }

                        // destroy block and advance to next
                        BucketBlock* next = current->next;
                        current->destroy_items();
                        operator delete (current);
                        current = next;
                    }

                    buckets_[i] = NULL;
                }

                num_items_per_frame[frame_id] = 0;

                /////
                // emit data
                /////
                for (size_t i = 0; i < frame_length; i++)
                {
                    BucketBlock* current = second_reduce[i];

                    while (current != NULL)
                    {
                        for (KeyValuePair* bi = current->items;
                             bi != current->items + current->size; ++bi)
                        {
                            ht->EmitAll(std::make_pair(bi->first, bi->second));
                        }

                        // destroy block and advance to next
                        BucketBlock* next = current->next;
                        current->destroy_items();
                        operator delete (current);
                        current = next;
                    }

                    second_reduce[i] = NULL;
                }

            // no spilled items, just flush already reduced
            // data in primary table in current frame
            } else
            {
                /////
                // emit data
                /////
                for (size_t i = offset; i < offset + ht->FrameSize(); i++)
                {
                    BucketBlock* current = buckets_[i];

                    while (current != NULL)
                    {
                        for (KeyValuePair* bi = current->items;
                             bi != current->items + current->size; ++bi)
                        {
                            ht->EmitAll(std::make_pair(bi->first, bi->second));
                        }

                        // destroy block and advance to next
                        BucketBlock* next = current->next;
                        current->destroy_items();
                        operator delete (current);
                        current = next;
                    }

                    buckets_[i] = NULL;
                }
            }
        }
        ht->SetNumBlocks(0);
        ht->SetNumItems(0);
    }
private:
    //ReduceFunction reduce_function_;
    IndexFunction index_function_;
    EqualToFunction equal_to_function_;
};

template <typename Value>
class PostReduceFlushToIndex
{
public:
    template <typename ReducePostTable>
    void
    operator () (ReducePostTable* ht) const {

        using BucketBlock = typename ReducePostTable::BucketBlock;

        using KeyValuePair = typename ReducePostTable::KeyValuePair;

        auto& buckets_ = ht->Items();

        std::vector<Value> elements_to_emit
            (ht->EndLocalIndex() - ht->BeginLocalIndex(), ht->NeutralElement());

        for (size_t i = 0; i < ht->NumBuckets(); i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != nullptr)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    elements_to_emit[bi->first - ht->BeginLocalIndex()] =
                        bi->second;
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }

            buckets_[i] = nullptr;
        }

        size_t index = ht->BeginLocalIndex();
        for (auto element_to_emit : elements_to_emit) {
            ht->EmitAll(std::make_pair(index++, element_to_emit));
        }
        assert(index == ht->EndLocalIndex());

        ht->SetNumBlocks(0);
        ht->SetNumItems(0);
    }
};

template <bool, typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl;

template <typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl<true, EmitterType, ValueType, SendType>{
    void EmitElement(ValueType ele, std::vector<EmitterType> emitters) {
        for (auto& emitter : emitters) {
            emitter(ele);
        }
    }
};

template <typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl<false, EmitterType, ValueType, SendType>{
    void EmitElement(ValueType ele, std::vector<EmitterType> emitters) {
        for (auto& emitter : emitters) {
            emitter(ele.second);
        }
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename FlushFunction = PostReduceFlushToDefault<Key, ReduceFunction>,
          typename IndexFunction = PostReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          size_t TargetBlockSize = 16*4
          >
class ReducePostTable
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const ValueType&)>;

    EmitImpl<SendPair, EmitterFunction, KeyValuePair, ValueType> emit_impl_;

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
            for (KeyValuePair* i = items; i != items + size; ++i)
                i->~KeyValuePair();
        }
    };

    /**
     * A data structure which takes an arbitrary value and extracts a key using a key extractor
     * function from that value. Afterwards, the value is hashed based on the key into some slot.
     *
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param index_function Function to be used for computing the bucket the item to be inserted.
     * \param flush_function Function to be used for flushing all items in the table.
     * \param begin_local_index Begin index for reduce to index.
     * \param end_local_index End index for reduce to index.
     * \param neutral element Neutral element for reduce to index.
     * \param num_buckets Number of buckets in the table.
     * \param max_frame_fill_rate Maximal number of items relative to maximal number of items in a frame.
     *        It the number is exceeded, no more blocks are added to a bucket, instead, items get spilled to disk.
     * \param max_num_blocks_table Maximal number of blocks allowed in the table. It the number is exceeded,
     *        no more blocks are added to a bucket, instead, items get spilled to disk using some spilling strategy
     *        defined in spill_function.
     * \param frame_size Number of buckets (=frame) exactly one file writer to be used for.
     * \param equal_to_function Function for checking equality of two keys.
     */
    ReducePostTable(KeyExtractor key_extractor,
                    ReduceFunction reduce_function,
                    std::vector<EmitterFunction>& emit,
                    const IndexFunction& index_function = IndexFunction(),
                    const FlushFunction& flush_function = FlushFunction(),
                    size_t begin_local_index = 0,
                    size_t end_local_index = 0,
                    Value neutral_element = Value(),
                    size_t num_buckets = 1024,
                    double max_frame_fill_rate = 0.5,
                    size_t max_num_blocks_table = 1024*16,
                    size_t frame_size = 32,
                    const EqualToFunction& equal_to_function = EqualToFunction())
        :   num_buckets_(num_buckets),
            max_frame_fill_rate_(max_frame_fill_rate),
            max_num_blocks_table_(max_num_blocks_table),
            emit_(std::move(emit)),
            begin_local_index_(begin_local_index),
            end_local_index_(end_local_index),
            neutral_element_(neutral_element),
            frame_size_(frame_size),
            key_extractor_(key_extractor),
            index_function_(index_function),
            equal_to_function_(equal_to_function),
            flush_function_(flush_function),
            reduce_function_(reduce_function)
    {
        sLOG << "creating ReducePostTable with" << emit_.size() << "output emitters";

        assert(num_buckets > 0 &&
                       (num_buckets & (num_buckets - 1)) == 0
               && "num_buckets must be a power of two");
        assert(max_frame_fill_rate >= 0.0 && max_frame_fill_rate <= 1.0);
        assert(max_num_blocks_table > 0);
        assert(frame_size > 0 && (frame_size & (frame_size - 1)) == 0
               && "frame_size must be a power of two");
        assert(frame_size <= num_buckets &&
               "frame_size must be less than or equal to num_buckets");
        assert(begin_local_index >= 0);
        assert(end_local_index >= 0);

        buckets_.resize(num_buckets_, NULL);

        num_frames_ = num_buckets_ / frame_size_;
        items_per_frame_.resize(num_frames_, 0);

        //frame_files_ = std::vector<data::File>(num_frames_, data::File(block_pool_));
        for (size_t i = 0; i < num_frames_; i++) {
            data::BlockPool block_pool_ { nullptr };
            frame_files_.push_back(data::File(block_pool_));
            frame_writers_.push_back(frame_files_[i].GetWriter(1024));
        }

        num_items_per_frame_ = (size_t) ((static_cast<double>(max_num_blocks_table * block_size_)
                                              / static_cast<double>(num_frames_)));
    }

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
    }

    /*!
     * Inserts a value. Calls the key_extractor_, makes a key-value-pair and
     * inserts the pair into the hashtable.
     */
    void Insert(const Value& p) {
        Key key = key_extractor_(p);

        Insert(std::make_pair(key, p));
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

        size_t global_index = index_function_(kv.first, this, num_buckets_);

        assert(global_index >= 0 && global_index < num_buckets_);

        size_t frame_id = global_index / frame_size_;

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

        // have an item that needs to be added.
        if (static_cast<double>(items_per_frame_[frame_id]+1)
            / static_cast<double>(num_items_per_frame_)
            > max_frame_fill_rate_)
        {
            SpillFrame(frame_id);
        }

        current = buckets_[global_index];

        // have an item that needs to be added.
        if (current == NULL ||
            current->size == block_size_)
        {
            if (num_blocks_ == max_num_blocks_table_)
            {
                SpillLargestFrame();
            }

            // allocate a new block of uninitialized items, postpend to bucket
            current =
                static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

            current->size = 0;
            current->next = buckets_[global_index];
            buckets_[global_index] = current;
            num_blocks_++;
        }

        // in-place construct/insert new item in current bucket block
        new (current->items + current->size++)KeyValuePair(kv.first, std::move(kv.second));
        // Number of items per frame.
        items_per_frame_[frame_id]++;
        // Increase total item count
        num_items_++;
    }

    /*!
    * Flushes all items in the whole table.
    */
    void Flush() {
        LOG << "Flushing items";

        flush_function_(this);

        LOG << "Flushed items";
    }

    /*!
     * Retrieve all items belonging to the frame
     * having the most items. Retrieved items are then spilled
     * to the provided file.
     */
    void SpillLargestFrame()
    {
        // get frame with max size
        size_t p_size_max = 0;
        size_t p_idx = 0;
        for (size_t i = 0; i < num_frames_; i++)
        {
            if (items_per_frame_[i] > p_size_max)
            {
                p_size_max = items_per_frame_[i];
                p_idx = i;
            }
        }

        SpillFrame(p_idx);
    }

    /*!
     * Spills all items of a frame.
     *
     * \param frame_id The id of the frame to be spilled.
     */
    void SpillFrame(size_t frame_id)
    {
        data::File::Writer& writer = frame_writers_[frame_id];

        for (size_t i = frame_id * frame_size_;
             i < frame_id * frame_size_ + frame_size_; i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != NULL)
            {
                num_blocks_--;

                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    std::cout << "write" << std::endl;
                    writer(*bi);
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }

            buckets_[i] = NULL;
        }

        // reset total counter
        num_items_ -= items_per_frame_[frame_id];
        // reset partition specific counter
        items_per_frame_[frame_id] = 0;
        // increase spill counter
        num_spills_++;
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const KeyValuePair& element) {
        // void EmitAll(const Key& key, const Value& value) {
        emit_impl_.EmitElement(element, emit_);
    }

    /*!
     * Returns the total num of buckets in the table.
     *
     * \return Number of buckets in the table.
     */
    size_t NumBuckets() const {
        return num_buckets_;
    }

    /*!
     * Returns the total num of blocks in the table.
     *
     * @return Number of blocks in the table.
     */
    size_t NumBlocks() const {
        return num_blocks_;
    }

    /*!
     * Sets the total num of blocks in the table.
     */
    void SetNumBlocks(const size_t &num_blocks) {
        num_blocks_ = num_blocks;
    }

    /*!
     * Sets the total num of items in the table.
     * Returns the total num of items in the table.
     *
     * \return Number of items in the table.
     */
    void SetNumItems(size_t num_items) {
        num_items_ = num_items;
    }

    /*!
     * Returns the vector of bucket blocks.
     *
     * \return Vector of bucket blocks.
     */
    std::vector<BucketBlock*>& Items() {
        return buckets_;
    }

    /*!
     * Returns the maximal fill rate.
     *
     * @return Maximal fill rate.
     */
    double MaxFrameFillRate() const {
        return max_frame_fill_rate_;
    }

    /*!
     * Sets the maximum number of blocks of the hash table. We don't want to push 2vt
     * elements before flush happens.
     *
     * \param size The maximal number of blocks the table may hold.
     */
    void SetMaxNumBlocksTable(const size_t &size) {
        max_num_blocks_table_ = size;
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
     * @return Vector of frame files.
     */
    std::vector<data::File>& FrameFiles() {
        return frame_files_;
    }

    /*!
     * Returns the vector of frame writers.
     *
     * @return Vector of frame writers.
     */
    std::vector<data::File::Writer>& FrameWriters() {
        return frame_writers_;
    }

    /*!
     * Returns the vector of number of items per frame.
     *
     * @return Vector of number of items per frame.
     */
    std::vector<size_t>& NumItemsPerFrame() {
        return items_per_frame_;
    }

    /*!
     * Returns the frame size.
     *
     * @return Frame size.
     */
    size_t FrameSize() const {
        return frame_size_;
    }

    /*!
     * Returns the number of frames.
     *
     * @return Number of frames.
     */
    size_t NumFrames() const {
        return num_frames_;
    }

    /*!
     * Returns the number of spills.
     *
     * @return Number of spills.
     */
    size_t NumSpills() const {
        return num_spills_;
    }

    /*!
     * Returns the number of items.
     *
     * @return Number of items.
     */
    size_t NumItems() const {
        return num_items_;
    }

    /*!
     * Prints content of hash table.
     */
    void Print() {
        LOG << "Printing";

        for (size_t i = 0; i < num_buckets_; i++)
        {
            if (buckets_[i] == nullptr)
            {
                LOG << "bucket id: "
                    << i
                    << " empty";
                continue;
            }

            std::string log = "";

            BucketBlock* current = buckets_[i];
            while (current != nullptr)
            {
                log += "block: ";

                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    log += "item: ";
                    log += std::to_string(i);
                    log += " (";
                    log += std::is_arithmetic<Key>::value || strcmp(typeid(Key).name(), "string")
                           ? std::to_string(bi->first) : "_";
                    log += ", ";
                    log += std::is_arithmetic<Value>::value || strcmp(typeid(Value).name(), "string")
                           ? std::to_string(bi->second) : "_";
                    log += ")\n";
                }
                current = current->next;
            }

            LOG << "bucket id: "
                << i
                << " "
                << log;
        }

        return;
    }

protected:
    //! Number of buckets.
    size_t num_buckets_;

    // Maximal frame fill rate.
    double max_frame_fill_rate_;

    //! Maximal number of blocks before some items
    //! are spilled.
    size_t max_num_blocks_table_;

    //! Keeps the total number of blocks in the table.
    size_t num_blocks_ = 0;

    //! Number of frames.
    size_t num_frames_ = 0;

    //! Set of emitters, one per partition.
    std::vector<EmitterFunction> emit_;

    //! Store the items.
    std::vector<BucketBlock*> buckets_;

    //! Store the files for frames.
    std::vector<data::File> frame_files_;

    //! Store the writers for frames.
    std::vector<data::File::Writer> frame_writers_;

    //! Begin local index (reduce to index).
    size_t begin_local_index_;

    //! End local index (reduce to index).
    size_t end_local_index_;

    //! Neutral element (reduce to index).
    Value neutral_element_;

    //! Frame size.
    size_t frame_size_ = 0;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Flush function.
    FlushFunction flush_function_;

    //! Keeps the total number of items in the table.
    size_t num_items_ = 0;

    //! Total num of items.
    size_t num_items_per_frame_;

    //! Number of items per frame.
    std::vector<size_t> items_per_frame_;

    //! Total num of spills.
    size_t num_spills_;

public:
    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_POST_TABLE_HEADER

/******************************************************************************/
