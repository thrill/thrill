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
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_POST_TABLE_HEADER
#define THRILL_CORE_REDUCE_POST_TABLE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>

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

        (void)ht;

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
          typename EqualToFunction = std::equal_to<Key> >
class PostReduceFlushToDefault
{
public:
    PostReduceFlushToDefault(ReduceFunction reduce_function,
                             const IndexFunction& index_function = IndexFunction(),
                             const EqualToFunction& equal_to_function = EqualToFunction())
        : reduce_function_(reduce_function),
          index_function_(index_function),
          equal_to_function_(equal_to_function)
    { }

    template <typename ReducePostTable>
    void
    operator () (bool consume, ReducePostTable* ht) const {

        using KeyValuePair = typename ReducePostTable::KeyValuePair;

        using BucketBlock = typename ReducePostTable::BucketBlock;

        size_t block_size = ht->BlockSize();

        std::vector<BucketBlock*>& buckets = ht->Items();

        std::vector<size_t>& num_blocks_mem_per_frame = ht->NumBlocksMemPerFrame();

        std::vector<size_t>& num_blocks_ext_per_frame = ht->NumBlocksExtPerFrame();

        std::vector<data::File>& frame_files = ht->FrameFiles();

        std::vector<data::File::Writer>& frame_writers = ht->FrameWriters();

        double bucket_rate = ht->BucketRate();

        size_t max_num_blocks_per_table = ht->MaxNumBlocksPerTable();

        size_t num_buckets_per_table = ht->NumBucketsPerTable();

        size_t num_buckets_per_frame = ht->NumBucketsPerFrame();

        std::vector<BucketBlock*> buckets_second;

        // in worst case, sum of number of blocks in internal and external memory
        // per frame needed
        size_t num_blocks_needed = 0;

        // get maximal number of blocks in frame in internal memory
        size_t num_blocks_max = 0;
        for (size_t i = 0; i < num_blocks_mem_per_frame.size(); i++)
        {
            if (num_blocks_mem_per_frame[i] > num_blocks_max)
            {
                num_blocks_max = num_blocks_mem_per_frame[i];
            }
        }
        num_blocks_needed += num_blocks_max;

        // get maximal number of blocks in frame in external memory
        num_blocks_max = 0;
        for (size_t i = 0; i < num_blocks_ext_per_frame.size(); i++)
        {
            if (num_blocks_ext_per_frame[i] > num_blocks_max)
            {
                num_blocks_max = num_blocks_ext_per_frame[i];
            }
        }
        num_blocks_needed += num_blocks_max;

        // add equivalent number of blocks to memory needed for pointers
        num_blocks_needed += std::max<size_t>((size_t)(std::ceil(
                                                           static_cast<double>(std::max<size_t>((size_t)(static_cast<double>(num_blocks_needed)
                                                                                                         * bucket_rate), 1) * sizeof(BucketBlock*))
                                                           / static_cast<double>(sizeof(BucketBlock)))), 0);

        // spilled more frames to ensure to have enough memory to process frames
        while ((max_num_blocks_per_table - ht->NumBlocksPerTable()) < num_blocks_needed) {
            if (ht->NumBlocksPerTable() == 0) {
                throw std::invalid_argument("not enough memory to process second reduce");
            }
            ht->SpillLargestFrame();
        }

        // from now on, it is ensured enough internal memory is available to process
        // every frame
        for (size_t frame_id = 0; frame_id != ht->NumFrames(); frame_id++) {

            // get the actual reader from the file
            data::File& file = frame_files[frame_id];
            data::File::Writer& writer = frame_writers[frame_id];
            writer.Close();

            // compute frame offset of current frame
            size_t offset = frame_id * num_buckets_per_frame;
            size_t length = std::min<size_t>(offset + num_buckets_per_frame, num_buckets_per_table);

            // only if items have been spilled,
            // process a second reduce
            if (file.num_items() > 0) {

                // compute how much blocks are needed
                size_t num_blocks = std::max<size_t>(
                    (size_t)std::ceil(static_cast<double>(file.num_items())
                                      / static_cast<double>(block_size)), 1);

                // calculate num buckets in second reduce table
                size_t num_buckets_second = std::max<size_t>((size_t)(static_cast<double>(num_blocks)
                                                                      * bucket_rate), 1);

                // keep enough memory for pointers
                num_blocks += std::max<size_t>((size_t)(std::ceil(
                                                            static_cast<double>(num_buckets_second * sizeof(BucketBlock*))
                                                            / static_cast<double>(sizeof(BucketBlock)))), 0);

                // set up size of second reduce table
                buckets_second.resize(num_buckets_second, nullptr);

                // flag used when item is reduced to advance to next item
                bool reduced = false;

                /////
                // reduce data from spilled files
                /////

                data::File::Reader reader = file.GetReader(consume);

                // get the items and insert them in secondary
                // table
                while (reader.HasNext()) {

                    KeyValuePair kv = reader.Next<KeyValuePair>();

                    size_t global_index = index_function_(kv.first, ht, num_buckets_second);

                    BucketBlock* current = buckets_second[global_index];
                    while (current != nullptr)
                    {
                        // iterate over valid items in a block
                        for (KeyValuePair* bi = current->items;
                             bi != current->items + current->size; ++bi)
                        {
                            // if item and key equals, then reduce.
                            if (equal_to_function_(kv.first, bi->first))
                            {
                                bi->second = reduce_function_(bi->second, kv.second);
                                reduced = true;
                                break;
                            }
                        }

                        if (reduced)
                        {
                            break;
                        }

                        current = current->next;
                    }

                    if (reduced)
                    {
                        reduced = false;
                        continue;
                    }

                    current = buckets_second[global_index];

                    // have an item that needs to be added.
                    if (current == nullptr ||
                        current->size == ht->block_size_)
                    {
                        // allocate a new block of uninitialized items, postpend to bucket
                        current = static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

                        current->size = 0;
                        current->next = buckets_second[global_index];
                        buckets_second[global_index] = current;
                    }

                    // in-place construct/insert new item in current bucket block
                    new (current->items + current->size++)KeyValuePair(kv.first, std::move(kv.second));
                }

                /////
                // reduce data from primary table
                /////
                for (size_t i = offset; i < length; i++)
                {
                    BucketBlock* current = buckets[i];

                    while (current != nullptr)
                    {
                        for (KeyValuePair* from = current->items;
                             from != current->items + current->size; ++from)
                        {
                            // insert in second reduce table
                            size_t global_index = index_function_(from->first, ht, num_buckets_second);
                            BucketBlock* current_second = buckets_second[global_index];
                            while (current_second != nullptr)
                            {
                                // iterate over valid items in a block
                                for (KeyValuePair* bi = current_second->items;
                                     bi != current_second->items + current_second->size; ++bi)
                                {
                                    // if item and key equals, then reduce.
                                    if (equal_to_function_(from->first, bi->first))
                                    {
                                        bi->second = reduce_function_(bi->second, from->second);
                                        reduced = true;
                                        break;
                                    }
                                }

                                if (reduced)
                                {
                                    break;
                                }

                                current_second = current_second->next;
                            }

                            if (reduced)
                            {
                                reduced = false;
                                continue;
                            }

                            current_second = buckets_second[global_index];

                            //////
                            // have an item that needs to be added.
                            //////

                            if (current_second == nullptr ||
                                current_second->size == ht->block_size_)
                            {
                                // allocate a new block of uninitialized items, postpend to bucket
                                current_second = static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

                                current_second->size = 0;
                                current_second->next = buckets_second[global_index];
                                buckets_second[global_index] = current_second;
                            }

                            // in-place construct/insert new item in current bucket block
                            new (current_second->items + current_second->size++)KeyValuePair(from->first, std::move(from->second));
                        }

                        // advance to next
                        if (consume)
                        {
                            BucketBlock* next = current->next;
                            // destroy block
                            current->destroy_items();
                            operator delete (current);
                            current = next;
                        }
                        else {
                            current = current->next;
                        }
                    }

                    if (consume)
                    {
                        buckets[i] = nullptr;
                    }
                }

                /////
                // emit data
                /////
                for (size_t i = 0; i < num_buckets_second; i++)
                {
                    BucketBlock* current = buckets_second[i];

                    while (current != nullptr)
                    {
                        for (KeyValuePair* bi = current->items;
                             bi != current->items + current->size; ++bi)
                        {
                            ht->EmitAll(bi->first, bi->second);
                        }

                        // destroy block and advance to next
                        BucketBlock* next = current->next;
                        current->destroy_items();
                        operator delete (current);
                        current = next;
                    }

                    buckets_second[i] = nullptr;
                }

                // no spilled items, just flush already reduced
                // data in primary table in current frame
            }
            else
            {
                /////
                // emit data
                /////
                for (size_t i = offset; i < length; i++)
                {
                    BucketBlock* current = buckets[i];

                    while (current != nullptr)
                    {
                        for (KeyValuePair* bi = current->items;
                             bi != current->items + current->size; ++bi)
                        {
                            ht->EmitAll(bi->first, bi->second);
                        }

                        // advance to next
                        if (consume)
                        {
                            BucketBlock* next = current->next;
                            // destroy block
                            current->destroy_items();
                            operator delete (current);
                            current = next;
                        }
                        else {
                            current = current->next;
                        }
                    }

                    if (consume)
                    {
                        buckets[i] = nullptr;
                    }
                }
            }

            // set num blocks for frame to zero
            if (consume)
            {
                ht->SetNumBlocksPerTable(ht->NumBlocksPerTable() - num_blocks_mem_per_frame[frame_id]);
                num_blocks_mem_per_frame[frame_id] = 0;
                num_blocks_ext_per_frame[frame_id] = 0;
            }
        }

        if (consume)
        {
            ht->SetNumItemsPerTable(0);
        }
    }

private:
    ReduceFunction reduce_function_;
    IndexFunction index_function_;
    EqualToFunction equal_to_function_;
};

template <typename Value>
class PostReduceFlushToIndex // TODO: no spilling here
{
public:
    template <typename ReducePostTable>
    void
    operator () (bool consume, ReducePostTable* ht) const {

        (void)consume;

        using BucketBlock = typename ReducePostTable::BucketBlock;

        using KeyValuePair = typename ReducePostTable::KeyValuePair;

        auto& buckets_ = ht->Items();

        std::vector<Value> elements_to_emit
            (ht->EndLocalIndex() - ht->BeginLocalIndex(), ht->NeutralElement());

        for (size_t i = 0; i < ht->NumBucketsPerTable(); i++)
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
            ht->EmitAll(index++, element_to_emit);
        }
        assert(index == ht->EndLocalIndex());

        ht->SetNumBlocksPerTable(0);
        ht->SetNumItemsPerTable(0);
    }
};

template <bool, typename EmitterFunction, typename Key, typename Value, typename SendType>
struct EmitImpl;

template <typename EmitterFunction, typename Key, typename Value, typename SendType>
struct EmitImpl<true, EmitterFunction, Key, Value, SendType>{
    void EmitElement(const Key& k, const Value& v, EmitterFunction emit) {
        emit(std::make_pair(k, v));
    }
};

template <typename EmitterFunction, typename Key, typename Value, typename SendType>
struct EmitImpl<false, EmitterFunction, Key, Value, SendType>{
    void EmitElement(const Key& k, const Value& v, EmitterFunction emit) {
        (void)k;
        emit(v);
    }
};

template <typename ValueType, typename Key, typename Value, // TODO: dont need both ValueType and Value
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename FlushFunction = PostReduceFlushToDefault<Key, ReduceFunction>,
          typename IndexFunction = PostReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          size_t TargetBlockSize = 16*16
          >
class ReducePostTable
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const ValueType&)>;

    EmitImpl<SendPair, EmitterFunction, Key, Value, ValueType> emit_impl_;

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
                    size_t byte_size = 1024 * 1024 * 128 * 4,
                    double bucket_rate = 0.9,
                    double max_frame_fill_rate = 0.6,
                    double frame_rate = 0.01,
                    const EqualToFunction& equal_to_function = EqualToFunction())
        : max_frame_fill_rate_(max_frame_fill_rate),
          emit_(emit),
          byte_size_(byte_size),
          bucket_rate_(bucket_rate),
          begin_local_index_(begin_local_index),
          end_local_index_(end_local_index),
          neutral_element_(neutral_element),
          key_extractor_(key_extractor),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          flush_function_(flush_function),
          reduce_function_(reduce_function) {
        //sLOG << "creating ReducePostTable with" << emit_.size() << "output emitters";

        assert(max_frame_fill_rate >= 0.0 && max_frame_fill_rate <= 1.0);
        assert(frame_rate > 0.0 && frame_rate <= 1.0);
        assert(byte_size > 0 && "byte_size must be greater than 0");
        assert(bucket_rate >= 0.0 && bucket_rate <= 1.0);
        assert(begin_local_index >= 0);
        assert(end_local_index >= 0);

        num_frames_ = std::max<size_t>((size_t)(1.0 / frame_rate), 1);

        max_num_blocks_per_table_ = std::max<size_t>((size_t)(static_cast<double>(byte_size_)
                                                              / static_cast<double>(sizeof(BucketBlock))), 1);
        max_num_blocks_mem_per_frame_ = std::max<size_t>((size_t)(static_cast<double>(max_num_blocks_per_table_)
                                                                  / static_cast<double>(num_frames_)), 1);
        num_buckets_per_frame_ = std::max<size_t>((size_t)(static_cast<double>(max_num_blocks_mem_per_frame_)
                                                           * bucket_rate), 1);
        num_buckets_per_table_ = num_buckets_per_frame_ * num_frames_;

        // reduce number of blocks once we know how many buckets we have, thus
        // knowing the size of pointers in the bucket vector
        max_num_blocks_per_table_ -= std::max<size_t>((size_t)(std::ceil(
                                                                   static_cast<double>(num_buckets_per_table_ * sizeof(BucketBlock*))
                                                                   / static_cast<double>(sizeof(BucketBlock)))), 0);

        assert(num_frames_ > 0);
        assert(max_num_blocks_per_table_ > 0);
        assert(max_num_blocks_mem_per_frame_ > 0);
        assert(num_buckets_per_frame_ > 0);
        assert(num_buckets_per_table_ > 0);

        buckets_.resize(num_buckets_per_table_, nullptr);
        num_blocks_mem_per_frame_.resize(num_frames_, 0);
        num_blocks_ext_per_frame_.resize(num_frames_, 0);

        for (size_t i = 0; i < num_frames_; i++) {
            frame_files_.push_back(ctx.GetFile());
        }
        for (size_t i = 0; i < num_frames_; i++) {
            frame_writers_.push_back(frame_files_[i].GetWriter());
        }
    }

    ReducePostTable(Context& ctx, KeyExtractor key_extractor,
                    ReduceFunction reduce_function, EmitterFunction emit)
            : ReducePostTable(ctx, key_extractor, reduce_function, emit, IndexFunction(),
                              FlushFunction(reduce_function)) {}

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

            // spill current frame if max frame fill rate reached
            if (static_cast<double>(num_blocks_mem_per_frame_[frame_id] + 1)
                / static_cast<double>(max_num_blocks_mem_per_frame_)
                > max_frame_fill_rate_)
            {
                SpillFrame(frame_id);
            }

            // spill largest frame if max number of blocks reached
            if (num_blocks_per_table_ == max_num_blocks_per_table_)
            {
                SpillLargestFrame();
            }

            // allocate a new block of uninitialized items, prepend to bucket
            current =
                static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

            current->size = 0;
            current->next = buckets_[global_index];
            buckets_[global_index] = current;

            // Number of blocks per frame.
            num_blocks_mem_per_frame_[frame_id]++;
            // Total number of blocks
            num_blocks_per_table_++;
        }

        // in-place construct/insert new item in current bucket block
        new (current->items + current->size++)KeyValuePair(kv);
        // Increase total item count
        num_items_per_table_++;
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
            if (num_blocks_mem_per_frame_[i] > p_size_max)
            {
                p_size_max = num_blocks_mem_per_frame_[i];
                p_idx = i;
            }
        }

        if (p_size_max == 0) {
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

                num_items_per_table_ -= current->size;

                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }

            buckets_[i] = nullptr;
        }

        // adjust number of blocks in table
        num_blocks_per_table_ -= num_blocks_mem_per_frame_[frame_id];
        // adjust number of blocks in external memory
        num_blocks_ext_per_frame_[frame_id] += num_blocks_mem_per_frame_[frame_id];
        // reset number of blocks in external memory
        num_blocks_mem_per_frame_[frame_id] = 0;
        // increase spill counter
        num_spills_++;
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const Key& k, const Value& v) {
        emit_impl_.EmitElement(k, v, emit_);
    }

    /*!
     * Returns the num of buckets in the table.
     *
     * \return Number of buckets in the table.
     */
    size_t NumBucketsPerTable() const {
        return num_buckets_per_table_;
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
     * Returns the num of blocks in the table.
     *
     * \return Number of blocks in the table.
     */
    size_t NumBlocksPerTable() const {
        return num_blocks_per_table_;
    }

    /*!
     * Returns the maximal num of blocks in the table.
     *
     * \return Maximal number of blocks in the table.
     */
    size_t MaxNumBlocksPerTable() const {
        return max_num_blocks_per_table_;
    }

    /*!
     * Sets the maximum number of blocks of the table.
     *
     * \param size The maximal number of blocks the table.
     */
    void SetMaxNumBlocksTable(const size_t& size) {
        max_num_blocks_per_table_ = size;
    }

    /*!
     * Sets the num of blocks in the table.
     */
    void SetNumBlocksPerTable(const size_t& num_blocks) {
        num_blocks_per_table_ = num_blocks;
    }

    /*!
     * Returns the number of items in the table.
     *
     * \return Number of items in the table.
     */
    size_t NumItemsPerTable() const {
        return num_items_per_table_;
    }

    /*!
     * Sets the num of items in the table.
     * Returns the num of items in the table.
     *
     * \return Number of items in the table.
     */
    void SetNumItemsPerTable(size_t num_items) {
        num_items_per_table_ = num_items;
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
     * Returns the maximal frame fill rate.
     *
     * \return Maximal frame fill rate.
     */
    double MaxFrameFillRate() const {
        return max_frame_fill_rate_;
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
     * Returns the vector of number of blocks per frame in internal memory.
     *
     * \return Vector of number of blocks per frame in internal memory.
     */
    std::vector<size_t> & NumBlocksMemPerFrame() {
        return num_blocks_mem_per_frame_;
    }

    /*!
     * Returns the vector of number of blocks per frame in external memory.
     *
     * \return Vector of number of blocks per frame in external memory.
     */
    std::vector<size_t> & NumBlocksExtPerFrame() {
        return num_blocks_ext_per_frame_;
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
     * Returns the bucket rate.
     *
     * \return Bucket rate.
     */
    double BucketRate() const {
        return bucket_rate_;
    }

    /*!
     * Returns the block size.
     *
     * \return Block size.
     */
    size_t BlockSize() const {
        return block_size_;
    }

    /*!
     * Prints content of hash table.
     */
    void Print() {
        LOG << "Printing";

        for (size_t i = 0; i < num_buckets_per_table_; i++)
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
    //! Number of buckets per table.
    size_t num_buckets_per_table_;

    // Maximal frame fill rate.
    double max_frame_fill_rate_;

    //! Maximal number of blocks in the table before some items
    //! are spilled.
    size_t max_num_blocks_per_table_;

    //! Total number of blocks in the table.
    size_t num_blocks_per_table_ = 0;

    //! Emitter function.
    EmitterFunction emit_;

    //! Size of the table in bytes.
    size_t byte_size_ = 0;

    //! Bucket rate.
    double bucket_rate_ = 0.0;

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

    //! Number of items in the table.
    size_t num_items_per_table_ = 0;

    //! Number of blocks per frame in internal memory.
    std::vector<size_t> num_blocks_mem_per_frame_;

    //! Number of blocks per frame in external memory.
    std::vector<size_t> num_blocks_ext_per_frame_;

    //! Number of spills.
    size_t num_spills_;

    //! Number of buckets per frame.
    size_t num_buckets_per_frame_;

    //! Maximal number of blocks per frame.
    size_t max_num_blocks_mem_per_frame_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_POST_TABLE_HEADER

/******************************************************************************/
