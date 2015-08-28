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

// REVIEW(ms): these two external classes make things complex, are they really
// needed? or can one move them into the main class and just do a if() switch?
// Or is there some type access reason that this cannot work?  On second
// thought: there probably is a reason. If so, please write a comment explaining
// why.
// COMMENT(tb): Type of Flushing needs be externally configurable as we have different flushes:
// currently for default reduce and for reduce to index. We use them same approach in the
// corresponding pre tables. Since the approach is templated, there are no explicit function calls.
template <typename Key,
          typename ReduceFunction,
          const bool ClearAfterFlush = false,
          typename IndexFunction = PostReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class PostReduceFlushToDefault
{
public:
    PostReduceFlushToDefault(const IndexFunction& index_function = IndexFunction(),
                             const EqualToFunction& equal_to_function = EqualToFunction())
        : index_function_(index_function),
          equal_to_function_(equal_to_function)
    { }

    template <typename ReducePostTable>
    void
    operator () (ReducePostTable* ht) const {

        using KeyValuePair = typename ReducePostTable::KeyValuePair;

        using BucketBlock = typename ReducePostTable::BucketBlock;

        std::vector<BucketBlock*>& buckets_ = ht->Items();

        std::vector<data::File>& frame_files = ht->FrameFiles();

        std::vector<data::File::Writer>& frame_writers = ht->FrameWriters();

        std::vector<size_t>& num_items_per_frame = ht->NumItemsPerFrame();

        //! Data structure for second reduce table
        std::vector<BucketBlock*> second_reduce;

        for (size_t frame_id = 0; frame_id < ht->NumFrames(); frame_id++) {

            // compute frame offset of current frame
            size_t offset = frame_id * ht->FrameSize();
            size_t length = std::min<size_t>(offset + ht->FrameSize(), ht->NumBuckets());

            // get the actual reader from the file
            data::File& file = frame_files[frame_id];
            data::File::Writer& writer = frame_writers[frame_id];
            writer.Close();

            // only if items have been spilled,
            // process a second reduce
            if (file.NumItems() > 0) {

                size_t frame_length = (size_t)std::ceil(static_cast<double>(file.NumItems())
                                                        / static_cast<double>(ht->block_size_));  // TODO

                // adjust size of second reduce table
                second_reduce.resize(frame_length, nullptr);

                /////
                // reduce data from spilled files
                /////

                data::File::Reader reader = file.GetReader();

                // flag used when item is reduced to advance to next item
                bool reduced = false;

                // get the items and insert them in secondary
                // table
                while (reader.HasNext()) {

                    KeyValuePair kv = reader.Next<KeyValuePair>();

                    size_t global_index = index_function_(kv.first, ht, frame_length);

                    BucketBlock* current = second_reduce[global_index];
                    while (current != nullptr)
                    {
                        // iterate over valid items in a block
                        for (KeyValuePair* bi = current->items;
                             bi != current->items + current->size; ++bi)
                        {
                            // if item and key equals, then reduce.
                            if (equal_to_function_(kv.first, bi->first))
                            {
                                bi->second = ht->reduce_function_(bi->second, kv.second);
                                reduced = true;
                                break;
                            }
                        }

                        current = current->next;
                    }

                    if (reduced)
                    {
                        reduced = false;
                        continue;
                    }

                    current = second_reduce[global_index];

                    // have an item that needs to be added.
                    if (current == nullptr ||
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
                for (size_t i = offset; i < length; i++)
                {
                    BucketBlock* current = buckets_[i];

                    while (current != nullptr)
                    {
                        for (KeyValuePair* from = current->items;
                             from != current->items + current->size; ++from)
                        {
                            // insert in second reduce table
                            size_t global_index = index_function_(from->first, ht, frame_length);
                            BucketBlock* current_second = second_reduce[global_index];
                            while (current_second != nullptr)
                            {
                                // iterate over valid items in a block
                                for (KeyValuePair* bi = current_second->items;
                                     bi != current_second->items + current_second->size; ++bi)
                                {
                                    // if item and key equals, then reduce.
                                    if (equal_to_function_(from->first, bi->first))
                                    {
                                        bi->second = ht->reduce_function_(bi->second, from->second);
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

                            current_second = second_reduce[global_index];

                            // have an item that needs to be added.
                            if (current_second == nullptr ||
                                current_second->size == ht->block_size_)
                            {
                                // allocate a new block of uninitialized items, postpend to bucket
                                current_second = static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

                                current_second->size = 0;
                                current_second->next = second_reduce[global_index];
                                second_reduce[global_index] = current_second;
                            }

                            // in-place construct/insert new item in current bucket block
                            new (current_second->items + current_second->size++)KeyValuePair(from->first, std::move(from->second));
                        }

                        // advance to next
                        if (ClearAfterFlush)
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

                    if (ClearAfterFlush)
                    {
                        buckets_[i] = nullptr;
                    }
                }

                if (ClearAfterFlush)
                {
                    num_items_per_frame[frame_id] = 0;
                }

                /////
                // emit data
                /////
                for (size_t i = 0; i < frame_length; i++)
                {
                    BucketBlock* current = second_reduce[i];

                    while (current != nullptr)
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

                    second_reduce[i] = nullptr;
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
                    BucketBlock* current = buckets_[i];

                    while (current != nullptr)
                    {
                        for (KeyValuePair* bi = current->items;
                             bi != current->items + current->size; ++bi)
                        {
                            ht->EmitAll(std::make_pair(bi->first, bi->second));
                        }

                        // advance to next
                        if (ClearAfterFlush)
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

                    if (ClearAfterFlush)
                    {
                        buckets_[i] = nullptr;
                    }
                }
            }
        }

        // REVIEW(ms): this resets the reduce table. I think we disagree about
        // what FlushData() does: in my view it should read and emit all data
        // from the external files, but LEAVE EVERYTHING in place, such that
        // this procedure can be REPEATED. Why? because that is what
        // DIANode::PushData() requires us to do: repeatable data emitting. Is
        // that too costly? Maybe, but then the user can append a .Cache() node
        // to the ReduceTable which will create an actual storage File.  But
        // then we cannot keep the Buckets in "memory", since they would always
        // take precious RAM while other stages are executed.

        // COMMENT(tb): In Spark, computed data is NOT kept in memory by default.
        // In Spark, Cache() keeps computed data IN memory (in memory by default, but
        // there are flags to store to disk and other targets as well).
        // What you suggest is actually the opposite way of how Spark handles it, keeping
        // stuff in memory by default, no matter what...
        // I am not saying its wrong, I just don't know. I suggest we just benchmark this using
        // some big data.
        //
        // As I implemented it, I assumed that we do not have a caching mechanism by now,
        // so I required data to be recomputed if necessary and free memory after
        // each flush.
        //
        // Just a thought: If you want to keep data in memory by default, and provide
        // a method for memory -> disk transfer, calling that method Spill() would probably more
        // suitable.
        //
        // We may have some intelligent, implicit memory -> disk transfer, using LRU or whatever.
        //
        // To come to and end, I added a template parameter so that the table can be configured
        // whether or not to free memory after each flush. It defaults to "keep all in memory".

        if (ClearAfterFlush)
        {
            ht->SetNumBlocks(0);
            ht->SetNumItems(0);
        }
    }

private:
    // ReduceFunction reduce_function_;
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
    // REVIEW(ms): these both should be const& ! check everywhere that ValueType
    // is not copied!
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
          const bool ClearAfterFlush = false,
          typename FlushFunction = PostReduceFlushToDefault<Key, ReduceFunction, ClearAfterFlush>,
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
     * \param context Context.
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param index_function Function to be used for computing the bucket the item to be inserted.
     * \param flush_function Function to be used for flushing all items in the table.
     * \param begin_local_index Begin index for reduce to index.
     * \param end_local_index End index for reduce to index.
     * \param neutral element Neutral element for reduce to index.
     * \param num_frames Number of frames in the table.
     * \param max_frame_fill_rate Maximal number of items relative to maximal number of items in a frame.
     *        It the number is exceeded, no more blocks are added to a bucket, instead, items get spilled to disk.
     * \param byte_size Maximal size of the table in byte. In case size of table exceeds that value, items
     *                  are flushed.
     * \param frame_size Number of buckets (=frame) exactly one file writer to be used for.
     * \param equal_to_function Function for checking equality of two keys.
     */
    ReducePostTable(Context& ctx,
                    KeyExtractor key_extractor,
                    ReduceFunction reduce_function,
                    std::vector<EmitterFunction>& emit,
                    const IndexFunction& index_function = IndexFunction(),
                    const FlushFunction& flush_function = FlushFunction(),
                    size_t begin_local_index = 0,
                    size_t end_local_index = 0,
                    Value neutral_element = Value(),
                    size_t byte_size = 1024 * 16,
                    double bucket_rate = 0.001,
                    double max_frame_fill_rate = 0.5,
                    size_t frame_size = 64,
                    const EqualToFunction& equal_to_function = EqualToFunction())
        : max_frame_fill_rate_(max_frame_fill_rate),
          emit_(std::move(emit)),
          byte_size_(byte_size),
          begin_local_index_(begin_local_index),
          end_local_index_(end_local_index),
          neutral_element_(neutral_element),
          frame_size_(frame_size),
          key_extractor_(key_extractor),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          flush_function_(flush_function),
          reduce_function_(reduce_function) {
        sLOG << "creating ReducePostTable with" << emit_.size() << "output emitters";

        assert(max_frame_fill_rate >= 0.0 && max_frame_fill_rate <= 1.0);
        assert(frame_size > 0 && (frame_size & (frame_size - 1)) == 0
               && "frame_size must be a power of two");
        assert(byte_size > 0 && "byte_size must be greater than 0");
        assert(bucket_rate > 0.0 && bucket_rate <= 1.0);
        assert(begin_local_index >= 0);
        assert(end_local_index >= 0);

        // TODO(ms): second reduce table is currently not considered for byte_size
        max_num_blocks_table_ = std::max<size_t>((size_t)(static_cast<double>(byte_size_)
                                                          / static_cast<double>(sizeof(BucketBlock))), 1);
        num_buckets_ = std::max<size_t>((size_t)(static_cast<double>(max_num_blocks_table_) * bucket_rate), 1);
        frame_size_ = std::min<size_t>(frame_size_, num_buckets_);
        num_frames_ = std::max<size_t>((size_t)(static_cast<double>(num_buckets_)
                                                / static_cast<double>(frame_size_)), 1);
        num_items_per_frame_ = std::max<size_t>((size_t)(static_cast<double>(max_num_blocks_table_
                                                                             * block_size_) / static_cast<double>(num_frames_)), 1);

        buckets_.resize(num_buckets_, nullptr);

        items_per_frame_.resize(num_frames_, 0);

        for (size_t i = 0; i < num_frames_; i++) {
            frame_files_.push_back(ctx.GetFile());
        }
        for (size_t i = 0; i < num_frames_; i++) {
            frame_writers_.push_back(frame_files_[i].GetWriter());
        }
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
        if (static_cast<double>(items_per_frame_[frame_id] + 1)
            / static_cast<double>(num_items_per_frame_)
            > max_frame_fill_rate_)
        {
            SpillFrame(frame_id);
        }

        current = buckets_[global_index];

        // have an item that needs to be added.
        if (current == nullptr ||
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
    void SpillLargestFrame() {
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
    void SpillFrame(size_t frame_id) {
        data::File::Writer& writer = frame_writers_[frame_id];

        for (size_t i = frame_id * frame_size_;
             i < frame_id * frame_size_ + frame_size_; i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != nullptr)
            {
                num_blocks_--;

                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    writer(*bi);
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }

            buckets_[i] = nullptr;
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
     * \return Number of blocks in the table.
     */
    size_t NumBlocks() const {
        return num_blocks_;
    }

    /*!
     * Sets the total num of blocks in the table.
     */
    void SetNumBlocks(const size_t& num_blocks) {
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
    std::vector<BucketBlock*> & Items() {
        return buckets_;
    }

    /*!
     * Returns the maximal fill rate.
     *
     * \return Maximal fill rate.
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
    void SetMaxNumBlocksTable(const size_t& size) {
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
     * Returns the vector of number of items per frame.
     *
     * \return Vector of number of items per frame.
     */
    std::vector<size_t> & NumItemsPerFrame() {
        return items_per_frame_;
    }

    /*!
     * Returns the frame size.
     *
     * \return Frame size.
     */
    size_t FrameSize() const {
        return frame_size_;
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
     * Returns the number of items.
     *
     * \return Number of items.
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

    //! Set of emitters, one per partition.
    std::vector<EmitterFunction> emit_;

    //! Size of the table in bytes
    size_t byte_size_ = 0;

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
