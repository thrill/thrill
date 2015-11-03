/*******************************************************************************
 * thrill/core/reduce_post_probing_table.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_POST_PROBING_TABLE_HEADER
#define THRILL_CORE_REDUCE_POST_PROBING_TABLE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/core/post_probing_reduce_flush.hpp>
#include <thrill/core/post_probing_reduce_flush_to_index.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <iostream>
#include <limits>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>
#include <limits.h>
#include <stddef.h>

namespace thrill {
namespace core {

template <typename Key, typename HashFunction = std::hash<Key> >
class PostProbingReduceByHashKey
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;

        IndexResult(size_t p_id, size_t g_id) {
            partition_id = p_id;
            global_index = g_id;
        }
    };

    explicit PostProbingReduceByHashKey(const HashFunction& hash_function = HashFunction())
            : hash_function_(hash_function)
    { }

    IndexResult
    operator () (const Key& k,
                 const size_t& num_frames,
                 const size_t& num_buckets_per_frame,
                 const size_t& num_buckets_per_table,
                 const size_t& offset) const {

        (void)num_buckets_per_table;
        (void)offset;

        size_t hashed = hash_function_(k);

        size_t partition_id = hashed % num_frames;

        return IndexResult(partition_id, partition_id *
                                         num_buckets_per_frame +
                                         hashed % num_buckets_per_frame);
    }

private:
    HashFunction hash_function_;
};

template <typename Key>
class PostProbingReduceByIndex
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;

        IndexResult(size_t p_id, size_t g_id) {
            partition_id = p_id;
            global_index = g_id;
        }
    };

    PostProbingReduceByIndex() { }

    IndexResult
    operator () (const Key& k,
                 const size_t& num_frames,
                 const size_t& num_buckets_per_frame,
                 const size_t& num_buckets_per_table,
                 const size_t& offset) const {

        (void)num_buckets_per_frame;

        size_t result = (k - offset) % num_buckets_per_table;

        return IndexResult(result / num_frames, result);
    }
};

/**
 * A data structure which takes an arbitrary value and extracts a key using
 * a key extractor function from that value. A key may also be provided initially as
 * part of a key/value pair, not requiring to extract a key.
 *
 * Afterwards, the key is hashed and the hash is used to assign that key/value pair
 * to some slot.
 *
 * In case a slot already has a key/value pair and the key of that value and the key of
 * the value to be inserted are them same, the values are reduced according to
 * some reduce function. No key/value is added to the data structure.
 *
 * If the keys are different, the next slot (moving to the right) is considered.
 * If the slot is occupied, the same procedure happens again (know as linear probing.)
 *
 * Finally, the key/value pair to be inserted may either:
 *
 * 1.) Be reduced with some other key/value pair, sharing the same key.
 * 2.) Inserted at a free slot.
 * 3.) Trigger a resize of the data structure in case there are no more free slots in
 *     the data structure.
 *
 * The following illustrations shows the general structure of the data structure.
 * The set of slots is divided into 1..n partitions. Each key is hashed into exactly
 * one partition.
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
template <bool, typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostProbingEmitImpl;

template <typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostProbingEmitImpl<true, EmitterFunction, KeyValuePair, SendType>{
    void EmitElement(const KeyValuePair& p, EmitterFunction emit) {
        emit(p);
    }
};

template <typename EmitterFunction, typename KeyValuePair, typename SendType>
struct PostProbingEmitImpl<false, EmitterFunction, KeyValuePair, SendType>{
    void EmitElement(const KeyValuePair& p, EmitterFunction emit) {
        emit(p.second);
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename FlushFunction = PostProbingReduceFlush<Key, Value, ReduceFunction>,
          typename IndexFunction = PostProbingReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class ReducePostProbingTable
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const ValueType&)>;

    PostProbingEmitImpl<SendPair, EmitterFunction, KeyValuePair, ValueType> emit_impl_;

    /**
     * A data structure which takes an arbitrary value and extracts a key using a key extractor
     * function from that value. Afterwards, the value is hashed based on the key into some slot.
     *
     * \param context Context.
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param sentinel Sentinel element used to flag free slots.
     * \param begin_local_index Begin index for reduce to index.
     * \param index_function Function to be used for computing the slot the item to be inserted.
     * \param flush_function Function to be used for flushing all items in the table.
     * \param end_local_index End index for reduce to index.
     * \param neutral element Neutral element for reduce to index.
     * \param byte_size Maximal size of the table in byte. In case size of table exceeds that value, items
     *                  are spilled to disk.
     * \param max_frame_fill_rate Maximal number of items per frame relative to number of slots allowed to be filled.
     *                            It the rate is exceeded, items get spilled to disk.
     * \param frame_rate Rate of number of buckets to number of frames. There is one file writer per frame.
     * \param equal_to_function Function for checking equality of two keys.
     * \param spill_function Function implementing a strategy to spill items to disk.
     */
    ReducePostProbingTable(Context& ctx,
                           const KeyExtractor& key_extractor,
                           const ReduceFunction& reduce_function,
                           const EmitterFunction& emit,
                           const Key& sentinel,
                           const IndexFunction& index_function,
                           const FlushFunction& flush_function,
                           size_t begin_local_index = 0,
                           size_t end_local_index = 0,
                           const Value& neutral_element = Value(),
                           size_t byte_size = 1024 * 16,
                           double max_frame_fill_rate = 0.5,
                           double frame_rate = 0.1,
                           const EqualToFunction& equal_to_function = EqualToFunction(),
                           double table_rate_multiplier = 1.05)
        : ctx_(ctx),
          byte_size_(byte_size),
          max_frame_fill_rate_(max_frame_fill_rate),
          key_extractor_(key_extractor),
          emit_(emit),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          flush_function_(flush_function),
          begin_local_index_(begin_local_index),
          end_local_index_(end_local_index),
          neutral_element_(neutral_element),
          reduce_function_(reduce_function) {

        assert(byte_size >= 0 && "byte_size must be greater than or equal to 0. "
                "a byte size of zero results in exactly one item per partition");
        assert(max_frame_fill_rate >= 0.0 && max_frame_fill_rate <= 1.0 && "max_partition_fill_rate "
                "must be between 0.0 and 1.0. with a fill rate of 0.0, items are immediately flushed.");
        assert(frame_rate > 0.0 && frame_rate <= 1.0 && "a frame rate of 1.0 causes exactly one frame.");
        assert(begin_local_index >= 0);
        assert(end_local_index >= 0);

        num_frames_ = std::max<size_t>((size_t)(1.0 / frame_rate), 1);

        table_rate_ = table_rate_multiplier * std::min<double>(1.0 / static_cast<double>(num_frames_), 0.5);

        frame_size_ = std::max<size_t>((size_t)(((byte_size_ * (1 - table_rate_))
                                                 / static_cast<double>(sizeof(KeyValuePair)))
                      / static_cast<double>(num_frames_)), 1);

        size_ = frame_size_ * num_frames_;

        fill_rate_num_items_per_frame_ = (size_t)(frame_size_ * max_frame_fill_rate_);

        assert(num_frames_ > 0);
        assert(frame_size_ > 0);
        assert(size_ > 0);
        assert(fill_rate_num_items_per_frame_ >= 0);

        items_per_frame_.resize(num_frames_, 0);

        for (size_t i = 0; i < num_frames_; i++) {
            frame_files_.push_back(ctx.GetFile());
        }
        for (size_t i = 0; i < num_frames_; i++) {
            frame_writers_.push_back(frame_files_[i].GetWriter());
        }

        sentinel_ = KeyValuePair(sentinel, Value());
        items_.resize(size_, sentinel_);

        // set up second table
        second_table_size_ = std::max<size_t>((size_t)((byte_size_ * table_rate_)
                                                              / static_cast<double>(sizeof(KeyValuePair))), 2);

        // ensure size of second table is even, in order to be able to split by half for spilling
        if (second_table_size_ % 2 != 0) {
            second_table_size_--;
        }
        second_table_size_ = std::max<size_t>(2, second_table_size_);

        assert(second_table_size_ > 0);

        second_table_.resize(second_table_size_, sentinel_);

        fill_rate_num_items_second_reduce_ = (size_t)(second_table_size_ * max_frame_fill_rate_);

        frame_sequence_.resize(num_frames_, 0);
        size_t idx = 0;
        for (size_t i=0; i<num_frames_; i++)
        {
            frame_sequence_[i] = i;
        }
    }

    ReducePostProbingTable(Context& ctx, KeyExtractor key_extractor,
            ReduceFunction reduce_function, EmitterFunction emit, const Key& sentinel)
    : ReducePostProbingTable(ctx, key_extractor, reduce_function, emit, sentinel, IndexFunction(),
            FlushFunction(reduce_function)) { }

    //! non-copyable: delete copy-constructor
    ReducePostProbingTable(const ReducePostProbingTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePostProbingTable& operator = (const ReducePostProbingTable&) = delete;

    ~ReducePostProbingTable() { }

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
     * Alternatively, it may trigger a resize of the table in case the maximal fill ratio
     * per partition is reached.
     *
     * \param p Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {

        typename IndexFunction::IndexResult h = index_function_(kv.first, num_frames_, frame_size_, size_, 0);

        assert(h.global_index >= 0 && h.global_index < size_);

        KeyValuePair* initial = &items_[h.global_index];
        KeyValuePair* current = initial;
        KeyValuePair* last_item = &items_[(h.partition_id + 1) * frame_size_ - 1];

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

            if (current == last_item)
            {
                current -= (frame_size_ - 1);
            }
            else
            {
                ++current;
            }

            // spill initial slot, if all the other slots
            // are occupied
            if (current == initial)
            {
                SpillFrame(h.partition_id);

                *current = kv;
                //current->first = kv.first;
                //current->second = kv.second;

                // increase counter for partition
                items_per_frame_[h.partition_id]++;

                return;
            }
        }

        // insert data
        *current = kv;
        //current->first = kv.first;
        //current->second = kv.second;

        // increase counter for frame
        items_per_frame_[h.partition_id]++;

        if (items_per_frame_[h.partition_id] > fill_rate_num_items_per_frame_)
        {
            SpillFrame(h.partition_id);
        }
    }

    /*!
     * Spills all items of a frame.
     *
     * \param frame_id The id of the frame to be spilled.
     */
    void SpillFrame(size_t frame_id) {

        data::File::Writer& writer = frame_writers_[frame_id];

        for (size_t i = frame_id * frame_size_;
             i < (frame_id + 1) * frame_size_; i++)
        {
            KeyValuePair& current = items_[i];
            if (current.first != sentinel_.first)
            {
                writer.PutItem(current);
                //items_[i].first = sentinel_.first;
                //items_[i].second = sentinel_.second;

                items_[i] = sentinel_;
            }
        }

        // reset partition specific counter
        items_per_frame_[frame_id] = 0;
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
     * Emits element to all children
     */
    void EmitAll(const KeyValuePair& p, const size_t& partition_id) {
        (void)partition_id;
        emit_impl_.EmitElement(p, emit_);
    }

    /*!
     * Returns the size of the table. The size corresponds to the number of slots.
     * A slot may be free or used.
     *
     * \return Size of the table.
     */
    size_t Size() const {
        return size_;
    }

    /*!
     * Returns the total num of items in the table.
     *
     * \return Number of items in the table.
     */
    size_t NumItems() const {

        size_t total_num_items = 0;
        for (size_t num_items : items_per_frame_) {
            total_num_items += num_items;
        }

        return total_num_items;
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
     * Returns the vector of frame files.
     *
     * \return Begin local index.
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
     * Returns the vector of key/value pairs.
     *
     * \return Vector of key/value pairs.
     */
    std::vector<KeyValuePair> & Items() {
        return items_;
    }

    /*!
     * Returns the maximal fill rate.
     *
     * \return Maximal fill rate.
     */
    double FillRateNumItemsSecondReduce() const {
        return fill_rate_num_items_second_reduce_;
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
     * Returns the sentinel element.
     *
     * \return Sentinal element.
     */
    KeyValuePair Sentinel() const {
        return sentinel_;
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
     * Returns the number of spills.
     *
     * \return Number of spills.
     */
    size_t NumSpills() const {
        return num_spills_;
    }

    /*!
     * Returns the vector of key/value pairs.
     *
     * \return Vector of key/value pairs.
     */
    std::vector<KeyValuePair> & SecondTable() {
        return second_table_;
    }

    /*!
     * Returns the vector of key/value pairs.
     *
     * \return Vector of key/value pairs.
     */
    Context& Ctx() {
        return ctx_;
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

    //! Size of the table in bytes
    size_t byte_size_ = 0;

    //! Size of the table, which is the number of slots
    //! available for items.
    size_t size_ = 0;

    //! Maximal allowed fill rate per partition
    //! before items get spilled.
    double max_frame_fill_rate_ = 1.0;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Emitter function.
    EmitterFunction emit_;

    //! Frame size.
    size_t frame_size_ = 0;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Comparator function for keys.
    FlushFunction flush_function_;

    //! Begin local index (reduce to index).
    size_t begin_local_index_ = 0;

    //! End local index (reduce to index).
    size_t end_local_index_ = 0;

    //! Neutral element (reduce to index).
    Value neutral_element_;

    //! Number of frames.
    size_t num_frames_ = 0;

    //! Storing the items.
    std::vector<KeyValuePair> items_;

    //! Store the files for frames.
    std::vector<data::File> frame_files_;

    //! Store the writers for frames.
    std::vector<data::File::Writer> frame_writers_;

    //! Sentinel element used to flag free slots.
    KeyValuePair sentinel_;

    //! Number of items per frame.
    std::vector<size_t> items_per_frame_;

    //! Total num of spills.
    size_t num_spills_ = 0;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Rate of sizes of primary to secondary table.
    double table_rate_ = 0.0;

    //! Storing the secondary table.
    std::vector<KeyValuePair> second_table_;

    //! Number of items per frame considering fill rate.
    size_t fill_rate_num_items_per_frame_ = 0;

    //! Size of the second table.
    size_t second_table_size_ = 0;

    size_t fill_rate_num_items_second_reduce_ = 0;

    //! Frame Sequence.
    std::vector<size_t> frame_sequence_;

    //! Number of recursive spills.
    size_t num_recursive_spills_ = 0;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_POST_PROBING_TABLE_HEADER

/******************************************************************************/
