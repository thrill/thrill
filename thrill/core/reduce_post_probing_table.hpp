/*******************************************************************************
 * thrill/core/reduce_post_probing_table.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
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

namespace thrill {
namespace core {

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
template <typename Key, typename HashFunction = std::hash<Key> >
class PostProbingReduceByHashKey
{
public:
    explicit PostProbingReduceByHashKey(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePostProbingTable>
    size_t
    operator () (const Key& k, ReducePostProbingTable* ht, const size_t& size) const {

        (void)ht;

        size_t hashed = hash_function_(k);

        return hashed % size;
    }

private:
    HashFunction hash_function_;
};

class PostProbingReduceByIndex
{
public:
    PostProbingReduceByIndex() { }

    template <typename ReducePostProbingTable>
    size_t
    operator () (const size_t& k, ReducePostProbingTable* ht, const size_t& size) const {

        return (k - ht->BeginLocalIndex()) % size;
    }
};

template <typename Key,
          typename ReduceFunction,
          const bool ClearAfterFlush = false,
          typename IndexFunction = PostProbingReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class PostProbingReduceFlushToDefault
{
public:
    // TODO(ms): find a way to inline the reduce function
    PostProbingReduceFlushToDefault(const IndexFunction& index_function = IndexFunction(),
                                    const EqualToFunction& equal_to_function = EqualToFunction())
        : index_function_(index_function),
          equal_to_function_(equal_to_function)
    { }

    template <typename ReducePostProbingTable>
    void
    operator () (ReducePostProbingTable* ht) const {

        using KeyValuePair = typename ReducePostProbingTable::KeyValuePair;

        std::vector<KeyValuePair>& items = ht->Items();

        std::vector<data::File>& frame_files = ht->FrameFiles();

        std::vector<data::File::Writer>& frame_writers = ht->FrameWriters();

        std::vector<size_t>& num_items_per_frame = ht->NumItemsPerFrame();

        //! Data structure for second reduce table
        std::vector<KeyValuePair> second_reduce;

        for (size_t frame_id = 0; frame_id < ht->NumFrames(); frame_id++) {

            // compute frame offset and length of current frame
            size_t offset = frame_id * ht->FrameSize();
            size_t length = (frame_id != ht->NumFrames() - 1) ? offset + ht->FrameSize() : ht->Size();

            // get the actual reader from the file
            data::File& file = frame_files[frame_id];
            data::File::Writer& writer = frame_writers[frame_id];
            writer.Close(); // also closes the file

            // only if items have been spilled,
            // process a second reduce
            if (file.NumItems() > 0) {

                size_t frame_length = (size_t)std::ceil(static_cast<double>(file.NumItems())
                                                        / ht->MaxFrameFillRate());

                // adjust size of second reduce table
                second_reduce.resize(frame_length, ht->Sentinel());

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

                    KeyValuePair* initial = &second_reduce[global_index];
                    KeyValuePair* current = initial;
                    KeyValuePair* last_item = &second_reduce[frame_length - 1];

                    while (!equal_to_function_(current->first, ht->Sentinel().first))
                    {
                        if (equal_to_function_(current->first, kv.first))
                        {
                            current->second = ht->reduce_function_(current->second, kv.second);
                            reduced = true;
                            break;
                        }

                        if (current == last_item)
                        {
                            current -= (frame_length - 1);
                        }
                        else
                        {
                            ++current;
                        }
                    }

                    if (reduced)
                    {
                        reduced = false;
                        continue;
                    }

                    // insert new pair
                    current->first = kv.first;
                    current->second = kv.second;
                }

                /////
                // reduce data from primary table
                /////
                for (size_t i = offset; i < length; i++)
                {
                    KeyValuePair& kv = items[i];
                    if (kv.first != ht->Sentinel().first)
                    {
                        size_t global_index = index_function_(kv.first, ht, frame_length);

                        KeyValuePair* initial = &second_reduce[global_index];
                        KeyValuePair* current = initial;
                        KeyValuePair* last_item = &second_reduce[frame_length - 1];

                        while (!equal_to_function_(current->first, ht->Sentinel().first))
                        {
                            if (equal_to_function_(current->first, kv.first))
                            {
                                current->second = ht->reduce_function_(current->second, kv.second);
                                reduced = true;
                                break;
                            }

                            if (current == last_item)
                            {
                                current -= (frame_length - 1);
                            }
                            else
                            {
                                ++current;
                            }
                        }

                        if (reduced)
                        {
                            reduced = false;
                            continue;
                        }

                        // insert new pair
                        current->first = kv.first;
                        current->second = kv.second;

                        if (ClearAfterFlush)
                        {
                            items[i] = ht->Sentinel();
                        }
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
                    KeyValuePair& current = second_reduce[i];
                    if (current.first != ht->Sentinel().first)
                    {
                        ht->EmitAll(std::make_pair(current.first, current.second));

                        second_reduce[i] = ht->Sentinel();
                    }
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
                    KeyValuePair& current = items[i];
                    if (current.first != ht->Sentinel().first)
                    {
                        ht->EmitAll(std::make_pair(current.first, current.second));

                        if (ClearAfterFlush)
                        {
                            items[i] = ht->Sentinel();
                        }
                    }
                }
            }
        }

        if (ClearAfterFlush)
        {
            ht->SetNumItems(0);
        }
    }

private:
    // ReduceFunction reduce_function_;
    IndexFunction index_function_;
    EqualToFunction equal_to_function_;
};

template <typename Value>
class PostProbingReduceFlushToIndex //TODO(ms): apply spilled files reading
{
public:
    template <typename ReducePostProbingTable>
    void
    operator () (ReducePostProbingTable* ht) const {

        using KeyValuePair = typename ReducePostProbingTable::KeyValuePair;

        auto& vector_ = ht->Items();

        std::vector<Value> elements_to_emit
            (ht->EndLocalIndex() - ht->BeginLocalIndex(), ht->NeutralElement());

        for (size_t i = 0; i < ht->Size(); i++)
        {
            KeyValuePair& current = vector_[i];
            if (current.first != ht->Sentinel().first)
            {
                elements_to_emit[current.first - ht->BeginLocalIndex()] =
                    current.second;

                vector_[i] = ht->Sentinel();
            }
        }

        size_t index = ht->BeginLocalIndex();
        for (auto element_to_emit : elements_to_emit) {
            ht->EmitAll(std::make_pair(index++, element_to_emit));
        }
        assert(index == ht->EndLocalIndex());

        ht->SetNumItems(0);
    }
};

template <bool, typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl;

template <typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl<true, EmitterType, ValueType, SendType>{
    void EmitElement(const ValueType& ele, const std::vector<EmitterType>& emitters) {
        for (auto& emitter : emitters) {
            emitter(ele);
        }
    }
};

template <typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl<false, EmitterType, ValueType, SendType>{
    void EmitElement(const ValueType& ele, const std::vector<EmitterType>& emitters) {
        for (auto& emitter : emitters) {
            emitter(ele.second);
        }
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          const bool ClearAfterFlush = false,
          typename FlushFunction = PostProbingReduceFlushToDefault<Key, ReduceFunction, ClearAfterFlush>,
          typename IndexFunction = PostProbingReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key> >
class ReducePostProbingTable
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const ValueType&)>;

    EmitImpl<SendPair, EmitterFunction, KeyValuePair, ValueType> emit_impl_;

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
     * \param frame_size Number of slots exactly one file writer to be used for.
     * \param equal_to_function Function for checking equality of two keys.
     * \param spill_function Function implementing a strategy to spill items to disk.
     */
    ReducePostProbingTable(Context& ctx,
                           KeyExtractor key_extractor,
                           ReduceFunction reduce_function,
                           std::vector<EmitterFunction>& emit,
                           Key sentinel,
                           const IndexFunction& index_function = IndexFunction(),
                           const FlushFunction& flush_function = FlushFunction(),
                           size_t begin_local_index = 0,
                           size_t end_local_index = 0,
                           Value neutral_element = Value(),
                           size_t byte_size = 1024* 16,
                           double max_frame_fill_rate = 0.5,
                           size_t frame_size = 32, // TODO(ms): use percentage instead
                           const EqualToFunction& equal_to_function = EqualToFunction())
        : byte_size_(byte_size),
          max_frame_fill_rate_(max_frame_fill_rate),
          key_extractor_(key_extractor),
          emit_(std::move(emit)),
          frame_size_(frame_size),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          flush_function_(flush_function),
          begin_local_index_(begin_local_index),
          end_local_index_(end_local_index),
          neutral_element_(neutral_element),
          reduce_function_(reduce_function) {
        sLOG << "creating ReducePostProbingTable with" << emit_.size() << "output emiters";

        assert(byte_size > 0 && "byte_size must be greater than 0");
        assert(max_frame_fill_rate >= 0.0 && max_frame_fill_rate <= 1.0);
        assert(frame_size > 0 && (frame_size & (frame_size - 1)) == 0
               && "frame_size must be a power of two");
        assert(begin_local_index >= 0);
        assert(end_local_index >= 0);

        // TODO(ms): second reduce table is currently not considered for byte_size
        size_ = (size_t)(static_cast<double>(byte_size_) / static_cast<double>(sizeof(KeyValuePair)));
        num_frames_ = (size_t)std::ceil(static_cast<double>(size_) / static_cast<double>(frame_size_));
        items_per_frame_.resize(num_frames_, 0);

        for (size_t i = 0; i < num_frames_; i++) {
            frame_files_.push_back(ctx.GetFile());
        }
        for (size_t i = 0; i < num_frames_; i++) {
            frame_writers_.push_back(frame_files_[i].GetWriter());
        }

        sentinel_ = KeyValuePair(sentinel, Value());
        items_.resize(size_, sentinel_);

        num_items_per_frame_ = (size_t)(static_cast<double>(size_) / static_cast<double>(num_frames_));
    }

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
     * Alternatively, it may trigger a resize of the table in case the maximal fill ratio
     * per partition is reached.
     *
     * \param p Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {

        size_t global_index = index_function_(kv.first, this, size_);

        assert(global_index >= 0 && global_index < size_);

        KeyValuePair* initial = &items_[global_index];
        KeyValuePair* current = initial;
        KeyValuePair* last_item = &items_[size_ - 1];

        size_t frame_id = global_index / frame_size_;

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
                current -= (size_ - 1);
            }
            else
            {
                ++current;
            }

            // spill initial slot, if all the other slots
            // are occupied
            if (current == initial)
            {
                SpillFrame(frame_id);
                current->first = kv.first;
                current->second = kv.second;
                // increase counter for partition
                items_per_frame_[frame_id]++;
                // increase total counter
                num_items_++;
                return;
            }
        }

        if (static_cast<double>(items_per_frame_[frame_id] + 1)
            / static_cast<double>(num_items_per_frame_)
            > max_frame_fill_rate_)
        {
            SpillFrame(frame_id);
        }

        // insert data
        current->first = kv.first;
        current->second = kv.second;

        // increase counter for frame
        items_per_frame_[frame_id]++;
        // increase total counter
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

        size_t offset = frame_id * frame_size_;
        size_t length = (frame_id != num_frames_ - 1) ? offset + frame_size_ : size_;

        for (size_t global_index = offset;
             global_index < length; global_index++)
        {
            KeyValuePair& current = items_[global_index];
            if (current.first != sentinel_.first)
            {
                writer(current);
            }
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
        return num_items_;
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
     * Returns the sentinel element.
     *
     * \return Sentinal element.
     */
    KeyValuePair Sentinel() const {
        return sentinel_;
    }

    /*!
     * Prints content of hash table.
     */
    void Print() {

        std::string log = "Printing\n";

        for (size_t i = 0; i < size_; i++)
        {
            if (items_[i].first == sentinel_.first)
            {
                log += "item: ";
                log += std::to_string(i);
                log += " empty\n";
                continue;
            }

            log += "item: ";
            log += std::to_string(i);
            log += " (";
            // log += std::is_arithmetic<Key>::value || strcmp(typeid(Key).name(), "string")
            //       ? std::to_string(vector_[i].first) : "_";
            log += ", ";
            // log += std::is_arithmetic<Value>::value || strcmp(typeid(Value).name(), "string")
            //       ? std::to_string(vector_[i].second) : "_";
            log += ")\n";
        }

        std::cout << log << std::endl;

        return;
    }

protected:
    //! Size of the table in bytes
    size_t byte_size_ = 0;

    //! Size of the table, which is the number of slots
    //! available for items.
    size_t size_ = 0;

    //! Maximal allowed fill rate per partition
    //! before items get spilled.
    double max_frame_fill_rate_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Set of emitters, one per partition.
    std::vector<EmitterFunction> emit_;

    //! Frame size.
    size_t frame_size_ = 0;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Comparator function for keys.
    FlushFunction flush_function_;

    //! Begin local index (reduce to index).
    size_t begin_local_index_;

    //! End local index (reduce to index).
    size_t end_local_index_;

    //! Neutral element (reduce to index).
    Value neutral_element_;

    //! Keeps the total number of items in the table.
    size_t num_items_ = 0;

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

#endif // !THRILL_CORE_REDUCE_POST_PROBING_TABLE_HEADER

/******************************************************************************/
