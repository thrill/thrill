/*******************************************************************************
 * c7a/core/reduce_PostProbing_probing_table.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_PostProbing_PROBING_TABLE_HEADER
#define C7A_CORE_REDUCE_PostProbing_PROBING_TABLE_HEADER

#include <c7a/common/function_traits.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/data/block_writer.hpp>

#include <algorithm>
#include <cassert>
#include <functional>
#include <iostream>
#include <limits>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>
#include <c7a/data/file.hpp>
#include <math.h>

namespace c7a {
    namespace core {

/**
 *
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
    PostProbingReduceByHashKey(const HashFunction& hash_function = HashFunction())
            : hash_function_(hash_function)
    { }

    template <typename ReducePostProbingTable>
    typename ReducePostProbingTable::index_result
    operator () (Key v, ReducePostProbingTable* ht) const {

        using index_result = typename ReducePostProbingTable::index_result;

        size_t hashed = hash_function_(v);

        size_t global_index = hashed % ht->Size();
        return index_result(global_index);
    }

private:
    HashFunction hash_function_;
};

class PostProbingReduceByIndex
{
public:
    PostProbingReduceByIndex() { }

    template <typename ReducePostProbingTable>
    typename ReducePostProbingTable::index_result
    operator () (size_t key, ReducePostProbingTable* ht) const {

        using index_result = typename ReducePostProbingTable::index_result;

        size_t global_index = (key - ht->BeginLocalIndex()) % ht->Size();
        return index_result(global_index);
    }
};

class PostProbingReduceFlushToDefault
{
public:
    template <typename ReducePostProbingTable>
    void
    operator () (ReducePostProbingTable* ht) const {

        using KeyValuePair = typename ReducePostProbingTable::KeyValuePair;

        using index_result = typename ReducePostProbingTable::index_result;

        // initially, spill some items to make sure enough memory is available
        ht->SpillItems(ht->NumItemsSpillDefault());

        auto &items = ht->Items();

        auto &frame_writers = ht->FrameWriters();

        ht->SetNumItems(0);
    }
};

template <typename Value>
class PostProbingReduceFlushToIndex
{
public:
    template <typename ReducePostProbingTable>
    void
    operator () (ReducePostProbingTable* ht) const {

        using KeyValuePair = typename ReducePostProbingTable::KeyValuePair;

        auto &items_ = ht->Items();

        std::vector<Value> elements_to_emit
                (ht->EndLocalIndex() - ht->BeginLocalIndex(), ht->NeutralElement());

        for (size_t i = 0; i < ht->Size(); i++)
        {
            KeyValuePair current = items_[i];
            if (current.first != ht->Sentinel().first)
            {
                elements_to_emit[current.first - ht->BeginLocalIndex()] =
                        current.second;

                items_[i] = ht->Sentinel();
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
        typename FlushFunction = PostProbingReduceFlushToDefault,
        typename IndexFunction = PostProbingReduceByHashKey<Key>,
        typename EqualToFunction = std::equal_to<Key>
>
class ReducePostProbingTable
{
    static const bool debug = false;

public:
    typedef std::pair<Key, Value> KeyValuePair;

    typedef std::function<void (const ValueType&)> EmitterFunction;

    EmitImpl<SendPair, EmitterFunction, KeyValuePair, ValueType> emit_impl_;

    struct index_result
    {
    public:
        //! index within the whole hashtable
        size_t global_index;

        index_result(size_t g_id) {
            global_index = g_id;
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
     * \param sentinel Sentinel element used to flag free slots.
     * \param num_items_init_scale Used to calculate the initial number of slots
     *                  (num_partitions * num_items_init_scale).
     * \param num_items_resize_scale Used to calculate the number of slots during resize
     *                  (size * num_items_resize_scale).
     * \param max_partition_fill_ratio Used to decide when to resize. If the current number of items
     *                  in some partitions divided by the number of maximal number of items per partition
     *                  is greater than max_partition_fill_ratio, resize.
     * \param max_num_items_table Maximal number of items allowed before some items are flushed. The items
     *                  of the partition with the most items gets flushed.
     * \param index_function Function to be used for computing the slot the item to be inserted.
     * \param equal_to_function Function for checking equality fo two keys.
     */
    ReducePostProbingTable(KeyExtractor key_extractor,
                           ReduceFunction reduce_function,
                           std::vector<EmitterFunction>& emit,
                           Key sentinel,
                           const IndexFunction& index_function = IndexFunction(),
                           const FlushFunction& flush_function = FlushFunction(),
                           size_t begin_local_index = 0,
                           size_t end_local_index = 0,
                           Value neutral_element = Value(),
                           size_t block_size = 10,
                           size_t num_items_init_scale = 10,
                           size_t num_items_resize_scale = 2,
                           double max_items_fill_ratio = 1.0,
                           size_t max_num_items_table = 1048576,
                           size_t frame_size = 1024,
                           const EqualToFunction& equal_to_function = EqualToFunction())
            : key_extractor_(key_extractor),
              reduce_function_(reduce_function),
              emit_(std::move(emit)),
              index_function_(index_function),
              flush_function_(flush_function),
              begin_local_index_(begin_local_index),
              end_local_index_(end_local_index),
              neutral_element_(neutral_element),
              num_items_init_scale_(num_items_init_scale),
              num_items_resize_scale_(num_items_resize_scale),
              max_items_fill_ratio_(max_items_fill_ratio),
              max_num_items_table_(max_num_items_table),
              frame_size_(frame_size),
              equal_to_function_(equal_to_function) {
        assert(num_items_init_scale > 0);
        assert(num_items_resize_scale > 1);
        assert(max_items_fill_ratio >= 0.0 && max_items_fill_ratio <= 1.0);
        assert(max_num_items_table > 0);

        init(sentinel);
    }

    //! non-copyable: delete copy-constructor
    ReducePostProbingTable(const ReducePostProbingTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePostProbingTable& operator = (const ReducePostProbingTable&) = delete;

    ~ReducePostProbingTable() { }

    /**
     * Initializes the data structure by calculating some metrics based on input.
     *
     * \param sentinel Sentinel element used to flag free slots.
     */
    void init(Key sentinel) {

        sLOG << "creating ReducePostProbingTable with" << emit_.size() << "output emiters";

        table_size_ =  num_items_init_scale_;
        sentinel_ = KeyValuePair(sentinel, Value());
        items_.resize(table_size_, sentinel_);

        num_frames_ = (size_t) (static_cast<double>(num_items_)
                                / static_cast<double>(frame_size_));
        frame_writers_.resize(num_frames_);
        // prepare writers
        for (int i = 0; i < num_frames_; i++) {
            data::File file;
            frame_writers_.push_back(file.GetWriter(1024));
        }

        num_items_spill_default_ = frame_size_;

        srand(time(NULL));
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
     * Alternatively, it may trigger a resize of the table in case the maximal fill ratio
     * per partition is reached.
     *
     * \param p Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {

        index_result h = index_function_(kv.first, this);

        assert(h.global_index >= 0 && h.global_index < table_size_);

        KeyValuePair* initial = &items_[h.global_index];
        KeyValuePair* current = initial;
        KeyValuePair* last_item = &items_[table_size_ - 1];

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
                current -= (table_size_ - 1);
            } else
            {
                ++current;
            }

            if (current == initial)
            {
                ResizeUp();
                Insert(std::move(kv));
                return;
            }
        }

        // insert new pair
        if (equal_to_function_(current->first, sentinel_.first))
        {
            current->first = std::move(kv.first);
            current->second = std::move(kv.second);

            // increase total counter
            num_items_++;
        }

        if (num_items_ > max_num_items_table_)
        {
            LOG << "spill";
            SpillItems(num_items_spill_default_);
        }

        if (static_cast<double>(num_items_) /
            static_cast<double>(table_size_)
            > max_items_fill_ratio_)
        {
            LOG << "resize";
            ResizeUp();
        }
    }

    /*!
    * Flushes all items in the whole table.
    */
    void Flush() {
        LOG << "Flushing items";

        flush_function_(this);

        // reset total counter
        num_items_ = 0;

        LOG << "Flushed items";
    }

    /*!
    * Spill items.
    */
    void SpillItems(size_t num_items_spill) {

        // randomly select frame, get frame id
        size_t frame_id = (size_t) (rand() % num_frames_);

        // items spilled
        size_t items_spilled = 0;

        while (items_spilled < num_items_spill) {

            size_t offset = frame_id * frame_size_;
            // spill items
            for (size_t i = offset; i < offset + frame_size_; i++) {

                KeyValuePair current = items_[i];
                if (current.first != sentinel_.first)
                {
                    frame_writers_[i](current);

                    items_[i] = sentinel_.first;
                    items_spilled++;
                }
            }

            frame_id++;
            if (frame_id >= num_frames_) {
                frame_id = 0;
            }
        }

        num_items_ -= items_spilled;
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const KeyValuePair& element) {
        //void EmitAll(const Key& key, const Value& value) {
        emit_impl_.EmitElement(element, emit_);
    }

    /*!
     * Returns the size of the table. The size corresponds to the number of slots.
     * A slot may be free or used.
     *
     * @return Size of the table.
     */
    size_t Size() const {
        return table_size_;
    }

    /*!
     * Returns the total num of items in the table in all partitions.
     *
     * @return Number of items in the table.
     */
    size_t NumItems() const {
        return num_items_;
    }

    /*!
     * Returns the total num of items in the table.
     *
     * @return Number of items in the table.
     */
    void SetNumItems(size_t num_items) {
        num_items_ = num_items;
    }

    /*!
     * Returns the vector of key/value pairs.
     *
     * @return Vector of key/value pairs.
     */
    std::vector<KeyValuePair>& Items() {
        return items_;
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
     * Sets the maximum number of items of the hash table. We don't want to push 2vt
     * elements before flush happens.
     *
     * \param size The maximal number of items the table may hold.
     */
    void SetMaxNumItems(size_t size) {
        max_num_items_table_ = size;
    }

    /*!
     * Returns the begin local index.
     *
     * @return Begin local index.
     */
    size_t BeginLocalIndex() {
        return begin_local_index_;
    }

    /*!
     * Returns the end local index.
     *
     * @return End local index.
     */
    size_t EndLocalIndex() {
        return end_local_index_;
    }

    /*!
     * Returns the neutral element.
     *
     * @return Neutral element.
     */
    Value NeutralElement() {
        return neutral_element_;
    }

    /*!
     * Returns the neutral element.
     *
     * @return Neutral element.
     */
    KeyValuePair Sentinel() {
        return sentinel_;
    }

    /*!
     * Returns the frame size.
     *
     * @return Frame size.
     */
    size_t FrameSize() {
        return frame_size_;
    }

    /*!
     * Returns the number of frames.
     *
     * @return Number of frames.
     */
    size_t NumFrames() {
        return num_frames_;
    }

    /*!
     * Returns the number of frames.
     *
     * @return Number of frames.
     */
    size_t NumItemsSpillDefault() const {
        return num_items_spill_default_;
    }

    /*!
     * Resizes the table by increasing the number of slots using some
     * scale factor (num_items_resize_scale_). All items are rehashed as
     * part of the operation.
     */
    void ResizeUp() {
        LOG << "Resizing";
        table_size_ *= num_items_resize_scale_;
        // reset items_per_partition and table_size
        num_items_ = 0;

        // move old hash array
        std::vector<KeyValuePair> items_old;
        std::swap(items_old, items_);

        // init new hash array
        items_.resize(table_size_, sentinel_);

        // rehash all items in old array
        for (KeyValuePair k_v_pair : items_old)
        {
            if (k_v_pair.first != sentinel_.first)
            {
                Insert(std::move(k_v_pair.second));
            }
        }
        LOG << "Resized";
    }

    /*!
     * Removes all items from the table, but does not flush them nor does
     * it resets the table to it's initial size.
     */
    void Clear() {
        LOG << "Clearing";

        for (KeyValuePair k_v_pair : items_)
        {
            k_v_pair.first = sentinel_.first;
            k_v_pair.second = sentinel_.second;
        }

        num_items_ = 0;
        LOG << "Cleared";
    }

    /*!
     * Removes all items from the table, but does not flush them. However, it does
     * reset the table to it's initial size.
     */
    void Reset() {
        LOG << "Resetting";
        table_size_ = num_items_init_scale_;

        for (KeyValuePair k_v_pair : items_)
        {
            k_v_pair.first = sentinel_.first;
            k_v_pair.second = sentinel_.second;
        }

        items_.resize(table_size_, sentinel_);
        num_items_ = 0;
        LOG << "Resetted";
    }

    /*!
    * Prints content of hash table.
    */
    void Print() {

        std::string log = "Printing\n";

        for (size_t i = 0; i < table_size_; i++)
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
            //log += std::is_arithmetic<Key>::value || strcmp(typeid(Key).name(), "string")
            //       ? std::to_string(items_[i].first) : "_";
            log += ", ";
            //log += std::is_arithmetic<Value>::value || strcmp(typeid(Value).name(), "string")
            //       ? std::to_string(items_[i].second) : "_";
            log += ")\n";
        }

        std::cout << log << std::endl;

        return;
    }

private:
    //! Scale factor to compute the initial size
    //! (=number of slots for items).
    size_t num_items_init_scale_;

    //! Scale factor to compute the number of slots
    //! during resize relative to current size.
    size_t num_items_resize_scale_;

    //! Maximal allowed fill ratio per partition before
    //! resize.
    double max_items_fill_ratio_;

    //! Maximal number of items before some items
    //! are flushed (-> partial flush).
    size_t max_num_items_table_;

    //! Size of the table, which is the number of slots
    //! available for items.
    size_t table_size_ = 0;

    //! Keeps the total number of items in the table.
    size_t num_items_ = 0;

    //! frame size.
    size_t frame_size_ = 0;

    //! Number of frames.
    size_t num_frames_ = 0;

    //! Number of items to spill per spill iteration
    size_t num_items_spill_default_ = 0;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Set of emitters, one per partition.
    std::vector<EmitterFunction> emit_;

    //! Data structure for actually storing the items.
    std::vector<KeyValuePair> items_;

    //! Data structure for actually storing the items.
    std::vector<data::File::Writer> frame_writers_;

    //! Sentinel element used to flag free slots.
    KeyValuePair sentinel_;

    //! Hash functions.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Comparator function for keys.
    FlushFunction flush_function_;

    //! Begin local index (reduce to index)
    size_t begin_local_index_;

    //! End local index (reduce to index)
    size_t end_local_index_;

    //! Neutral element (reduce to index)
    Value neutral_element_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_PostProbing_PROBING_TABLE_HEADER

/******************************************************************************/
