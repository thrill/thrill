/*******************************************************************************
 * thrill/core/reduce_pre_probing_table.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER
#define THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/core/pre_probing_reduce_by_hash_key.hpp>
#include <thrill/core/pre_probing_reduce_by_index.hpp>
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

//template <bool, typename EmitterFunction, typename Key, typename Value, typename SendType>
//struct EmitImpl;
//
//template <typename EmitterFunction, typename Key, typename Value, typename SendType>
//struct EmitImpl<true, EmitterFunction, Key, Value, SendType>{
//    void EmitElement(const Key& k, const Value& v, EmitterFunction emit) {
//        emit(std::make_pair(k, v));
//    }
//};
//
//template <typename EmitterFunction, typename Key, typename Value, typename SendType>
//struct EmitImpl<false, EmitterFunction, Key, Value, SendType>{
//    void EmitElement(const Key& k, const Value& v, EmitterFunction emit) {
//        (void)k;
//        emit(v);
//    }
//};

template <typename ValueType, typename Key, typename Value,
        typename KeyExtractor, typename ReduceFunction,
        const bool RobustKey = false,
        typename FlushFunction = PostProbingReduceFlush<Key, Value, ReduceFunction>,
        typename IndexFunction = PreProbingReduceByHashKey<Key>,
        typename EqualToFunction = std::equal_to<Key>,
        const bool FullPreReduce = false>
class ReducePreProbingTable
{
    static const bool debug = false;

    static const bool bench = true;

    static const bool emit = true;

public:
    using KeyValuePair = std::pair<Key, Value>;

    //EmitImpl<RobustKey, EmitterFunction, Key, Value, ValueType> emit_impl_;

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
     * \param max_partition_fill_rate Maximal number of items per partition relative to number of slots allowed to be filled.
     *                            It the rate is exceeded, items get spilled to disk.
     * \param partition_rate Rate of number of buckets to number of partitions. There is one file writer per partition.
     * \param equal_to_function Function for checking equality of two keys.
     * \param spill_function Function implementing a strategy to spill items to disk.
     */
    ReducePreProbingTable(Context& ctx,
                           size_t num_partitions,
                           const KeyExtractor& key_extractor,
                           const ReduceFunction& reduce_function,
                           std::vector<data::DynBlockWriter>& emit,
                           const Key& sentinel,
                           const IndexFunction& index_function,
                           const FlushFunction& flush_function,
                           size_t byte_size = 1024 * 16,
                           double max_partition_fill_rate = 0.5,
                           const EqualToFunction& equal_to_function = EqualToFunction())
            : ctx_(ctx),
              num_partitions_(num_partitions),
              byte_size_(byte_size),
              max_partition_fill_rate_(max_partition_fill_rate),
              key_extractor_(key_extractor),
              emit_(emit),
              index_function_(index_function),
              equal_to_function_(equal_to_function),
              flush_function_(flush_function),
              reduce_function_(reduce_function)
    {
        assert(num_partitions > 0);
        assert(num_partitions == emit.size());
        assert(byte_size >= 0 && "byte_size must be greater than or equal to 0. "
                "a byte size of zero results in exactly one item per partition");
        assert(max_partition_fill_rate >= 0.0 && max_partition_fill_rate <= 1.0 && "max_partition_fill_rate "
                "must be between 0.0 and 1.0. with a fill rate of 0.0, items are immediately flushed.");

        num_items_per_partition_ = std::max<size_t>((size_t)(((byte_size_ * (1 - table_rate_))
                                                 / static_cast<double>(sizeof(KeyValuePair)))
                                                / static_cast<double>(num_partitions_)), 1);

        size_ = num_items_per_partition_ * num_partitions_;

        fill_rate_num_items_per_partition_ = (size_t)(num_items_per_partition_ * max_partition_fill_rate_);

        assert(num_partitions_ > 0);
        assert(num_items_per_partition_ > 0);
        assert(size_ > 0);
        assert(fill_rate_num_items_per_partition_ >= 0);

        items_per_partition_.resize(num_partitions_, 0);

        for (size_t i = 0; i < num_partitions_; i++) {
            partition_files_.push_back(ctx.GetFile());
        }
        for (size_t i = 0; i < num_partitions_; i++) {
            partition_writers_.push_back(partition_files_[i].GetWriter());
        }

        sentinel_ = KeyValuePair(sentinel, Value());
        items_.resize(size_, sentinel_);

        // set up second table
        second_table_size_ = std::max<size_t>((size_t)((byte_size_ * table_rate_)
                                                       / static_cast<double>(sizeof(KeyValuePair))), 1);

        fill_rate_num_items_second_reduce_ = (size_t)(second_table_size_ * max_partition_fill_rate_);

        // ensure size of second table is even, in order to be able to split by half for spilling
        if (second_table_size_ % 2 != 0) {
            second_table_size_--;
        }
        second_table_size_ = std::max<size_t>(2, second_table_size_);

        assert(second_table_size_ > 0);

        second_table_.resize(second_table_size_, sentinel_);

        for (size_t i = 0; i < emit.size(); i++) {
            emit_stats_.push_back(0);
        }
    }

    ReducePreProbingTable(Context& ctx, size_t num_partitions, KeyExtractor key_extractor,
                           ReduceFunction reduce_function, std::vector<data::DynBlockWriter>& emit, const Key& sentinel)
            : ReducePreProbingTable(ctx, num_partitions, key_extractor, reduce_function, emit, sentinel, IndexFunction(),
                                     FlushFunction(reduce_function)) { }

    //! non-copyable: delete copy-constructor
    ReducePreProbingTable(const ReducePreProbingTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreProbingTable& operator = (const ReducePreProbingTable&) = delete;

    ~ReducePreProbingTable() { }

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

        typename IndexFunction::IndexResult h = index_function_(kv.first, this);

        assert(h.partition_id >= 0 && h.partition_id < num_partitions_);
        assert(h.global_index >= 0 && h.global_index < size_);

        KeyValuePair *initial = &items_[h.global_index];
        KeyValuePair *current = initial;
        KeyValuePair *last_item = &items_[h.global_index - (h.global_index % num_items_per_partition_)
                                          + num_items_per_partition_ - 1];

        while (!equal_to_function_(current->first, sentinel_.first)) {
            if (equal_to_function_(current->first, kv.first)) {
                LOG << "match of key: " << kv.first
                << " and " << current->first << " ... reducing...";

                current->second = reduce_function_(current->second, kv.second);

                LOG << "...finished reduce!";
                return;
            }

            if (bench) {
                num_collisions_++;
            }

            if (current == last_item) {
                current -= (num_items_per_partition_ - 1);
            }
            else {
                ++current;
            }

            // flush partition, if all slots are reserved
            if (current == initial) {

                if (FullPreReduce) {
                    SpillPartition(h.partition_id);
                } else {
                    FlushPartition(h.partition_id);
                }

                current->first = kv.first;
                current->second = kv.second;
                // increase counter for partition
                items_per_partition_[h.partition_id]++;

                return;
            }
        }

        // insert new pair
        *current = kv;
        // increase counter for partition
        items_per_partition_[h.partition_id]++;

        if (items_per_partition_[h.partition_id] > fill_rate_num_items_per_partition_)
        {
            if (FullPreReduce) {
                SpillPartition(h.partition_id);
            } else {
                FlushPartition(h.partition_id);
            }
        }
    }

    /*!
     * Spill partition of certain partition id.
     */
    void SpillPartition(size_t partition_id) {

        data::File::Writer& writer = partition_writers_[partition_id];

        size_t offset = partition_id * num_items_per_partition_;
        size_t length = offset + num_items_per_partition_;

        for (size_t global_index = offset;
             global_index < length; global_index++)
        {
            KeyValuePair& current = items_[global_index];
            if (current.first != sentinel_.first)
            {
                if (RobustKey) {
                    if (emit) {
                        writer.PutItem(current.second);
                    }
                }
                else {
                    if (emit) {
                        writer.PutItem(current);
                    }
                }

                items_[global_index] = sentinel_;
            }
        }

        // reset partition specific counter
        items_per_partition_[partition_id] = 0;

        if (bench) {
            // increase spill counter
            num_spills_++;
        }
    }

    /*!
     * Flush.
     */
    void Flush(bool consume = false) {

        if (FullPreReduce) {
            flush_function_(consume, this);

        } else {

            for (size_t i = 0; i < num_partitions_; i++)
            {
                FlushPartition(i);
            }
        }
    }

    /*!
     * Flushes all items of a partition.
     *
     * \param partition_id The id of the partition to be flushed.
     */
    void FlushPartition(size_t partition_id) {
        LOG << "Flushing items of partition with id: "
        << partition_id;

        for (size_t i = partition_id * num_items_per_partition_;
             i < partition_id * num_items_per_partition_ + num_items_per_partition_; i++)
        {
            KeyValuePair& current = items_[i];
            if (current.first != sentinel_.first)
            {
                if (RobustKey) {
                    if (emit) {
                        emit_[partition_id](current.second);
                    }
                }
                else {
                    if (emit) {
                        emit_[partition_id](current);
                    }
                }
                emit_stats_[partition_id]++;

                items_[i] = sentinel_;
            }
        }

        // reset partition specific counter
        items_per_partition_[partition_id] = 0;
        // flush elements pushed into emitter
        emit_[partition_id].Flush();

        if (bench) {
            // increase flush counter
            num_flushes_++;
        }

        LOG << "Flushed items of partition with id: "
        << partition_id;
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const Key& k, const Value& v) {
        (void)k;
        (void)v;
       // emit_impl_.EmitElement(k, v, emit_);
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
        for (size_t num_items : items_per_partition_) {
            total_num_items += num_items;
        }

        return total_num_items;
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
     * Returns the number of partitions.
     *
     * \return The number of partitions.
     */
    size_t NumFrames() const {
        return num_partitions_;
    }

    /*!
     * Returns the vector of partition files.
     *
     * \return Begin local index.
     */
    std::vector<data::File> & FrameFiles() {
        return partition_files_;
    }

    /*!
     * Returns the vector of partition writers.
     *
     * \return Vector of partition writers.
     */
    std::vector<data::File::Writer> & FrameWriters() {
        return partition_writers_;
    }

    /*!
     * Returns the vector of number of items per partition.
     *
     * \return Vector of number of items per partition.
     */
    std::vector<size_t> & NumItemsPerFrame() {
        return items_per_partition_;
    }

    /*!
     * Returns the vector of number of items per partition.
     *
     * \return Vector of number of items per partition.
     */
    size_t FrameSize() {
        return num_items_per_partition_;
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
     * Returns the sentinel element.
     *
     * \return Sentinal element.
     */
    KeyValuePair Sentinel() const {
        return sentinel_;
    }

    /*!
     * Returns the partition size.
     *
     * \return Frame size.
     */
    size_t FrameSize() const {
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
     * Returns the vector of key/value pairs.
     *
     * \return Vector of key/value pairs.
     */
    std::vector<KeyValuePair> & SecondTable() {
        return second_table_;
    }

    /*!
    * Returns the number of items of a partition.
    *
    * \param partition_id The id of the partition the number of
    *                  items to be returned..
    * \return The number of items in the partitions.
    */
    size_t PartitionNumItems(size_t partition_id) {
        return items_per_partition_[partition_id];
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
    * Closes all emitter.
    */
    void CloseEmitter() {
        sLOG << "emit stats: ";
        unsigned int i = 0;
        for (auto& e : emit_) {
            e.Close();
            sLOG << "emitter " << i << " pushed " << emit_stats_[i++];
        }
    }

private:
    //! Context
    Context& ctx_;

    //! Number of partitions.
    size_t num_partitions_ = 1;

    //! Size of the table in bytes
    size_t byte_size_ = 0;

    //! Size of the table, which is the number of slots
    //! available for items.
    size_t size_ = 0;

    //! Maximal allowed fill rate per partition
    //! before items get spilled.
    double max_partition_fill_rate_ = 1.0;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& emit_;

    //! Frame size.
    size_t num_items_per_partition_ = 0;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Comparator function for keys.
    FlushFunction flush_function_;

    //! Emitter stats.
    std::vector<int> emit_stats_;

    //! Storing the items.
    std::vector<KeyValuePair> items_;

    //! Store the files for partitions.
    std::vector<data::File> partition_files_;

    //! Store the writers for partitions.
    std::vector<data::File::Writer> partition_writers_;

    //! Sentinel element used to flag free slots.
    KeyValuePair sentinel_;

    //! Number of items per partition.
    std::vector<size_t> items_per_partition_;

    //! Total num of spills.
    size_t num_spills_ = 0;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Rate of sizes of primary to secondary table.
    double table_rate_ = 0.0;

    //! Storing the secondary table.
    std::vector<KeyValuePair> second_table_;

    //! Number of items per partition considering fill rate.
    size_t fill_rate_num_items_per_partition_ = 0;

    //! Size of the second table.
    size_t second_table_size_ = 0;

    //! Number of flushes.
    size_t num_flushes_ = 0;

    //! Number of collisions.
    size_t num_collisions_ = 0;

    size_t fill_rate_num_items_second_reduce_ = 0;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

/******************************************************************************/
