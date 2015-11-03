/*******************************************************************************
 * thrill/core/reduce_pre_probing_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER
#define THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_writer.hpp>

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
class PreProbingReduceByHashKey
{
public:
    explicit PreProbingReduceByHashKey(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePreProbingTable>
    typename ReducePreProbingTable::IndexResult
    operator () (const Key& k, ReducePreProbingTable* ht) const {

        using IndexResult = typename ReducePreProbingTable::IndexResult;

        size_t hashed = hash_function_(k);

        size_t local_index = hashed % ht->NumItemsPerPartition();
        size_t partition_id = hashed % ht->NumPartitions();
        size_t global_index = partition_id *
                              ht->NumItemsPerPartition() +
                              local_index;

        return IndexResult(partition_id, local_index, global_index);
    }

private:
    HashFunction hash_function_;
};

class PreProbingReduceByIndex
{
public:
    size_t size_;

    explicit PreProbingReduceByIndex(size_t size)
        : size_(size)
    { }

    template <typename ReducePreProbingTable>
    typename ReducePreProbingTable::IndexResult
    operator () (const size_t& k, ReducePreProbingTable* ht) const {

        using IndexResult = typename ReducePreProbingTable::IndexResult;

        size_t global_index = k * ht->Size() / size_;
        size_t partition_id = k * ht->NumPartitions() / size_;
        size_t local_index = global_index - partition_id * ht->NumItemsPerPartition();

        return IndexResult(partition_id, local_index, global_index);
    }
};

template <typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename IndexFunction = PreProbingReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>
          >
class ReducePreProbingTable
{
    static const bool debug = false;

public:
    using KeyValuePair = std::pair<Key, Value>;

    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the partition's sub-hashtable of this item
        size_t local_index;
        //! index within the whole hashtable
        size_t global_index;

        IndexResult(size_t p_id, size_t p_off, size_t g_id) {
            partition_id = p_id;
            local_index = p_off;
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
     * \param byte_size Maximal size of the table in byte. In case size of table exceeds that value, items
     *                  are flushed.
     * \param max_partition_fill_rate Maximal number of items per partition relative to number of slots allowed
     *                                to be filled. It the rate is exceeded, items get flushed.
     * \param index_function Function to be used for computing the slot the item to be inserted.
     * \param equal_to_function Function for checking equality fo two keys.
     */
    ReducePreProbingTable(size_t num_partitions,
                          KeyExtractor key_extractor,
                          ReduceFunction reduce_function,
                          std::vector<data::DynBlockWriter>& emit,
                          Key sentinel,
                          size_t byte_size = 1024* 16,
                          double max_partition_fill_rate = 0.5,
                          const IndexFunction& index_function = IndexFunction(),
                          const EqualToFunction& equal_to_function = EqualToFunction())
        : num_partitions_(num_partitions),
          max_partition_fill_rate_(max_partition_fill_rate),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          byte_size_(byte_size),
          emit_(emit),
          index_function_(index_function),
          equal_to_function_(equal_to_function) {
        sLOG << "creating ReducePreProbingTable with" << emit_.size() << "output emiters";

        assert(num_partitions > 0);
        assert(num_partitions == emit.size());
        assert(byte_size > 0 && "byte_size must be greater than 0");
        assert(max_partition_fill_rate >= 0.0 && max_partition_fill_rate <= 1.0);

        size_ = (size_t)(static_cast<double>(byte_size_) / static_cast<double>(sizeof(KeyValuePair)));
        num_items_per_partition_ = (size_t)std::ceil(static_cast<double>(size_) / static_cast<double>(num_partitions));

        for (size_t i = 0; i < emit.size(); i++) {
            emit_stats_.push_back(0);
        }

        // set the key to initial key
        sentinel_ = KeyValuePair(sentinel, Value());
        items_.resize(size_, sentinel_);
        items_per_partition_.resize(num_partitions_, 0);
    }

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
     * \param kv Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {

        IndexResult h = index_function_(kv.first, this);

        assert(h.partition_id >= 0 && h.partition_id < num_partitions_);
        assert(h.local_index >= 0 && h.local_index < num_items_per_partition_);
        assert(h.global_index >= 0 && h.global_index < size_);

        KeyValuePair* initial = &items_[h.global_index];
        KeyValuePair* current = initial;
        size_t num_items_per_partition = (h.partition_id != num_partitions_ - 1) ?
                                         num_items_per_partition_ : size_ - (h.partition_id * num_items_per_partition_);
        KeyValuePair* last_item = &items_[h.global_index - (h.global_index % num_items_per_partition_)
                                          + num_items_per_partition - 1];

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
                current -= (num_items_per_partition - 1);
            }
            else
            {
                ++current;
            }

            // flush partition, if all slots are occupied
            if (current == initial)
            {
                FlushPartition(h.partition_id);
                current->first = kv.first;
                current->second = kv.second;
                // increase counter for partition
                items_per_partition_[h.partition_id]++;
                // increase total counter
                num_items_++;
                return;
            }
        }

        if (static_cast<double>(items_per_partition_[h.partition_id] + 1)
            / static_cast<double>(num_items_per_partition_)
            > max_partition_fill_rate_)
        {
            FlushPartition(h.partition_id);
        }

        // insert new pair
        *current = kv;

        // increase counter for partition
        items_per_partition_[h.partition_id]++;
        // increase total counter
        num_items_++;
    }

    /*!
    * Flushes all items in the whole table.
    */
    void Flush() {
        LOG << "Flushing all items";

        // retrieve items
        for (size_t i = 0; i < num_partitions_; i++)
        {
            FlushPartition(i);
        }

        LOG << "Flushed all items";
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
            if (items_per_partition_[i] > p_size_max)
            {
                p_size_max = items_per_partition_[i];
                p_idx = i;
            }
        }

        LOG << "currMax: "
            << p_size_max
            << " currentIdx: "
            << p_idx
            << " currentIdx*p_size: "
            << p_idx * num_items_per_partition_
            << " CurrentIdx*p_size+p_size-1 "
            << p_idx * num_items_per_partition_ + num_items_per_partition_ - 1;

        LOG << "Largest patition id: "
            << p_idx;

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

        size_t num_items_per_partition = (partition_id != num_partitions_ - 1) ?
                                         num_items_per_partition_ : size_ - (partition_id * num_items_per_partition_);

        for (size_t i = partition_id * num_items_per_partition_;
             i < partition_id * num_items_per_partition_ + num_items_per_partition; i++)
        {
            KeyValuePair& current = items_[i];
            if (current.first != sentinel_.first)
            {
                if (RobustKey) {
                    emit_[partition_id](current.second);
                }
                else {
                    emit_[partition_id](current);
                }

                items_[i] = sentinel_;
            }
        }

        // reset total counter
        num_items_ -= items_per_partition_[partition_id];
        // reset partition specific counter
        items_per_partition_[partition_id] = 0;
        // flush elements pushed into emitter
        emit_[partition_id].Flush();
        // increase flush counter
        num_flushes_++;

        LOG << "Flushed items of partition with id: "
            << partition_id;
    }

    /*!
     * Returns the size of the table. The size corresponds to the number of slots.
     * A slot may be free or occupied by some item.
     *
     * \return Size of the table.
     */
    size_t Size() const {
        return size_;
    }

    /*!
     * Returns the total num of items in the table in all partitions.
     *
     * \return Number of items in the table.
     */
    size_t NumItems() const {
        return num_items_;
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
     * Returns the number of partitions.
     *
     * \return The number of partitions.
     */
    size_t NumPartitions() const {
        return num_partitions_;
    }

    /*!
     * Returns the number of items per partitions.
     *
     * \return The number of items per partitions.
     */
    size_t NumItemsPerPartition() const {
        return num_items_per_partition_;
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

private:
    //! Number of partitions
    size_t num_partitions_;

    //! Maximal allowed fill ratio per partition before
    //! resize.
    double max_partition_fill_rate_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Size of the table in bytes
    size_t byte_size_ = 0;

    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& emit_;

    //! Hash functions.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Size of the table, which is the number of slots
    //! available for items.
    size_t size_ = 0;

    //! Calculated according to size divided by num partitions.
    size_t num_items_per_partition_;

    //! Number of items per partition.
    std::vector<size_t> items_per_partition_;

    //! Emitter stats.
    std::vector<int> emit_stats_;

    //! Data structure for actually storing the items.
    std::vector<KeyValuePair> items_;

    //! Sentinel element used to flag free slots.
    KeyValuePair sentinel_;

    //! Keeps the total number of items in the table.
    size_t num_items_ = 0;

    //! Number of flushes.
    size_t num_flushes_ = 0;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

/******************************************************************************/
