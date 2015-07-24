/*******************************************************************************
 * c7a/core/reduce_pre_probing_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_PRE_PROBING_TABLE_HEADER
#define C7A_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

#include <c7a/common/function_traits.hpp>
#include <c7a/common/logger.hpp>

#include <limits>
#include <algorithm>
#include <cassert>
#include <string>
#include <utility>
#include <vector>

namespace c7a {
namespace core {
template <typename KeyExtractor, typename ReduceFunction, typename EmitterFunction>
class ReducePreProbingTable
{
    static const bool debug = true;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;

    using Value = typename common::FunctionTraits<ReduceFunction>::result_type;

    typedef std::pair<Key, Value> KeyValuePair;

public:
    struct hash_result
    {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the partition's sub-hashtable of this item
        size_t partition_offset;
        //! index within the whole hashtable
        size_t global_index;

        hash_result(size_t p_id, size_t p_off, size_t g_id) {
            partition_id = p_id;
            partition_offset = p_off;
            global_index = g_id;
        }
    };

public:
    typedef std::function<hash_result(Key, ReducePreProbingTable*)> HashFunction;

    typedef std::function<bool(Key, Key)> EqualToFunction;

    ReducePreProbingTable(size_t num_partitions,
                          size_t num_items_init_scale,
                          size_t num_items_resize_scale,
                          size_t num_collisions_to_resize,
                          double max_partition_fill_ratio,
                          size_t max_num_items_table,
                          KeyExtractor key_extractor, ReduceFunction reduce_function,
                          std::vector<EmitterFunction>& emit,
                          Key sentinel,
                          HashFunction hash_function
                              = [](Key v, ReducePreProbingTable* ht) {
                                    size_t hashed = std::hash<Key>() (v);

                                    size_t partition_offset = hashed %
                                                              ht->num_items_per_partition_;
                                    size_t partition_id = hashed % ht->num_partitions_;
                                    size_t global_index = partition_id *
                                                          ht->num_items_per_partition_ +
                                                          partition_offset;
                                    hash_result hr(partition_id, partition_offset, global_index);
                                    return hr;
                                },
                          EqualToFunction equal_to_function
                            = [](Key k1, Key k2) {
                                return k1 == k2;
                            })
        : num_partitions_(num_partitions),
          num_items_init_scale_(num_items_init_scale),
          num_items_resize_scale_(num_items_resize_scale),
          num_collisions_to_resize_(num_collisions_to_resize),
          max_partition_fill_ratio_(max_partition_fill_ratio),
          max_num_items_table_(max_num_items_table),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)),
          hash_function_(hash_function),
          equal_to_function_(equal_to_function)
    {
        init(sentinel);
    }

    ReducePreProbingTable(size_t num_partitions, KeyExtractor key_extractor,
                          ReduceFunction reduce_function, std::vector<EmitterFunction>& emit,
                          Key sentinel,
                          HashFunction hash_function
                              = [](Key v, ReducePreProbingTable* ht) {
                                    size_t hashed = std::hash<Key>() (v);

                                    size_t partition_offset = hashed %
                                                              ht->num_items_per_partition_;
                                    size_t partition_id = hashed % ht->num_partitions_;
                                    size_t global_index = partition_id *
                                                          ht->num_items_per_partition_ +
                                                          partition_offset;
                                    hash_result hr(partition_id, partition_offset, global_index);
                                    return hr;
                                },
                          EqualToFunction equal_to_function
                                = [](Key k1, Key k2) {
                                    return k1 == k2;
                                })
        : num_partitions_(num_partitions),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)),
          hash_function_(hash_function),
          equal_to_function_(equal_to_function)
    {
        init(sentinel);
    }

    //! non-copyable: delete copy-constructor
    ReducePreProbingTable(const ReducePreProbingTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreProbingTable& operator = (const ReducePreProbingTable&) = delete;

    ~ReducePreProbingTable() { }

    void init(Key sentinel) {

        sLOG << "creating ReducePreProbingTable with" << emit_.size() << "output emiters";
        for (size_t i = 0; i < emit_.size(); i++)
            emit_stats_.push_back(0);

        table_size_ = num_partitions_ * num_items_init_scale_;
        if (num_partitions_ > table_size_ &&
                table_size_ % num_partitions_ != 0) {
            throw std::invalid_argument("partition_size must be less than or equal to num_items "
                                        "AND partition_size a divider of num_items");
        }
        num_items_per_partition_ = table_size_ / num_partitions_;

        // set the key to initial key
        sentinel_ = KeyValuePair(sentinel, Value());
        vector_.resize(table_size_, sentinel_);
        items_per_partition_.resize(num_partitions_, 0);
    }

    /*!
     * Inserts a key/value pair.
     *
     * Optionally, this may be reduce using the reduce function
     * in case the key already exists.
     */
    void Insert(const Value& p) {
        Key key = key_extractor_(p);

        hash_result h = hash_function_(key, this);

        assert(h.partition_id >= 0 && h.partition_id < num_partitions_);
        assert(h.partition_offset >= 0 && h.partition_offset < num_items_per_partition_);
        assert(h.global_index >= 0 && h.global_index < table_size_);

        //std::cout << key << " " << h.partition_id << " " << h.partition_offset << " " << h.global_index << std::endl;

        int pos = h.global_index;
        size_t pos_offset = 0;

        // REVIEW(ms): try to make the loop tighter, remove extra variables and
        // try to reduce the number of +/-/< operations, have only current + end
        // iterators.
        KeyValuePair* current = &vector_[pos];

        while (!equal_to_function_(current->first, sentinel_.first))
        {
            if (equal_to_function_(current->first, key))
            {
                LOG << "match of key: " << key
                    << " and " << current->first << " ... reducing...";

                current->second = reduce_function_(current->second, p);

                LOG << "...finished reduce!";
                return;
            }

            ++pos_offset;

            if (pos_offset > num_collisions_to_resize_ || pos_offset >= num_items_per_partition_)
            {
                ResizeUp();
                Insert(std::move(p));
                return;
            }

            if (h.partition_offset + pos_offset >= num_items_per_partition_)
            {
                pos -= (h.partition_offset + pos_offset);
            }

            //std::cout << "7" << std::endl;
            //std::cout << pos + pos_offset << std::endl;

            current = &vector_[pos + pos_offset];
        }

        // insert new pair
        if (current->first == sentinel_.first)
        {
            vector_[pos + pos_offset] = KeyValuePair(key, p);

            // increase total counter
            num_items_++;

            // increase counter for partition
            items_per_partition_[h.partition_id]++;
        }

        if (num_items_ > max_num_items_table_)
        {
            LOG << "flush";
            FlushLargestPartition();
        }

        //std::cout << key << std::endl;
        //std::cout << items_per_partition_[h.partition_id] / num_items_per_partition_ << std::endl;
        //std::cout << max_partition_fill_ratio_ << std::endl;

        if ((float)items_per_partition_[h.partition_id]
            / (float)num_items_per_partition_ > max_partition_fill_ratio_)
        {
            std::cout << "resize" << std::endl;
            ResizeUp();
        }
    }

    /*!
    * Flushes all items.
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
     * having the most items. Retrieved items are then forward
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
     */
    void FlushPartition(size_t partition_id) {
        LOG << "Flushing items of partition with id: "
            << partition_id;

        for (size_t i = partition_id * num_items_per_partition_;
             i < partition_id * num_items_per_partition_ + num_items_per_partition_; i++)
        {
            KeyValuePair current = vector_[i];
            if (current.first != sentinel_.first)
            {
                emit_[partition_id](std::move(current.second));
                vector_[i] = sentinel_;
            }
        }

        // reset total counter
        num_items_ -= items_per_partition_[partition_id];
        // reset partition specific counter
        items_per_partition_[partition_id] = 0;
        // flush elements pushed into emitter
        emit_[partition_id].Flush();

        LOG << "Flushed items of partition with id: "
            << partition_id;
    }

    /*!
     * Returns the size of the table.
     */
    size_t Size() {
        return table_size_;
    }

    /*!
     * Returns the total num of items in table in all partitions.
     */
    size_t NumItems() {
        return num_items_;
    }

    /*!
     * Returns the number of buckets per partition.
     */
    size_t NumItemsPerPartition() {
        return num_items_per_partition_;
    }

    /*!
     * Returns the number of partitions.
     */
    size_t NumPartitions() {
        return num_partitions_;
    }

    /*!
     * Returns the size of a partition referenzed by partition_id.
     */
    size_t PartitionSize(size_t partition_id) {
        return items_per_partition_[partition_id];
    }

    /*!
     * Sets the maximum size of the hash table. We don't want to push 2vt
     * elements before flush happens.
     */
    void SetMaxSize(size_t size) {
        max_num_items_table_ = size;
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
     * Resizes the table by increasing the number of items using some
     * resize scale factor. All items are rehashed as part of the operation.
     */
    void ResizeUp() {
        std::cout << "Resizing" << std::endl;
        table_size_ *= num_items_resize_scale_;
        num_items_per_partition_ = table_size_ / num_partitions_;
        // reset items_per_partition and table_size
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        num_items_ = 0;

        // move old hash array
        std::vector<KeyValuePair> vector_old;
        std::swap(vector_old, vector_);

        // init new hash array
        vector_.resize(table_size_, sentinel_);

        // rehash all items in old array
        for (KeyValuePair k_v_pair : vector_old)
        {
            KeyValuePair current = k_v_pair;
            if (current.first != sentinel_.first)
            {
                Insert(std::move(current.second));
            }
        }
        std::cout << "Resized" << std::endl;
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Clear() {
        LOG << "Clearing";

        for (KeyValuePair k_v_pair : vector_)
        {
            k_v_pair = sentinel_; // TODO(ms): fix, doesnt work
        }

        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        num_items_ = 0;
        LOG << "Cleared";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Reset() {
        LOG << "Resetting";
        table_size_ = num_partitions_ * num_items_init_scale_;
        num_items_per_partition_ = table_size_ / num_partitions_;

        for (KeyValuePair k_v_pair : vector_)
        {
            k_v_pair = sentinel_; // TODO(ms): fix, doesnt work
        }

        vector_.resize(table_size_, sentinel_);
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
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
            if (vector_[i].first == sentinel_.first)
            {
                log += "item: ";
                log += std::to_string(i);
                log += " empty\n";
                continue;
            }

            log += "item: ";
            log += std::to_string(i);
            log += " (";
            log += vector_[i].first;
            log += ", ";
            // REVIEW(ms): about Value -> String: you cannot in general.

            //log += vector_[i].second; // TODO(ms): How to convert Value to a string?
            log += ")\n";
        }

        std::cout << log << std::endl;

        return;
    }

private:
    // REVIEW(ms): use doxygen format like everyone else! which of these are
    // static const?

    size_t num_partitions_;                         // partition size

    size_t num_items_init_scale_ = 10;              // set number of items per partition based on num_partitions
    // multiplied with some scaling factor, must be equal to or greater than 1

    size_t num_items_resize_scale_ = 2;             // resize scale triggered by max_partition_fill_ratio_

    size_t num_collisions_to_resize_ = std::numeric_limits<size_t>::max();    // max num of collisions before resize

    double max_partition_fill_ratio_ = 1.0;         // max partition fill ratio before resize

    size_t max_num_items_table_ = 1048576;          // max num of items before spilling of largest partition

    size_t num_items_ = 0;                          // num items in the table

    size_t num_items_per_partition_;                // num items per partition

    std::vector<size_t> items_per_partition_;       // current number of items per partition

    size_t table_size_ = 0;                         // number of slots

    KeyExtractor key_extractor_;

    ReduceFunction reduce_function_;

    std::vector<EmitterFunction> emit_;
    std::vector<int> emit_stats_;

    std::vector<KeyValuePair> vector_;

    KeyValuePair sentinel_;

    HashFunction hash_function_;

    EqualToFunction equal_to_function_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

/******************************************************************************/
