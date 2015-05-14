/*******************************************************************************
 * c7a/core/hash_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_HASH_TABLE_HEADER
#define C7A_CORE_HASH_TABLE_HEADER

#include <map>
#include <iostream>
#include <c7a/common/logger.hpp>
#include <string>
#include <vector>
#include <stdexcept>
//#include <cstddef>

#include "c7a/api/function_traits.hpp"

//TODO:Remove when we have block manager
#include "c7a/data/data_manager.hpp"

namespace c7a {
namespace core {

template <typename KeyExtractor, typename ReduceFunction>
class HashTable
{
    static const bool debug = true;

    using key_t = typename FunctionTraits<KeyExtractor>::result_type;

    using value_t = typename FunctionTraits<ReduceFunction>::result_type;

protected:
    struct hash_result {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the partition's sub-hashtable of this item
        size_t partition_offset;
        //! index within the whole hashtable
        size_t global_index;

        hash_result(key_t v, const HashTable& ht)
        {
            size_t hashed = std::hash<key_t>()(v);

            // partition idx
            partition_offset = hashed % ht.num_buckets_per_partition_;

            // partition id
            partition_id = hashed % ht.num_partitions_;

            // global idx
            global_index = partition_id * ht.num_buckets_per_partition_ + partition_offset;
        }
    };

    template <typename key_t, typename value_t>
    struct node {
        key_t   key;
        value_t value;
        node    * next;
    };

public:

    HashTable(size_t num_partitions, size_t num_buckets_init_scale, size_t num_buckets_resize_scale,
              size_t max_num_items_per_bucket, size_t max_num_items_table,
              KeyExtractor key_extractor, ReduceFunction reduce_function,
              data::BlockEmitter<value_t> emit)
        : num_partitions_(num_partitions),
          num_buckets_init_scale_(num_buckets_init_scale),
          num_buckets_resize_scale_(num_buckets_resize_scale),
          max_num_items_per_bucket_(max_num_items_per_bucket),
          max_num_items_table_(max_num_items_table),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit)
    {
        init();
    }

    // TODO(ms): the BlockEmitter must be a plain template like KeyExtractor.
    HashTable(size_t partition_size, KeyExtractor key_extractor,
              ReduceFunction reduce_function, data::BlockEmitter<value_t> emit)
        : num_partitions_(partition_size),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit)
    {
        init();
    }

    ~HashTable() { }

    void init()
    {
        num_buckets_ = num_partitions_ * num_buckets_init_scale_;
        if (num_partitions_ > num_buckets_ &&
            num_buckets_ % num_partitions_ != 0) {
            throw std::invalid_argument("partition_size must be less than or equal to num_buckets "
                                                "AND partition_size a divider of num_buckets");
        }
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;

        // TODO: Make this work
        // initialize array with size num_partitions_ with nullpt
        //const int n_b_ = 10;
        //node<key_t, value_t>* a[n_b_] = { nullptr };
        //array_ &= a;

        items_per_partition_ = new size_t[num_partitions_];
        for (size_t i = 0; i < num_partitions_; i++) { // TODO: just a tmp fix
            items_per_partition_[i] = 0;
        }
    }

    /*!
     * Inserts a key/value pair.
     *
     * Optionally, this may be reduce using the reduce function
     * in case the key already exists.
     */
    void Insert(const value_t& p)
    {
        key_t key = key_extractor_(p);

        hash_result h(key, *this);

        LOG << "key: "
            << key
            << " to idx: "
            << h.global_index;

        // TODO(ms): the first nullptr case is identical. remove and use null as
        // sentinel.

        // bucket is empty
        if (array_[h.global_index] == nullptr) {
            LOG << "bucket empty, inserting...";

            node<key_t, value_t>* n = new node<key_t, value_t>;
            n->key = key;
            n->value = p;
            n->next = nullptr;
            array_[h.global_index] = n;

            // increase counter for partition
            items_per_partition_[h.partition_id]++;

            // increase total counter
            table_size_++;

            // bucket is not empty
        }
        else {
            LOG << "bucket not empty, checking if key already exists...";

            // check if item with same key
            node<key_t, value_t>* curr_node = array_[h.global_index];
            do {
                if (key == curr_node->key) {
                    LOG << "match of key: "
                        << key
                        << " and "
                        << curr_node->key
                        << " ... reducing...";

                    (*curr_node).value = reduce_function_(curr_node->value, p);

                    LOG << "...finished reduce!";

                    break;
                }

                curr_node = curr_node->next;
            } while (curr_node != nullptr);

            // no item found with key
            if (curr_node == nullptr) {
                LOG << "key doesn't exists in bucket, appending...";

                // insert at first pos
                node<key_t, value_t>* n = new node<key_t, value_t>;
                n->key = key;
                n->value = p;
                n->next = array_[h.global_index];
                array_[h.global_index] = n;

                // increase counter for partition
                items_per_partition_[h.partition_id]++;
                // increase total counter
                table_size_++;

                LOG << "key appendend, metrics updated!";
            }
        }

        if (table_size_ > max_num_items_table_) {
            LOG << "spilling in progress";
            PopLargestSubtable();
        }
    }

    /*!
     * Returns a vector containing all items belonging to the partition
     * having the most items.
     */
    void PopLargestSubtable()
    {
        // get partition with max size
        size_t p_size_max = 0;
        size_t p_idx = 0;
        for (size_t i = 0; i < num_partitions_; i++) {
            if (items_per_partition_[i] > p_size_max) {
                p_size_max = items_per_partition_[i];
                p_idx = i;
            }
        }

        LOG << "currMax: "
            << p_size_max
            << " currentIdx: "
            << p_idx
            << " currentIdx*p_size: "
            << p_idx * num_buckets_per_partition_
            << " CurrentIdx*p_size+p_size-1 "
            << p_idx * num_buckets_per_partition_ + num_buckets_per_partition_ - 1;

        // TODO(ms): use iterators instead of indexes. easier code.

        // retrieve items
        for (size_t i = p_idx * num_buckets_per_partition_;
             i != p_idx * num_buckets_per_partition_ + num_buckets_per_partition_; i++)
        {
            if (array_[i] != nullptr) {
                node<key_t, value_t>* curr_node = array_[i];
                do {
                    emit_(curr_node->value);
                    curr_node = curr_node->next;
                } while (curr_node != nullptr);
                array_[i] = nullptr;
            }
        }

        // reset partition specific counter
        items_per_partition_[p_idx] = 0;
        // reset total counter
        table_size_ -= p_size_max;
    }

    /*!
     * Flushes the HashTable after all elements are inserted.
     */
    void Flush()
    {
        LOG << "Flushing in progress";

        // TODO(ms): this smells like this should be FlushPE(), since same as above.

        // retrieve items
        for (size_t i = 0; i < num_partitions_; i++) {
            for (size_t j = i * num_buckets_per_partition_; j <= i * num_buckets_per_partition_ + num_buckets_per_partition_ - 1; j++) {
                if (array_[j] != nullptr) {
                    node<key_t, value_t>* curr_node = array_[j];
                    do {
                        emit_(curr_node->value);
                        curr_node = curr_node->next;
                    } while (curr_node != nullptr);
                    array_[j] = nullptr;
                }
            }

            // set size of partition to 0
            items_per_partition_[i] = 0;
        }

        // reset counters
        table_size_ = 0;
    }

    /*!
     * Returns the total num of items.
     */
    size_t Size()
    {
        return table_size_;
    }

    void Resize()
    {
        // TODO(ms): make sure that keys still map to the SAME pe.
        LOG << "to be implemented";
    }

    // prints content of hash table
    void Print()
    {
        for (int i = 0; i < num_buckets_; i++) {
            if (array_[i] == nullptr) {
                LOG << "bucket "
                    << i
                    << " empty";
            }
            else {
                std::string log = "";

                // check if item with same key
                node<key_t, value_t>* curr_node = array_[i];
                value_t curr_item;
                do {
                    curr_item = curr_node->value;

                    log += "(";
                    log += curr_item.second;
                    log += ") ";

                    curr_node = curr_node->next;
                } while (curr_node != nullptr);

                LOG << "bucket "
                    << i
                    << ": "
                    << log;
            }
        }

        return;
    }

private:
    size_t num_partitions_ = 0;             // partition size

    size_t num_buckets_ = 0;                // num buckets

    size_t num_buckets_per_partition_ = 0;  // num buckets per partition

    size_t num_buckets_init_scale_ = 2.0;    // set number of buckets per partition based on num_partitions
    // multiplied with some scaling factor, must be equal to or greater than 1

    size_t num_buckets_resize_scale_ = 2.0;  // resize scale on max_num_items_per_bucket_

    size_t max_num_items_per_bucket_ = 10;  // max num of items per bucket before resize

    size_t* items_per_partition_;           // num items per partition

    size_t table_size_ = 0;                 // total number of items

    size_t max_num_items_table_ = 3;             // max num of items before spilling of largest partition

    KeyExtractor key_extractor_;

    ReduceFunction reduce_function_;

    //TODO:Network-Emitter when it's there (:
    data::BlockEmitter<value_t> emit_;

    node<key_t, value_t>* array_[10] = { nullptr }; // TODO: fix this static assignment
};
}
}

#endif // !C7A_CORE_HASH_TABLE_HEADER

/******************************************************************************/
