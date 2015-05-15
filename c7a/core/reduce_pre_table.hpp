/******************************************************************************
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
#include <array>

#include "c7a/api/function_traits.hpp"
#include "c7a/data/data_manager.hpp"

namespace c7a {
namespace core {
template <typename KeyExtractor, typename ReduceFunction, typename EmitterFunction>
class ReducePreTable
{
    static const bool debug = false;

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

        hash_result(key_t v, const ReducePreTable& ht)
        {
            size_t hashed = std::hash<key_t>() (v);

            LOG << "hashed to " << hashed;

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
    ReducePreTable(size_t num_partitions, size_t num_buckets_init_scale, size_t num_buckets_resize_scale,
                   size_t max_num_items_per_bucket, size_t max_num_items_table,
                   KeyExtractor key_extractor, ReduceFunction reduce_function,
                   std::vector<EmitterFunction> emit)
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
    ReducePreTable(size_t partition_size, KeyExtractor key_extractor,
                   ReduceFunction reduce_function, std::vector<EmitterFunction> emit)
        : num_partitions_(partition_size),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit)
    {
        init();
    }

    ~ReducePreTable() { }

    void init()
    {
        num_buckets_ = num_partitions_ * num_buckets_init_scale_;
        if (num_partitions_ > num_buckets_ &&
            num_buckets_ % num_partitions_ != 0) {
            throw std::invalid_argument("partition_size must be less than or equal to num_buckets "
                                        "AND partition_size a divider of num_buckets");
        }
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;

        vector_.resize(num_buckets_, nullptr);
        items_per_partition_.resize(num_partitions_, 0);
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
        if (vector_[h.global_index] == nullptr) {
            LOG << "bucket empty, inserting...";

            node<key_t, value_t>* n = new node<key_t, value_t>;
            n->key = key;
            n->value = p;
            n->next = nullptr;
            vector_[h.global_index] = n;

            // increase counter for partition
            items_per_partition_[h.partition_id]++;

            // increase total counter
            table_size_++;

            // bucket is not empty
        }
        else {
            LOG << "bucket not empty, checking if key already exists...";

            // check if item with same key
            node<key_t, value_t>* curr_node = vector_[h.global_index];
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
                LOG << "key doesn't exist in baguette, appending...";

                // insert at first pos
                node<key_t, value_t>* n = new node<key_t, value_t>;
                n->key = key;
                n->value = p;
                n->next = vector_[h.global_index];
                vector_[h.global_index] = n;

                // increase counter for partition
                items_per_partition_[h.partition_id]++;
                // increase total counter
                table_size_++;

                LOG << "key appended, metrics updated!";
            }
        }

        if (table_size_ > max_num_items_table_) {
            LOG << "spilling in progress";
            FlushLargestPartition();
        }
    }

    /*!
     * Retrieves all items belonging to the partition
     * having the most items. Retrieved items are then forward
     * to the provided emitter.
     */
    void FlushLargestPartition()
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
            if (vector_[i] != nullptr) {
                node<key_t, value_t>* curr_node = vector_[i];
                do {
                    emit_[p_idx](curr_node->value);
                    curr_node = curr_node->next;
                } while (curr_node != nullptr);
                vector_[i] = nullptr;
            }
        }

        // reset partition specific counter
        items_per_partition_[p_idx] = 0;
        // reset total counter
        table_size_ -= p_size_max;
    }

    /*!
     * Flushes all items.
     */
    void Flush()
    {
        LOG << "Flushing in progress";

        // TODO(ms): this smells like this should be FlushPE(), since same as above.

        // retrieve items
        for (size_t i = 0; i < num_partitions_; i++) {
            for (size_t j = i * num_buckets_per_partition_;
                 j <= i * num_buckets_per_partition_ + num_buckets_per_partition_ - 1; j++) {
                if (vector_[j] != nullptr) {
                    node<key_t, value_t>* curr_node = vector_[j];
                    do {
                        emit_[i](curr_node->value);
                        curr_node = curr_node->next;
                    } while (curr_node != nullptr);
                    vector_[j] = nullptr;
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

    /*!
     * Returns the total num of items.
     */
    size_t NumBuckets()
    {
        return num_buckets_;
    }

    /*!
     * Sets the maximum size of the hash table. We don't want to push 2vt elements before flush happens.
     */
    void SetMaxSize(size_t size)
    {
        max_num_items_table_ = size;
    }

    /*!
     * Resizes the table by increasing the number of buckets using some
     * resize scale factor. All items are rehashed as part of the operation
     */
    void ResizeUp()
    {
        LOG << "Resizing";
        num_buckets_ *= num_buckets_resize_scale_;
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;
        // init new array
        std::vector<node<key_t, value_t>*> vector_old = vector_;
        std::vector<node<key_t, value_t>*> vector_new;
        vector_new.resize(num_buckets_, nullptr);
        vector_ = vector_new;
        // rehash all items in old array
        for (auto bucket : vector_old) {
            Insert(bucket);
        }
        LOG << "Resized";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Clear()
    {
        LOG << "Clearing";
        std::fill(vector_.begin(), vector_.end(), nullptr);
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;
        LOG << "Cleared";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Reset()
    {
        LOG << "Resetting";
        num_buckets_ = num_partitions_ * num_buckets_init_scale_;
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;
        vector_.resize(num_buckets_, nullptr);
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;
        LOG << "Resetted";
    }

    // prints content of hash table
    void Print()
    {
        for (int i = 0; i < num_buckets_; i++) {
            if (vector_[i] == nullptr) {
                LOG << "bucket "
                    << i
                    << " empty";
            }
            else {
                std::string log = "";

                // check if item with same key
                node<key_t, value_t>* curr_node = vector_[i];
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
    size_t num_partitions_;                   // partition size

    size_t num_buckets_;                      // num buckets

    size_t num_buckets_per_partition_;        // num buckets per partition

    size_t num_buckets_init_scale_ = 65536;   // set number of buckets per partition based on num_partitions
    // multiplied with some scaling factor, must be equal to or greater than 1

    size_t num_buckets_resize_scale_ = 2;     // resize scale on max_num_items_per_bucket_

    size_t max_num_items_per_bucket_ = 256;   // max num of items per bucket before resize

    std::vector<size_t> items_per_partition_; // num items per partition

    size_t table_size_ = 0;                   // total number of items

    size_t max_num_items_table_ = 1048576;    // max num of items before spilling of largest partition

    KeyExtractor key_extractor_;

    ReduceFunction reduce_function_;

    std::vector<EmitterFunction> emit_;

    std::vector<node<key_t, value_t>*> vector_;
};
}
}

#endif // !C7A_CORE_HASH_TABLE_HEADER

/******************************************************************************/
