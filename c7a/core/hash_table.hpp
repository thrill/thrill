/*******************************************************************************
 * c7a/core/hashtable.hpp
 *
 * Hash table with support for reduce and partitions.
 ******************************************************************************/

#ifndef C7A_HASH_TABLE_HPP
#define C7A_HASH_TABLE_HPP

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
struct hash_result {
    std::size_t partition_id;     // which partition
    std::size_t partition_offset; // which idx within a partition
    std::size_t global_index;
};

template <typename key_t, typename value_t>
struct node {
    key_t   key;
    value_t value;
    node    * next;
};

template <typename KeyExtractor, typename ReduceFunction>
class HashTable
{
    static const bool debug = true;

    using key_t = typename FunctionTraits<KeyExtractor>::result_type;

    using value_t = typename FunctionTraits<ReduceFunction>::result_type;

public:
    HashTable(std::size_t partition_size, KeyExtractor key_extractor,
              ReduceFunction reduce_function, data::BlockEmitter<value_t> emit) :
        num_partitions_(partition_size),
        key_extractor_(key_extractor),
        reduce_function_(reduce_function),
        emit_(emit)
    {
        if (partition_size > num_buckets_) {
            throw std::invalid_argument("num partitions must be less than num buckets");
        }

        //num_buckets_ = num_partitions_*10; // TODO scale initial bucket size based on num of workers
        buckets_per_part_ = num_buckets_ / num_partitions_;
        items_in_part_ = new int[num_partitions_];
        for (int i = 0; i < num_partitions_; i++) { // TODO: just a tmp fix
            items_in_part_[i] = 0;
        }
    }

    ~HashTable() { }

    /*!
     * Inserts a key/value pair.
     *
     * Optionally, this may be reduce using the reduce function
     * in case the key already exists.
     */
    void Insert(value_t& p)
    {
        key_t key = key_extractor_(p);

        hash_result h = hash(std::to_string(key));

        LOG << "key: "
            << key
            << " to idx: "
            << h.global_index;

        // bucket is empty
        if (a[h.global_index] == nullptr) {
            LOG << "bucket empty, inserting...";

            node<key_t, value_t>* n = new node<key_t, value_t>;
            n->key = key;
            n->value = p;
            n->next = nullptr;
            a[h.global_index] = n;

            // increase counter for partition
            items_in_part_[h.partition_id]++;

            // increase total counter
            total_table_size_++;

            // bucket is not empty
        }
        else {
            LOG << "bucket not empty, checking if key already exists...";

            // check if item with same key
            node<key_t, value_t>* curr_node = a[h.global_index];
            value_t* curr_value;
            do {
                curr_value = &curr_node->value;
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
                n->next = a[h.global_index];
                a[h.global_index] = n;

                // increase counter for partition
                items_in_part_[h.partition_id]++;
                // increase total counter
                total_table_size_++;

                LOG << "key appendend, metrics updated!";
            }
        }

        // TODO should be externally configureably somehow
        if (total_table_size_ > max_table_size_) {
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
        int p_size_max = 0;
        int p_idx = 0;
        for (int i = 0; i < num_partitions_; i++) {
            if (items_in_part_[i] > p_size_max) {
                p_size_max = items_in_part_[i];
                p_idx = i;
            }
        }

        LOG << "currMax: "
            << p_size_max
            << " currentIdx: "
            << p_idx
            << " currentIdx*p_size: "
            << p_idx * buckets_per_part_
            << " CurrentIdx*p_size+p_size-1 "
            << p_idx * buckets_per_part_ + buckets_per_part_ - 1;

        // retrieve items
        for (int i = p_idx * buckets_per_part_; i <= p_idx * buckets_per_part_ + buckets_per_part_ - 1; i++) {
            if (a[i] != nullptr) {
                node<key_t, value_t>* curr_node = a[i];
                do {
                    emit_(curr_node->value);
                    curr_node = curr_node->next;
                } while (curr_node != nullptr);
                a[i] = nullptr;
            }
        }

        // reset partition specific counter
        items_in_part_[p_idx] = 0;
        // reset total counter
        total_table_size_ -= p_size_max;
    }

    /*!
     * Flushes the HashTable after all elements are inserted.  
     */
    void Flush()
    {
        // retrieve items
        std::map<int, std::vector<value_t> > items;
        for (int i = 0; i < num_partitions_; i++) {
            for (int j = i * buckets_per_part_; j <= i * buckets_per_part_ + buckets_per_part_ - 1; j++) {
                if (a[i] != nullptr) {
                    node<key_t, value_t>* curr_node = a[i];
                    do {
                        emit_(curr_node->value);
                        curr_node = curr_node->next;
                    } while (curr_node != nullptr);
                    a[i] = nullptr;
                }
            }

            // set size of partition to 0
            items_in_part_[i] = 0;
        }

        // reset counters
        total_table_size_ = 0;
    }

    /*!
     * Returns the total num of items.
     */
    std::size_t Size()
    {
        return total_table_size_;
    }

    void Resize()
    {
        LOG << "to be implemented";
    }

    // prints content of hash table
    void Print()
    {
        for (int i = 0; i < num_buckets_; i++) {
            if (a[i] == nullptr) {
                LOG << "bucket "
                    << i
                    << " empty";
            }
            else {
                std::string log = "";

                // check if item with same key
                node<key_t, value_t>* curr_node = a[i];
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
    int max_table_size_ = 3;             //maximum number of elements in whole table, spill largest subtable when full

    int num_partitions_ = 0;             // partition size

    int buckets_per_part_ = 0;           // num buckets per partition

    int* items_in_part_;                 // num items per partition

    int total_table_size_ = 0;           // total sum of items

    static const int num_buckets_ = 100; // bucket size

    KeyExtractor key_extractor_;

    ReduceFunction reduce_function_;

    //TODO:Network-Emitter when it's there (:
    data::BlockEmitter<value_t> emit_;

    node<key_t, value_t>* a[num_buckets_] = { nullptr }; // TODO: fix this static assignment

    hash_result hash(std::string v)
    {
        hash_result* h = new hash_result();

        // partition idx
        std::size_t hashed = std::hash<std::string>()(v);
        h->partition_offset = hashed % buckets_per_part_;

        // partition id
        h->partition_id = h->partition_offset % num_partitions_;

        // global idx
        h->global_index = h->partition_offset + h->partition_id * buckets_per_part_;

        return *h;
    }
};
}
}

#endif //C7A_HASH_TABLE_HPP

/******************************************************************************/
