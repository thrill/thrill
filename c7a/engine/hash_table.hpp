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
#include <cstddef>
#include "c7a/api/function_traits.hpp"


namespace c7a {
namespace engine {

struct h_result {
    std::size_t p_id; // which partition
    std::size_t p_offset; // which idx within a partition
    std::size_t global_idx;
};

template <typename key_t, typename value_t>
struct node {
    key_t key;
    value_t value;
    node *next;
};

template<typename KeyExtractor, typename ReduceFunction>
class HashTable
{

static const bool debug = true;

using key_t = typename FunctionTraits<KeyExtractor>::result_type;

using value_t = typename FunctionTraits<ReduceFunction>::result_type;

public:

    HashTable(std::size_t p_n, KeyExtractor key_extractor, ReduceFunction f_reduce) : 
      f_red(f_reduce),
      key_extractor(key_extractor)  
        {
        if (p_n > b_size) {
            throw std::invalid_argument("num partitions must be less than num buckets");
        }

        p_num = p_n;
        //b_size = p_size*10; // scale bucket size based on num of processors TODO: implement resize
        p_size = b_size/p_num;
        p_items_size = new int[p_num];
        for (int i=0; i<p_num; i++) { // TODO: just a tmp fix
            p_items_size[i] = 0;
        }
    }

    ~HashTable() {
    }

    /*!
     * Inserts a key/value pair.
     *
     * Optionally, this may be reduce using the reduce function
     * in case the key already exists.
     */
    void insert(value_t &p)
    {
        key_t key = key_extractor(p);

        h_result h = hash(key);

        LOG << "key: "
            << p.first
            << " to idx: "
            << h.global_idx;

        // bucket is empty
        if (a[h.global_idx] == nullptr) {

            LOG << "bucket empty, inserting...";

            node<key_t, value_t> *n = new node<key_t, value_t>;
            n->key = key;
            n->value = p;
            n->next = nullptr;
            a[h.global_idx] = n;

            // increase counter for partition
            p_items_size[h.p_id]++;

            // increase total counter
            p_items_total_size++;

        // bucket is not empty
        } else {

            LOG << "bucket not empty, checking if key already exists...";

            // check if item with same key
            node<key_t, value_t> *curr_node = a[h.global_idx];
            value_t *curr_value;
            do {
                curr_value = &curr_node->value;
                if (key == curr_node->key) {

                    LOG << "match of key: "
                        << key
                        << " and "
                        << curr_node->key
                        << " ... reducing...";

                    // reduce
                    LOG << "before reduce: "
                        << curr_node->value.second
                        << " and "
                        << p.second;

                    (*curr_node).value = f_red(curr_node->value, p);

                    LOG << "after reduce: "
                        << curr_node->value.second;

                    LOG << "...finished reduce!";

                    break;
                }

                curr_node = curr_node->next;
            } while (curr_node != nullptr);

            // no item found with key
            if (curr_node == nullptr) {

                LOG << "key doesn't exists in bucket, appending...";

                // insert at first pos
                node<key_t, value_t> *n = new node<key_t, value_t>;
                n->key = key;
                n->value = p;
                n->next = a[h.global_idx];
                a[h.global_idx] = n;

                // increase counter for partition
                p_items_size[h.p_id]++;
                // increase total counter
                p_items_total_size++;

                LOG << "key appendend, metrics updated!";
            }
        }
    }

    /*!
     * Returns a vector containing all items belonging to the partition
     * having the most items.
     */
    std::vector<value_t> pop() {

        // get partition with max size
        int p_size_max = 0;
        int p_idx = 0;
        for (int i=0; i<p_num; i++) {
            if (p_items_size[i] > p_size_max) {
                p_size_max = p_items_size[i];
                p_idx = i;
            }
        }

        LOG << "currMax: "
            << p_size_max
            << " currentIdx: "
            << p_idx
            << " currentIdx*p_size: "
            << p_idx*p_size
            << " CurrentIdx*p_size+p_size-1 "
            << p_idx*p_size+p_size-1;

        // retrieve items
        std::vector<value_t> items;
        for (int i=p_idx*p_size; i<=p_idx*p_size+p_size-1; i++) {
            if (a[i] != nullptr) {
                node<key_t, value_t> *curr_node = a[i];
                do {
                    items.push_back(curr_node->value);
                    curr_node = curr_node->next;
                } while (curr_node != nullptr);
                a[i] = nullptr;
            }
        }

        // reset partition specific counter
        p_items_size[p_idx] = 0;
        // reset total counter
        p_items_total_size -=p_size_max;

        return items;
    }

    /*!
     * Returns a map containing all items per partition.
     */
    std::map<int, std::vector<value_t>> erase() {

        // retrieve items
        std::map<int, std::vector<value_t>> items;
        for (int i=0; i<p_num; i++) {
            std::vector<value_t> curr_items;
            for (int j=i*p_size; j<=i*p_size+p_size-1; j++) {
                if (a[i] != nullptr) {
                    items.insert(std::make_pair<int, std::vector<value_t>>(i, curr_items));
                    node<key_t, value_t> *curr_node = a[i];
                    do {
                        curr_items.push_back(curr_node->value);
                        curr_node = curr_node->next;
                    } while (curr_node != nullptr);
                    a[i] = nullptr;
                }
            }

            // set size of partition to 0
            p_items_size[i] = 0;
        }

        // reset counters
        p_items_total_size = 0;

        return items;
    }

    /*!
     * Returns the total num of items.
     */
    std::size_t size() {
        return p_items_total_size;
    }

    void resize() {
        LOG << "to be implemented";
    }

    // prints content of hash table
    void print() {

        for (int i=0; i<b_size; i++) {
            if (a[i] == nullptr) {

                LOG << "bucket "
                    << i
                    << " empty";

            } else {

                std::string log = "";

                // check if item with same key
                node<key_t, value_t> *curr_node = a[i];
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

    int p_num = 0; // partition size

    int p_size = 0; // num buckets per partition

    int* p_items_size; // num items per partition

    int p_items_total_size = 0; // total sum of items

    static const int b_size = 100; // bucket size

    KeyExtractor key_extractor;

    ReduceFunction f_red;

    node<key_t, value_t> *a[b_size] = { nullptr }; // TODO: fix this static assignment

    h_result hash(std::string v) {

        h_result *h = new h_result();

        // partition idx
        std::size_t h1 = std::hash<std::string>()(v);
        h->p_offset = h1 % p_size;

        // partition id
        h->p_id = h->p_offset % p_num;

        // global idx
        h->global_idx = h->p_offset + h->p_id*p_size;

        return *h;
    }
};

}
}

#endif //C7A_HASH_TABLE_HPP

/******************************************************************************/
