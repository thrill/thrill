/*******************************************************************************
 * c7a/core/reduce_post_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_POST_TABLE_HEADER
#define C7A_CORE_REDUCE_POST_TABLE_HEADER

#include <c7a/api/function_traits.hpp>
#include <c7a/data/data_manager.hpp>
#include <c7a/common/logger.hpp>

#include <map>
#include <iostream>
#include <string>
#include <vector>
#include <stdexcept>
#include <array>

namespace c7a {
namespace core {

template <typename KeyExtractor, typename ReduceFunction, typename EmitterFunction>
class ReducePostTable
{
    static const bool debug = false;

    // TODO(ms): according to coding style: Types are CamelCase.
    using key_t = typename FunctionTraits<KeyExtractor>::result_type;

    using value_t = typename FunctionTraits<ReduceFunction>::result_type;

protected:
    template <typename key_t, typename value_t>
    struct node {
        key_t   key;
        value_t value;
        node    * next;
    };

public:
    ReducePostTable(size_t num_buckets, size_t num_buckets_resize_scale,
                    size_t max_num_items_per_bucket, size_t max_num_items_table,
                    KeyExtractor key_extractor, ReduceFunction reduce_function,
                    std::vector<EmitterFunction> emit)
        : num_buckets_init_scale_(num_buckets),
          num_buckets_resize_scale_(num_buckets_resize_scale),
          max_num_items_per_bucket_(max_num_items_per_bucket),
          max_num_items_table_(max_num_items_table),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit) {
        init();
    }

    // TODO(ms): the BlockEmitter must be a plain template like KeyExtractor.
    ReducePostTable(KeyExtractor key_extractor,
                    ReduceFunction reduce_function, std::vector<EmitterFunction> emit)
        : key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit) {
        init();
    }

    ~ReducePostTable() { }

    void init() {
        num_buckets_ = num_buckets_init_scale_;
        vector_.resize(num_buckets_, nullptr);
    }

    /*!
     * Inserts a key/value pair.
     *
     * Optionally, this may be reduce using the reduce function
     * in case the key already exists.
     */
    void Insert(const value_t& p) {
        key_t key = key_extractor_(p);

        size_t hashed_key = std::hash<key_t>() (key) % num_buckets_;

        LOG << "key: "
            << key
            << " to idx: "
            << hashed_key;

        // TODO(ms): the first nullptr case is identical. remove and use null as
        // sentinel.

        // bucket is empty
        if (vector_[hashed_key] == nullptr) {
            LOG << "bucket empty, inserting...";

            node<key_t, value_t>* n = new node<key_t, value_t>;
            n->key = key;
            n->value = p;
            n->next = nullptr;
            vector_[hashed_key] = n;

            // increase total counter
            table_size_++;
        }
        else {
            LOG << "bucket not empty, checking if key already exists...";

            // check if item with same key
            node<key_t, value_t>* curr_node = vector_[hashed_key];
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
                n->next = vector_[hashed_key];
                vector_[hashed_key] = n;

                // increase total counter
                table_size_++;

                LOG << "key appended, metrics updated!";
            }
        }

        if (table_size_ > max_num_items_table_) {
            throw std::invalid_argument("Hashtable overflown. No external memory functionality implemented yet");
        }
    }

    /*!
     * Emits element to all childs
     */
    void EmitAll(value_t& element) {
        for (auto emitter : emit_) {
            emitter(element);
        }
    }
    /*!
     * Flushes all items.
     */
    void Flush() {
        LOG << "Flushing in progress";

        // retrieve items
        for (size_t i = 0; i < num_buckets_; i++) {
            if (vector_[i] != nullptr) {
                node<key_t, value_t>* curr_node = vector_[i];
                do {
                    EmitAll(curr_node->value);
                    curr_node = curr_node->next;
                } while (curr_node != nullptr);
                vector_[i] = nullptr;
            }
        }

        //flush emitters
        //for (auto& emitter : emit_) {
        //TODO call this, as soon as API gives us the 'real' emitter objects: emitter.Flush();
        //}

        // reset counters
        table_size_ = 0;
    }

    /*!
     * Returns the total num of items.
     */
    size_t Size() {
        return table_size_;
    }

    /*!
     * Returns the total num of buckets.
     */
    size_t NumBuckets() {
        return num_buckets_;
    }

    /*!
     * Sets the maximum size of the hash table. We don't want to push 2vt elements before flush happens.
     */
    void SetMaxSize(size_t size) {
        max_num_items_table_ = size;
    }

    /*!
     * Resizes the table by increasing the number of buckets using some
     * resize scale factor. All items are rehashed as part of the operation
     */
    void ResizeUp() {
        LOG << "Resizing";
        num_buckets_ *= num_buckets_resize_scale_;
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
    void Clear() {
        LOG << "Clearing";
        std::fill(vector_.begin(), vector_.end(), nullptr);
        table_size_ = 0;
        LOG << "Cleared";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Reset() {
        LOG << "Resetting";
        num_buckets_ = num_buckets_init_scale_;
        vector_.resize(num_buckets_, nullptr);
        table_size_ = 0;
        LOG << "Resetted";
    }

    // prints content of hash table
    void Print() {
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
                    //log += curr_item.second;
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
    // TODO(ms): reformat using doxygen-style comments

    //! num buckets
    size_t num_buckets_;

    //! set number of buckets per partition based on num_partitions
    size_t num_buckets_init_scale_ = 65536;

    // multiplied with some scaling factor, must be equal to or greater than 1

    size_t num_buckets_resize_scale_ = 2;   // resize scale on max_num_items_per_bucket_

    size_t max_num_items_per_bucket_ = 256; // max num of items per bucket before resize

    size_t table_size_ = 0;                 // total number of items

    size_t max_num_items_table_ = 1048576;  // max num of items before spilling of largest partition

    KeyExtractor key_extractor_;

    ReduceFunction reduce_function_;

    std::vector<EmitterFunction> emit_;

    std::vector<node<key_t, value_t>*> vector_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_POST_TABLE_HEADER

/******************************************************************************/
