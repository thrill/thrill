/*******************************************************************************
 * c7a/core/reduce_post_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_POST_TABLE_HEADER
#define C7A_CORE_REDUCE_POST_TABLE_HEADER

#include <c7a/common/function_traits.hpp>
#include <c7a/common/logger.hpp>

#include <string>
#include <utility>
#include <vector>

namespace c7a {
namespace core {

template <bool, typename T, typename Value, typename Table>
struct FlushImpl;

template <typename T, typename Value, typename Table>
struct FlushImpl<true, T, Value, Table>{
    void FlushToAll(std::vector<T>* vector_, size_t num_buckets_,
                    size_t begin_local_index_, size_t end_local_index_,
                    Table* table, Value neutral_element_) {
        // retrieve items

        std::vector<Value> elements_to_emit
            (end_local_index_ - begin_local_index_, neutral_element_);

        for (size_t i = 0; i < num_buckets_; i++) {
            if ((*vector_)[i] != nullptr) {
                T curr_node = (*vector_)[i];
                do {
                    elements_to_emit[curr_node->key - begin_local_index_] =
                        curr_node->value;
                    auto del = curr_node;
                    curr_node = curr_node->next;
                    delete del;
                } while (curr_node != nullptr);

                (*vector_)[i] = nullptr;                         //TODO(ms) I can't see deallocation of the nodes. Is that done somewhere else?
            }
        }
        for (auto element_to_emit : elements_to_emit) {
            table->EmitAll(element_to_emit);
        }
    }
};

template <typename T, typename Value, typename Table>
struct FlushImpl<false, T, Value, Table>{
    void FlushToAll(std::vector<T>* vector_, size_t num_buckets_,
                    size_t, size_t, Table* table, Value) {
        for (size_t i = 0; i < num_buckets_; i++) {
            if ((*vector_)[i] != nullptr) {
                T curr_node = (*vector_)[i];
                do {
                    table->EmitAll(curr_node->value);
                    auto del = curr_node;
                    curr_node = curr_node->next;
                    delete del;
                } while (curr_node != nullptr);

                (*vector_)[i] = nullptr;                         //TODO(ms) I can't see deallocation of the nodes. Is that done somewhere else?
            }
        }
    }
};

template <typename KeyExtractor, typename ReduceFunction, const bool ToIndex = false>
class ReducePostTable
{
public:
    static const bool debug = false;

    using Key = typename common::FunctionTraits<KeyExtractor>::result_type;

    using Value = typename common::FunctionTraits<ReduceFunction>::result_type;

    using KeyValuePair = std::pair<Key, Value>;

    using EmitterFunction = std::function<void(const Value&)>;

protected:
    template <typename Key, typename Value>
    struct node {
        Key   key;
        Value value;
        node  * next;
    };

public:
    typedef std::function<size_t(Key, ReducePostTable*)> HashFunction;

    ReducePostTable(size_t num_buckets, size_t num_buckets_resize_scale,
                    size_t max_num_items_per_bucket, size_t max_num_items_table,
                    KeyExtractor key_extractor, ReduceFunction reduce_function,
                    std::vector<EmitterFunction>& emit,
                    HashFunction hash_function = [](Key key,
                                                    ReducePostTable* pt) {
                                                     return std::hash<Key>() (key) % pt->num_buckets_;
                                                 },
                    size_t begin_local_index = 0,
                    size_t end_local_index = 0,
                    Value neutral_element = Value()
                    )
        : num_buckets_init_scale_(num_buckets),
          num_buckets_resize_scale_(num_buckets_resize_scale),
          max_num_items_per_bucket_(max_num_items_per_bucket),
          max_num_items_table_(max_num_items_table),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)),
          hash_function_(hash_function),
          begin_local_index_(begin_local_index),
          end_local_index_(end_local_index),
          neutral_element_(neutral_element)
    {
        init();
    }

    ReducePostTable(KeyExtractor key_extractor,
                    ReduceFunction reduce_function,
                    std::vector<EmitterFunction>& emit,
                    HashFunction hash_function = [](Key key,
                                                    ReducePostTable* pt) {
                                                     return std::hash<Key>() (key) % pt->num_buckets_;
                                                 },
                    size_t begin_local_index = 0,
                    size_t end_local_index = 0,
                    Value neutral_element = Value()
                    )
        : key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)),
          hash_function_(hash_function),
          begin_local_index_(begin_local_index),
          end_local_index_(end_local_index),
          neutral_element_(neutral_element)
    {
        init();
    }

    ~ReducePostTable() { }

    void init() {
        num_buckets_ = num_buckets_init_scale_;
        vector_.resize(num_buckets_, nullptr);
    }

    void Insert(const Value& p) {
        Key key = key_extractor_(p);
        Insert(std::make_pair(key, p));
    }

    /*!
     * Inserts a key/value pair.
     *
     * Optionally, this may be reduce using the reduce function
     * in case the key already exists.
     */
    void Insert(const KeyValuePair& p) {

        size_t hashed_key = hash_function_(p.first, this);

        LOG << "key: "
            << p.first
            << " to idx: "
            << hashed_key;

        // TODO(ms): the first nullptr case is identical. remove and use null as
        // sentinel.

        assert(hashed_key < vector_.size());

        // bucket is empty
        if (vector_[hashed_key] == nullptr) {
            LOG << "bucket empty, inserting...";

            node<Key, Value>* n = new node<Key, Value>;
            n->key = p.first;
            n->value = p.second;
            n->next = nullptr;
            vector_[hashed_key] = n;

            // increase total counter
            table_size_++;
        }
        else {
            LOG << "bucket not empty, checking if key already exists...";

            // check if item with same key
            node<Key, Value>* curr_node = vector_[hashed_key];
            do {
                if (p.first == curr_node->key) {
                    LOG << "match of key: "
                        << p.first
                        << " and "
                        << curr_node->key
                        << " ... reducing...";

                    (*curr_node).value = reduce_function_(curr_node->value, p.second);

                    LOG << "...finished reduce!";

                    break;
                }

                curr_node = curr_node->next;
            } while (curr_node != nullptr);

            // no item found with key
            if (curr_node == nullptr) {
                LOG << "key doesn't exist, appending...";

                // insert at first pos
                node<Key, Value>* n = new node<Key, Value>;
                n->key = p.first;
                n->value = p.second;
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
    void EmitAll(Value& element) {
        for (auto& emitter : emit_) {
            emitter(element);
        }
    }

    /*!
     * Flushes all items.
     */
    void Flush() {

        FlushImpl<ToIndex, node<Key, Value>*, Value,
                  ReducePostTable<KeyExtractor, ReduceFunction, ToIndex> > flush_impl;
        flush_impl.FlushToAll(&vector_, num_buckets_, begin_local_index_,
                              end_local_index_, this, neutral_element_);
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
        std::vector<node<Key, Value>*> vector_old = vector_;
        std::vector<node<Key, Value>*> vector_new;
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
        for (size_t i = 0; i < num_buckets_; i++) {
            if (vector_[i] == nullptr) {
                LOG << "bucket "
                    << i
                    << " empty";
            }
            else {
                std::string log = "";

                // check if item with same key
                node<Key, Value>* curr_node = vector_[i];
                Value curr_item;
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

    std::vector<node<Key, Value>*> vector_;

    HashFunction hash_function_;

    size_t begin_local_index_;

    size_t end_local_index_;

    Value neutral_element_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_POST_TABLE_HEADER

/******************************************************************************/
