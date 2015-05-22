/*******************************************************************************
 * c7a/core/reduce_pre_table_bench.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_PRE_TABLE_BENCH_HEADER
#define C7A_CORE_REDUCE_PRE_TABLE_BENCH_HEADER

#include <c7a/common/logger.hpp>
#include <string>
#include <vector>
#include <unordered_map>
#include <functional>
#include <iostream>
#include <cassert>
#include "c7a/api/function_traits.hpp"
#include "c7a/data/data_manager.hpp"

namespace c7a {
namespace core {

// Use this hashtable to benchmark our own hash table against

template <typename KeyExtractor, typename ReduceFunction, typename EmitterFunction>
class ReducePreTableBench
{
    static const bool debug = false;
    using key_t = typename FunctionTraits<KeyExtractor>::result_type;
    using value_t = typename FunctionTraits<ReduceFunction>::result_type;

protected:
public:
    size_t Size() {
        return table_size_;
    }

    void SetMaxSize(size_t size) {
        max_num_items_table_ = size;
    }

    ReducePreTableBench(size_t num_workers,
                        KeyExtractor key_extractor,
                        ReduceFunction reduce_function,
                        std::vector<EmitterFunction> emit)
        : num_workers_(num_workers),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit),
          the_hash_table_(num_workers_, std::unordered_map<key_t, value_t>()),
          key_count_(num_workers, 0) {
        init();
    }

    //set max_num_items_table in constructor just to make testing easier
    ReducePreTableBench(size_t num_workers,
                        KeyExtractor key_extractor,
                        ReduceFunction reduce_function,
                        std::vector<EmitterFunction> emit,
                        size_t max_num_items_table)
        : num_workers_(num_workers),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit),
          the_hash_table_(num_workers_, std::unordered_map<key_t, value_t>()),
          key_count_(num_workers, 0),
          max_num_items_table_(max_num_items_table) {
        init();
    }

    ~ReducePreTableBench() { }

    void init()
    { }

    /*!
     * Inserts a key/value pair.
     *
     * Item will be reduced immediately using the reduce function
     * in case the key already exists.
     */
    void Insert(const value_t& item) {
        key_t key = key_extractor_(item);
        size_t hash_result = hash_fn_(key);
        size_t hash_worker = hash_result % num_workers_;
        auto elem = the_hash_table_[hash_worker].find(key);

        if (elem != the_hash_table_[hash_worker].end()) {
            LOG << "[Insert] REDUCED ITEM";
            auto new_elem = reduce_function_(item, elem->second);
            the_hash_table_[hash_worker].at(key) = new_elem;
        }
        else {
            LOG << "[Insert] INSERTED ITEM";
            the_hash_table_[hash_worker].insert(std::make_pair(key, item));
            ++key_count_[hash_worker];
            ++table_size_;
        }

        if (debug) Print();

        if (table_size_ > max_num_items_table_) {
            FlushLargestPartition();
        }
    }

    /*!
     * Retrieves all items belonging to the partition
     * having the most items. Retrieved items are then forward
     * to the provided emitter.
     */
    void FlushLargestPartition() {
        //find the worker with the most assigned keys
        int max_count = 0;
        int max_index = -1;
        int index = 0;
        for (int counter : key_count_) {
            if (counter > max_count) {
                max_count = counter;
                max_index = index;
            }
            ++index;
        }
        assert(max_index >= 0);

        //emit all keys in table and send it to worker
        auto curr_ht = the_hash_table_[max_index];
        auto start = curr_ht.begin();
        auto end = curr_ht.end();
        for (auto it = start; it != end; ++it) {
            LOG << "[FlushLargestPartition] FLUSHED";
            emit_[max_index](it->second);
        }
        the_hash_table_[max_index].clear();

        //reset table size and key count of worker
        table_size_ -= key_count_[max_index];
        key_count_[max_index] = 0;

        if (debug) Print();
    }

    /*!
     * Flushes all items.
     */
    void Flush() {
        for (size_t worker = 0; worker < num_workers_; ++worker) {
            //emit all keys in table and send it to worker
            for (auto elem = the_hash_table_[worker].begin(); elem != the_hash_table_[worker].end(); ++elem) {
                emit_[worker](elem->second);
            }
            the_hash_table_[worker].clear();
            //reset table size and key count of worker
            table_size_ -= key_count_[worker];
            key_count_[worker] = 0;
        }
        if (debug) Print();
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Clear() {
        for (size_t worker = 0; worker < num_workers_; ++worker) {
            the_hash_table_[worker].clear();
            table_size_ -= key_count_[worker];
            key_count_[worker] = 0;
        }
        if (debug) Print();
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Reset() {
        for (size_t worker = 0; worker < num_workers_; ++worker) {
            the_hash_table_[worker].clear();
            table_size_ -= key_count_[worker];
            key_count_[worker] = 0;
        }
        if (debug) Print();
    }

    // prints content of hash table
    void Print() {
        LOG1 << "[THE HORROR] Current hash table";
        for (auto worker : the_hash_table_) {
            LOG1 << "WORKER";
            for (auto it = worker.begin(); it != worker.end(); ++it) {
                LOG1 << "elem";
            }
        }
        LOG1 << "TABLE SIZE " << table_size_;
        LOG1 << "\n";
    }

private:
    size_t num_workers_;
    KeyExtractor key_extractor_;
    ReduceFunction reduce_function_;
    std::vector<EmitterFunction> emit_;
    std::hash<key_t> hash_fn_;
    std::vector<std::unordered_map<key_t, value_t> > the_hash_table_;
    std::vector<int> key_count_;
    size_t table_size_ = 0;
    size_t max_num_items_table_ = 1048576;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_PRE_TABLE_BENCH_HEADER

/******************************************************************************/
