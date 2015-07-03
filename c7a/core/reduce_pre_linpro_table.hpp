/*******************************************************************************
 * c7a/core/reduce_pre_linpro_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_PRE_LINPRO_TABLE_HEADER
#define C7A_CORE_REDUCE_PRE_LINPRO_TABLE_HEADER

#include <c7a/api/function_traits.hpp>
#include <c7a/data/manager.hpp>

#include <map>
#include <iostream>
#include <c7a/common/logger.hpp>
#include <string>
#include <vector>
#include <stdexcept>
#include <array>
#include <deque>
#include <utility>

namespace c7a {
namespace core {
template <typename KeyExtractor, typename ReduceFunction, typename EmitterFunction>
class ReducePreLinProTable
{
    static const bool debug = false;

    using Key = typename FunctionTraits<KeyExtractor>::result_type;

    using Value = typename FunctionTraits<ReduceFunction>::result_type;

    typedef std::pair<Key, Value> KeyValuePair;

protected:
    struct hash_result
    {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the partition's sub-hashtable of this item
        size_t partition_offset;
        //! index within the whole hashtable
        size_t global_index;

        hash_result(Key v, const ReducePreLinProTable& ht) {
            size_t hashed = std::hash<Key>() (v);

            // partition idx
            partition_offset = hashed % ht.num_items_per_partition_;

            // partition id
            partition_id = hashed % ht.num_partitions_;

            // global idx
            global_index = partition_id * ht.num_items_per_partition_ + partition_offset;
        }
    };

public:
    ReducePreLinProTable(size_t num_partitions,
                         size_t num_items_init_scale,
                         size_t num_items_resize_scale,
                         size_t stepsize,
                         size_t max_stepsize,
                         double max_partition_fill_ratio,
                         size_t max_num_items_table,
                         KeyExtractor key_extractor, ReduceFunction reduce_function,
                         std::vector<EmitterFunction>& emit)
        : num_partitions_(num_partitions),
          num_items_init_scale_(num_items_init_scale),
          num_items_resize_scale_(num_items_resize_scale),
          stepsize_(stepsize),
          max_stepsize_(max_stepsize),
          max_partition_fill_ratio_(max_partition_fill_ratio),
          max_num_items_table_(max_num_items_table),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)) {
        init();
    }

    ReducePreLinProTable(size_t num_partitions, KeyExtractor key_extractor,
                         ReduceFunction reduce_function, std::vector<EmitterFunction>& emit)
        : num_partitions_(num_partitions),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)) {
        init();
    }

    //! non-copyable: delete copy-constructor
    ReducePreLinProTable(const ReducePreLinProTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreLinProTable& operator = (const ReducePreLinProTable&) = delete;

    ~ReducePreLinProTable() { }

    void init() {
        sLOG << "creating reducePreLinProTable with" << emit_.size() << "output emiters";
        for (size_t i = 0; i < emit_.size(); i++)
            emit_stats_.push_back(0);

        num_items_ = num_partitions_ * num_items_init_scale_;
        if (num_partitions_ > num_items_ &&
            num_items_ % num_partitions_ != 0) {
            throw std::invalid_argument("partition_size must be less than or equal to num_items "
                                        "AND partition_size a divider of num_items");
        }
        num_items_per_partition_ = num_items_ / num_partitions_;

        vector_.resize(num_items_, NULL);
        items_per_partition_.resize(num_partitions_, 0);
    }

    /*!
     * Inserts a key/value pair.
     *
     * Optionally, this may be reduce using the reduce function
     * in case the key already exists.
     */
    void Insert(Value&& p) {
        Key key = key_extractor_(p);
        LOG << "key " << key;

        hash_result h = hash_result(key, *this);

        LOG << num_items_;

        int pos = h.global_index;
        size_t count = 0;
        KeyValuePair* current = vector_[pos];

        while (current != NULL)
        {
            if (current->first == key)
            {
                LOG << "match of key: " << key
                    << " and " << current->first << " ... reducing...";

                current->second = reduce_function_(current->second, p);

                LOG << "...finished reduce!";
                return;
            }

            ++count;
            if (count >= max_stepsize_ || count >= num_items_per_partition_)
            {
                ResizeUp();
                Insert(std::move(p));
                return;
            }

            if (h.partition_offset + count >= num_items_per_partition_)
            {
                pos -= (h.partition_offset + count);
            }

            current = vector_[pos + count];
        }

        // insert new pair
        if (current == NULL)
        {
            vector_[pos + count] = new KeyValuePair(key, std::move(p));

            // increase total counter
            table_size_++;

            // increase counter for partition
            items_per_partition_[h.partition_id]++;
        }

        if (table_size_ > max_num_items_table_)
        {
            LOG << "flush";
            FlushLargestPartition();
        }

        if (items_per_partition_[h.partition_id]
            / num_items_per_partition_ >= max_partition_fill_ratio_)
        {
            LOG << "resize";
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
            KeyValuePair* current = vector_[i];
            if (current != NULL)
            {
                emit_[partition_id](std::move(current->second));
                operator delete (vector_[i]);
                vector_[i] = NULL;
            }
        }

        // reset total counter
        table_size_ -= items_per_partition_[partition_id];
        // reset partition specific counter
        items_per_partition_[partition_id] = 0;
        // flush elements pushed into emitter
        emit_[partition_id].Flush();

        LOG << "Flushed items of partition with id: "
            << partition_id;
    }

    /*!
     * Returns the total num of items.
     */
    size_t Size() {
        return table_size_;
    }

    /*!
     * Returns the total num of items.
     */
    size_t NumItems() {
        return num_items_;
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
        LOG << "Resizing";
        num_items_ *= num_items_resize_scale_;
        num_items_per_partition_ = num_items_ / num_partitions_;
        // reset items_per_partition and table_size
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;

        // move old hash array
        std::vector<KeyValuePair*> vector_old;
        std::swap(vector_old, vector_);

        // init new hash array
        vector_.resize(num_items_, NULL);

        // rehash all items in old array
        for (KeyValuePair* k_v_pair : vector_old)
        {
            KeyValuePair* current = k_v_pair;
            if (current != NULL)
            {
                Insert(std::move(current->second));
            }
        }
        LOG << "Resized";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Clear() {
        LOG << "Clearing";

        for (KeyValuePair* k_v_pair : vector_)
        {
            k_v_pair = NULL; // TODO(ms): fix, doesnt work
        }

        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;
        LOG << "Cleared";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Reset() {
        LOG << "Resetting";
        num_items_ = num_partitions_ * num_items_init_scale_;
        num_items_per_partition_ = num_items_ / num_partitions_;

        for (KeyValuePair* k_v_pair : vector_)
        {
            k_v_pair = NULL; // TODO(ms): fix, doesnt work
        }

        vector_.resize(num_items_, NULL);
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;
        LOG << "Resetted";
    }

    /*!
     * Prints content of hash table.
     */
    void Print() {

        std::string log = "Printing\n";

        for (int i = 0; i < num_items_; i++)
        {
            if (vector_[i] == NULL)
            {
                log += "item: ";
                log += std::to_string(i);
                log += " empty\n";
                continue;
            }

            log += "item: ";
            log += std::to_string(i);
            log += " (";
            log += vector_[i]->first;
            log += ", ";
            //log += vector_[i]->second; // TODO(ms): How to convert Value to a string?
            log += ")\n";
        }

        std::cout << log << std::endl;

        return;
    }

private:
    size_t num_partitions_;                   // partition size

    size_t num_items_init_scale_ = 10;        // set number of items per partition based on num_partitions
    // multiplied with some scaling factor, must be equal to or greater than 1

    size_t num_items_resize_scale_ = 2;       // resize scale on max_num_items_per_bucket_

    size_t stepsize_ = 1;                     // stepsize in case of collision

    size_t max_stepsize_ = 10;                // max stepsize before resize

    double max_partition_fill_ratio_ = 0.9;   // max partition fill ratio before resize

    size_t max_num_items_table_ = 1048576;    // max num of items before spilling of largest partition

    size_t num_items_;                        // num items

    size_t num_items_per_partition_;          // num items per partition

    std::vector<size_t> items_per_partition_; // num items per partition

    size_t table_size_ = 0;                   // total number of items

    KeyExtractor key_extractor_;

    ReduceFunction reduce_function_;

    std::vector<EmitterFunction> emit_;
    std::vector<int> emit_stats_;

    // TODO(ms): remove *
    std::vector<KeyValuePair*> vector_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_PRE_LINPRO_TABLE_HEADER

/******************************************************************************/
