/*******************************************************************************
 * c7a/core/reduce_pre_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_PRE_TABLE_HEADER
#define C7A_CORE_REDUCE_PRE_TABLE_HEADER

#include <c7a/api/function_traits.hpp>
#include <c7a/data/data_manager.hpp>

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
class ReducePreTable
{
    static const bool debug = true;

    using key_t = typename FunctionTraits<KeyExtractor>::result_type;

    using value_t = typename FunctionTraits<ReduceFunction>::result_type;

protected:
    struct hash_result
    {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the partition's sub-hashtable of this item
        size_t partition_offset;
        //! index within the whole hashtable
        size_t global_index;

        hash_result(key_t v, const ReducePreTable& ht) {
            size_t hashed = std::hash<key_t>() (v);

            // partition idx
            //LOG << ht.num_buckets_per_partition_ << " " << ht.num_partitions_;
            partition_offset = hashed % ht.num_buckets_per_partition_;

            // partition id
            partition_id = hashed % ht.num_partitions_;
            //LOG << partition_offset << " " << partition_id;

            // global idx
            global_index = partition_id * ht.num_buckets_per_partition_ + partition_offset;
        }
    };

    struct bucket_block {
        // TODO(ms): use a new/delete here instead of a vector, it is faster.
        std::vector<std::pair<key_t, value_t> > items;

        bucket_block                            * next = NULL;

        bucket_block(const ReducePreTable& ht) {
            items.reserve(ht.bucket_block_size_);
        }
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
          emit_(emit) {
        init();
    }

    ReducePreTable(size_t partition_size, KeyExtractor key_extractor,
                   ReduceFunction reduce_function, std::vector<EmitterFunction> emit)
        : num_partitions_(partition_size),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit) {
        init();
    }

    ~ReducePreTable() { }

    void init() {
        sLOG << "creating reducePreTable with" << emit_.size() << "output emiters";
        for (size_t i = 0; i < emit_.size(); i++)
            emit_stats_.push_back(0);

        num_buckets_ = num_partitions_ * num_buckets_init_scale_;
        if (num_partitions_ > num_buckets_ &&
            num_buckets_ % num_partitions_ != 0) {
            throw std::invalid_argument("partition_size must be less than or equal to num_buckets "
                                        "AND partition_size a divider of num_buckets");
        }
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;

        vector_.resize(num_buckets_, NULL);
        items_per_partition_.resize(num_partitions_, 0);
    }

    /*!
     * Inserts a key/value pair.
     *
     * Optionally, this may be reduce using the reduce function
     * in case the key already exists.
     */
    void Insert(const value_t& p) {
        key_t key = key_extractor_(p);

        hash_result h(key, *this);

        LOG << "key: " << key << " to bucket id: " << h.global_index;

        size_t num_items_bucket = 0;
        bucket_block* current_bucket_block = vector_[h.global_index];
        while (current_bucket_block != NULL)
        {
            for (auto& bucket_item : current_bucket_block->items)
            {
                // if item and key equals
                // then reduce
                if (key == bucket_item.first)
                {
                    LOG << "match of key: " << key << " and " << bucket_item.first << " ... reducing...";
                    bucket_item.second = reduce_function_(bucket_item.second, p);
                    LOG << "...finished reduce!";
                    return;
                }

                // increase num items in bucket for visited item
                num_items_bucket++;
            }

            if (current_bucket_block->next == NULL)
            {
                break;
            }

            current_bucket_block = current_bucket_block->next;
        }

        if (current_bucket_block == NULL)
        {
            current_bucket_block = vector_[h.global_index] = new bucket_block(*this);
        }
        else if (current_bucket_block->items.size() == bucket_block_size_)
        {
            current_bucket_block = current_bucket_block->next = new bucket_block(*this);
        }

        // insert new item in current bucket block
        current_bucket_block->items.push_back(std::pair<key_t, value_t>(key, p));
        // increase counter for partition
        items_per_partition_[h.partition_id]++;
        // increase total counter
        table_size_++;

        // increase num items in bucket for inserted item
        num_items_bucket++;

        if (table_size_ > max_num_items_table_)
        {
            LOG << "spilling in progress";
            FlushLargestPartition();
        }

        if (num_items_bucket > max_num_items_per_bucket_)
        {
            LOG << "test test" << num_items_bucket << " " << max_num_items_per_bucket_;
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
            << p_idx * num_buckets_per_partition_
            << " CurrentIdx*p_size+p_size-1 "
            << p_idx * num_buckets_per_partition_ + num_buckets_per_partition_ - 1;

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

        for (size_t i = partition_id * num_buckets_per_partition_;
             i <= partition_id * num_buckets_per_partition_ + num_buckets_per_partition_ - 1; i++)
        {
            bucket_block* current_bucket_block = vector_[i];
            while (current_bucket_block != NULL)
            {
                for (std::pair<key_t, value_t>& bucket_item : current_bucket_block->items)
                {
                    emit_[partition_id](bucket_item.second);
                    emit_stats_[partition_id]++;
                }
                //TODO(ms) call emit_[partition_id].Flush here to ensure elements are acutally pushed via network
                //I could not make the change because there are some instances of this class with std::functions
                //and they don't offer the Flush() mehtod of course.
                bucket_block* tmp_current_bucket_block = current_bucket_block->next;
                delete current_bucket_block;
                current_bucket_block = tmp_current_bucket_block;
            }

            vector_[i] = NULL;
        }

        // reset total counter
        table_size_ -= items_per_partition_[partition_id];
        // reset partition specific counter
        items_per_partition_[partition_id] = 0;

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
    size_t NumBuckets() {
        return num_buckets_;
    }

    /*!
     * Returns the size of a partition referenzed by partition_id.
     */
    size_t PartitionSize(size_t partition_id) {
        return items_per_partition_[partition_id];
    }

    /*!
     * Sets the maximum size of the hash table. We don't want to push 2vt elements before flush happens.
     */
    void SetMaxSize(size_t size) {
        max_num_items_table_ = size;
    }

    /*!
     * Closes all emitter
     */
    void CloseEmitter() {
        sLOG << "emit stats:";
        unsigned int i = 0;
        for (auto& e : emit_) {
            e.Close();
            sLOG << "emiter" << i << "pushed" << emit_stats_[i++];
        }
    }

    /*!
     * Resizes the table by increasing the number of buckets using some
     * resize scale factor. All items are rehashed as part of the operation.
     */
    void ResizeUp() {
        LOG << "Resizing";
        LOG << num_buckets_;
        num_buckets_ *= num_buckets_resize_scale_;
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;
        // init new array
        std::vector<bucket_block*> vector_old = vector_;
        std::vector<bucket_block*> vector_new; // TODO(ms): 3 vectors? come on! -> make it happen with one vector only!
        std::cout << num_buckets_ << std::endl;
        vector_new.resize(num_buckets_, NULL);
        vector_ = vector_new;
        // rehash all items in old array
        for (bucket_block* b_block : vector_old)
        {
            bucket_block* current_bucket_block = b_block;
            while (current_bucket_block != NULL)
            {
                for (std::pair<key_t, value_t>& bucket_item : current_bucket_block->items)
                {
                    Insert(bucket_item.second);
                }
                bucket_block* tmp_current_bucket_block = current_bucket_block->next;
                delete current_bucket_block;
                current_bucket_block = tmp_current_bucket_block;
            }
        }
        LOG << "Resized";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Clear() {
        LOG << "Clearing";
        std::fill(vector_.begin(), vector_.end(), NULL);
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;
        LOG << "Cleared";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Reset() {
        LOG << "Resetting";
        num_buckets_ = num_partitions_ * num_buckets_init_scale_;
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;
        vector_.resize(num_buckets_, NULL);
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;
        LOG << "Resetted";
    }

    /*!
     * Prints content of hash table.
     */
    void Print() {
        LOG << "Printing";

        for (int i = 0; i < num_buckets_; i++)
        {
            if (vector_[i] == NULL)
            {
                LOG << "bucket id: "
                    << i
                    << " empty";
            }
            else
            {
                std::string log = "";

                bucket_block* current_bucket_block = vector_[i];
                while (current_bucket_block != NULL)
                {
                    log += "block: ";

                    for (std::pair<key_t, value_t> bucket_item : current_bucket_block->items)
                    {
                        if (&bucket_item != NULL)
                        {
                            log += "(";
                            log += bucket_item.first;
                            log += ", ";
                            //log += bucket_item.second; // TODO(ms): How to convert value_t to a string?
                            log += ") ";
                        }
                    }
                    current_bucket_block = current_bucket_block->next;
                }

                LOG << "bucket id: "
                    << i
                    << " "
                    << log;
            }
        }

        return;
    }

private:
    size_t num_partitions_;                   // partition size

    size_t num_buckets_;                      // num buckets

    size_t num_buckets_per_partition_;        // num buckets per partition

    size_t num_buckets_init_scale_ = 10;      // set number of buckets per partition based on num_partitions
    // multiplied with some scaling factor, must be equal to or greater than 1

    size_t num_buckets_resize_scale_ = 2;     // resize scale on max_num_items_per_bucket_

    size_t bucket_block_size_ = 32;           // size of bucket blocks

    size_t max_num_items_per_bucket_ = 256;   // max num of items per bucket before resize

    std::vector<size_t> items_per_partition_; // num items per partition

    size_t table_size_ = 0;                   // total number of items

    size_t max_num_items_table_ = 1048576;    // max num of items before spilling of largest partition

    KeyExtractor key_extractor_;

    ReduceFunction reduce_function_;

    std::vector<EmitterFunction> emit_;
    std::vector<int> emit_stats_;

    std::vector<bucket_block*> vector_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_PRE_TABLE_HEADER

/******************************************************************************/
