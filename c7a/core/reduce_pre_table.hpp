/*******************************************************************************
 * c7a/core/reduce_pre_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_PRE_TABLE_HEADER
#define C7A_CORE_REDUCE_PRE_TABLE_HEADER

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
template <typename KeyExtractor, typename ReduceFunction, typename EmitterFunction,
          size_t TargetBlockSize = 1024* 1024>
class ReducePreTable
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

        hash_result(Key v, const ReducePreTable& ht) {
            size_t hashed = std::hash<Key>() (v);

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

    //! template for constexpr max, because std::max is not good enough.
    template <typename T>
    constexpr
    static const T & max(const T& a, const T& b) {
        return a > b ? a : b;
    }

    //! calculate number of items such that each BucketBlock has about 1 MiB of
    //! size, or at least 8 items.
    static constexpr size_t block_size_ =
        max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));

    //! Block holding reduce key/value pairs.
    struct BucketBlock {
        //! number of _used_/constructed items in this block. next is unused if
        //! size != block_size.
        size_t       size;

        //! link of linked list to next block
        BucketBlock  * next;

        //! memory area of items
        KeyValuePair items[block_size_];

        //! helper to destroy all allocated items
        void         destroy_items() {
            for (KeyValuePair* i = items; i != items + size; ++i)
                i->~KeyValuePair();
        }
    };

public:
    ReducePreTable(size_t num_partitions, size_t num_buckets_init_scale,
                   size_t num_buckets_resize_scale,
                   size_t max_num_items_per_bucket, size_t max_num_items_table,
                   KeyExtractor key_extractor, ReduceFunction reduce_function,
                   std::vector<EmitterFunction>& emit)
        : num_partitions_(num_partitions),
          num_buckets_init_scale_(num_buckets_init_scale),
          num_buckets_resize_scale_(num_buckets_resize_scale),
          max_num_items_per_bucket_(max_num_items_per_bucket),
          max_num_items_table_(max_num_items_table),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)) {
        init();
    }

    ReducePreTable(size_t partition_size, KeyExtractor key_extractor,
                   ReduceFunction reduce_function, std::vector<EmitterFunction>& emit)
        : num_partitions_(partition_size),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)) {
        init();
    }

    //! non-copyable: delete copy-constructor
    ReducePreTable(const ReducePreTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreTable& operator = (const ReducePreTable&) = delete;

    ~ReducePreTable() {
        // destroy all block chains
        for (BucketBlock* b_block : vector_)
        {
            BucketBlock* current = b_block;
            while (current != NULL)
            {
                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }
        }
    }

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
    void Insert(Value&& p) {
        Key key = key_extractor_(p);

        hash_result h(key, *this);

        LOG << "key: " << key << " to bucket id: " << h.global_index;

        size_t num_items_bucket = 0;
        BucketBlock* current = vector_[h.global_index];

        while (current != NULL)
        {
            // iterate over valid items in a block
            for (KeyValuePair* bi = current->items;
                 bi != current->items + current->size; ++bi)
            {
                // if item and key equals, then reduce.
                if (key == bi->first)
                {
                    LOG << "match of key: " << key
                        << " and " << bi->first << " ... reducing...";

                    bi->second = reduce_function_(bi->second, p);

                    LOG << "...finished reduce!";
                    return;
                }

                // increase num items in bucket for visited item
                num_items_bucket++;
            }

            if (current->next == NULL)
                break;

            current = current->next;
        }

        // have an item that needs to be added.

        if (current == NULL ||
            current->size == block_size_)
        {
            // allocate a new block of uninitialized items, prepend to bucket
            current =
                static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

            current->size = 0;
            current->next = vector_[h.global_index];
            vector_[h.global_index] = current;
        }

        // in-place construct/insert new item in current bucket block
        new (current->items + current->size++)KeyValuePair(key, std::move(p));

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
        static const bool debug = false;

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
             i < partition_id * num_buckets_per_partition_ + num_buckets_per_partition_; i++)
        {
            BucketBlock* current = vector_[i];

            while (current != NULL)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    emit_[partition_id](std::move(bi->second));
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }

            vector_[i] = NULL;
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
     * Resizes the table by increasing the number of buckets using some
     * resize scale factor. All items are rehashed as part of the operation.
     */
    void ResizeUp() {
        LOG << "Resizing";
        LOG << num_buckets_;
        num_buckets_ *= num_buckets_resize_scale_;
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;
        // reset items_per_partition and table_size
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        table_size_ = 0;

        // move old hash array
        std::vector<BucketBlock*> vector_old;
        std::swap(vector_old, vector_);

        // init new hash array
        vector_.resize(num_buckets_, NULL);

        // rehash all items in old array
        for (BucketBlock* b_block : vector_old)
        {
            BucketBlock* current = b_block;
            while (current != NULL)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    Insert(std::move(bi->second));
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }
        }
        LOG << "Resized";
    }

    /*!
     * Removes all items in the table, but NOT flushing them.
     */
    void Clear() {
        LOG << "Clearing";

        for (BucketBlock* b_block : vector_)
        {
            BucketBlock* current = b_block;
            while (current != NULL)
            {
                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }
            b_block = NULL;
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
        num_buckets_ = num_partitions_ * num_buckets_init_scale_;
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;

        for (BucketBlock*& b_block : vector_)
        {
            BucketBlock* current = b_block;
            while (current != NULL)
            {
                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }
            b_block = NULL;
        }

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
                continue;
            }

            std::string log = "";

            BucketBlock* current = vector_[i];
            while (current != NULL)
            {
                log += "block: ";

                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    log += "(";
                    log += bi->first;
                    log += ", ";
                    //log += bucket_item.second; // TODO(ms): How to convert Value to a string?
                    log += ") ";
                }
                current = current->next;
            }

            LOG << "bucket id: "
                << i
                << " "
                << log;
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

    size_t max_num_items_per_bucket_ = 256;   // max num of items per bucket before resize

    std::vector<size_t> items_per_partition_; // num items per partition

    size_t table_size_ = 0;                   // total number of items

    size_t max_num_items_table_ = 1048576;    // max num of items before spilling of largest partition

    KeyExtractor key_extractor_;

    ReduceFunction reduce_function_;

    std::vector<EmitterFunction> emit_;
    std::vector<int> emit_stats_;

    std::vector<BucketBlock*> vector_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_PRE_TABLE_HEADER

/******************************************************************************/
