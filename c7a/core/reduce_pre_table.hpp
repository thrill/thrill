/*******************************************************************************
 * c7a/core/reduce_pre_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_PRE_TABLE_HEADER
#define C7A_CORE_REDUCE_PRE_TABLE_HEADER

#include <c7a/common/function_traits.hpp>
#include <c7a/common/functional.hpp>
#include <c7a/common/logger.hpp>
#include <c7a/data/block_writer.hpp>

#include <algorithm>
#include <cassert>
#include <cstring>
#include <string>
#include <utility>
#include <vector>

namespace c7a {
namespace core {

/**
 *
 * A data structure which takes an arbitrary value and extracts a key using
 * a key extractor function from that value. A key may also be provided initially
 * as part of a key/value pair, not requiring to extract a key.
 *
 * Afterwards, the key is hashed and the hash is used to assign that key/value pair
 * to some bucket. A bucket can have one or more slots to store items. There are
 * max_num_items_per_bucket slots in each bucket.
 *
 * In case a slot already has a key/value pair and the key of that value and the key of
 * the value to be inserted are them same, the values are reduced according to
 * some reduce function. No key/value is added to the current bucket.
 *
 * If the keys are different, the next slot (moving down) is considered. If the
 * slot is occupied, the same procedure happens again. This prociedure may be considered
 * as linear probing within the scope of a bucket.
 *
 * Finally, the key/value pair to be inserted may either:
 *
 * 1.) Be reduced with some other key/value pair, sharing the same key.
 * 2.) Inserted at a free slot in the bucket.
 * 3.) Trigger a resize of the data structure in case there are no more free slots
 *     in the bucket.
 *
 * The following illustrations shows the general structure of the data structure.
 * There are several buckets containing one or more slots. Each slot may store a item.
 * In order to optimize I/O, slots are organized in bucket blocks. Bucket blocks are
 * connected by pointers. Key/value pairs are directly stored in a bucket block, no
 * pointers are required here.
 *
 *
 *     Partition 0 Partition 1 Partition 2 Partition 3 Partition 4
 *     B00 B01 B02 B10 B11 B12 B20 B21 B22 B30 B31 B32 B40 B41 B42
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *    ||  |   |   ||  |   |   ||  |   |   ||  |   |   ||  |   |  ||
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *      |   |   |   |   |   |   |   |   |   |   |   |   |   |   |
 *      V   V   V   V   V   V   V   V   V   V   V   V   V   V   >
 *    +---+       +---+
 *    |   |       |   |
 *    +---+       +---+         ...
 *    |   |       |   |
 *    +---+       +---+
 *      |           |
 *      V           V
 *    +---+       +---+
 *    |   |       |   |
 *    +---+       +---+         ...
 *    |   |       |   |
 *    +---+       +---+
 *
 */
template <typename Key, typename HashFunction = std::hash<Key> >
class PreReduceByHashKey
{
public:
    PreReduceByHashKey(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePreTable>
    typename ReducePreTable::index_result
    operator () (Key v, ReducePreTable* ht) const {

        using index_result = typename ReducePreTable::index_result;

        size_t hashed = hash_function_(v);

        size_t local_index = hashed % ht->NumBucketsPerPartition();
        size_t partition_id = hashed % ht->NumPartitions();
        size_t global_index = partition_id *
                              ht->NumBucketsPerPartition() + local_index;
        return index_result(partition_id, local_index, global_index);
    }

private:
    HashFunction hash_function_;
};

class PreReduceByIndex
{
public:
    size_t size_;

    PreReduceByIndex(size_t size)
        : size_(size)
    { }

    template <typename ReducePreTable>
    typename ReducePreTable::index_result
    operator () (size_t key, ReducePreTable* ht) const {
        assert(key < size_);
        size_t global_index = key * ht->NumBuckets() / size_;
        size_t partition_id = key * ht->NumPartitions() / size_;
        size_t partition_offset = global_index -
                                  partition_id * ht->NumBucketsPerPartition();
        return typename ReducePreTable::index_result(partition_id,
                                                     partition_offset,
                                                     global_index);
    }
};

template <typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename IndexFunction = PreReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          size_t TargetBlockSize = 16*1024
          >
class ReducePreTable
{
    static const bool debug = false;

    typedef std::pair<Key, Value> KeyValuePair;

public:
    struct index_result
    {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the partition's sub-hashtable of this item
        size_t local_index;
        //! index within the whole hashtable
        size_t global_index;

        index_result(size_t p_id, size_t p_off, size_t g_id) {
            partition_id = p_id;
            local_index = p_off;
            global_index = g_id;
        }
    };

protected:
    //! calculate number of items such that each BucketBlock has about 1 MiB of
    //! size, or at least 8 items.
    static constexpr size_t block_size_ =
        common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));

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
    /**
     * A data structure which takes an arbitrary value and extracts a key using a key extractor
     * function from that value. Afterwards, the value is hashed based on the key into some slot.
     *
     * \param num_partitions The number of partitions.
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param num_buckets_init_scale Used to calculate the initial number of buckets
     *                  (num_partitions * num_buckets_init_scale).
     * \param num_buckets_resize_scale Used to calculate the number of buckets during resize
     *                  (size * num_buckets_resize_scale).
     * \param max_num_items_per_bucket Maximal number of items allowed in a bucket. Used to decide when to resize.
     * \param max_num_items_table Maximal number of items allowed before some items are flushed. The items
     *                  of the partition with the most items gets flushed.
     * \param index_function Function to be used for computing the bucket the item to be inserted.
     * \param equal_to_function Function for checking equality fo two keys.
     */
    ReducePreTable(size_t num_partitions,
                   KeyExtractor key_extractor,
                   ReduceFunction reduce_function,
                   std::vector<data::BlockWriter>& emit,
                   size_t num_buckets_init_scale = 10,
                   size_t num_buckets_resize_scale = 2,
                   size_t max_num_items_per_bucket = 256,
                   size_t max_num_items_table = 1048576,
                   const IndexFunction& index_function = IndexFunction(),
                   const EqualToFunction& equal_to_function = EqualToFunction())
        : num_partitions_(num_partitions),
          num_buckets_init_scale_(num_buckets_init_scale),
          num_buckets_resize_scale_(num_buckets_resize_scale),
          max_num_items_per_bucket_(max_num_items_per_bucket),
          max_num_items_table_(max_num_items_table),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(emit),
          index_function_(index_function),
          equal_to_function_(equal_to_function) {
        assert(num_partitions >= 0);
        assert(num_partitions == emit_.size());
        assert(num_buckets_init_scale > 0);
        assert(num_buckets_resize_scale > 1);
        assert(max_num_items_per_bucket > 0);
        assert(max_num_items_table > 0);

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

    /**
     * Initializes the data structure by calculating some metrics based on input.
     */
    void init() {

        sLOG << "creating ReducePreTable with" << emit_.size() << "output emitters";

        assert(emit_.size() == num_partitions_);

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
     * Inserts a value. Calls the key_extractor_, makes a key-value-pair and
     * inserts the pair into the hashtable.
     */
    void Insert(const Value& p) {
        Key key = key_extractor_(p);

        Insert(std::make_pair(key, p));
    }

    /*!
     * Inserts a value into the table, potentially reducing it in case both the key of the value
     * already in the table and the key of the value to be inserted are the same.
     *
     * An insert may trigger a partial flush of the partition with the most items if the maximal
     * number of items in the table (max_num_items_table) is reached.
     *
     * Alternatively, it may trigger a resize of table in case maximal number of items per
     * bucket is reached.
     *
     * \param p Value to be inserted into the table.
     */
    void Insert(const KeyValuePair& kv) {

        index_result h = index_function_(kv.first, this);

        assert(h.partition_id >= 0 && h.partition_id < num_partitions_);
        assert(h.local_index >= 0 && h.local_index < num_buckets_per_partition_);
        assert(h.global_index >= 0 && h.global_index < num_buckets_);

        LOG << "key: " << kv.first << " to bucket id: " << h.global_index;

        size_t num_items_bucket = 0;
        BucketBlock* current = vector_[h.global_index];

        while (current != NULL)
        {
            // iterate over valid items in a block
            for (KeyValuePair* bi = current->items;
                 bi != current->items + current->size; ++bi)
            {
                // if item and key equals, then reduce.
                if (equal_to_function_(kv.first, bi->first))
                {
                    LOG << "match of key: " << kv.first
                        << " and " << bi->first << " ... reducing...";

                    bi->second = reduce_function_(bi->second, kv.second);

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
        new (current->items + current->size++)KeyValuePair(kv.first, std::move(kv.second));

        // increase counter for partition
        // we do not check if num items per partition is above
        // a certain threshold on insert. we only use this the determine
        // the largest partition for partial flushing
        items_per_partition_[h.partition_id]++;
        // increase total counter
        num_items_++;

        // increase num items in bucket for inserted item
        num_items_bucket++;

        if (num_items_ > max_num_items_table_)
        {
            LOG << "flush";
            FlushLargestPartition();
        }

        if (num_items_bucket > max_num_items_per_bucket_)
        {
            LOG << "resize";
            ResizeUp();
        }
    }

    /*!
    * Flushes all items in the whole table.
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
     * having the most items. Retrieved items are then pushed
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
     *
     * \param partition_id The id of the partition to be flushed.
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
                    if (RobustKey) {
                        emit_[partition_id](bi->second);
						sLOG << "Pushing value";
						emit_stats_[partition_id]++;
                    }
                    else {
                        emit_[partition_id](*bi);
						sLOG << "pushing pair";
						emit_stats_[partition_id]++;
                    }
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
        num_items_ -= items_per_partition_[partition_id];
        // reset partition specific counter
        items_per_partition_[partition_id] = 0;
        // flush elements pushed into emitter
        emit_[partition_id].Flush();

        LOG << "Flushed items of partition with id: "
            << partition_id;
    }

    /*!
     * Returns the total num of buckets in the table in all partitions.
     *
     * \return Number of buckets in the table.
     */
    size_t NumBuckets() const {
        return num_buckets_;
    }

    /*!
     * Returns the total num of items in the table in all partitions.
     *
     * \return Number of items in the table.
     */
    size_t NumItems() const {
        return num_items_;
    }

    /*!
     * Returns the number of buckets any partition can hold.
     *
     * \return Number of buckets a partition can hold.
     */
    size_t NumBucketsPerPartition() const {
        return num_buckets_per_partition_;
    }

    /*!
     * Returns the number of partitions.
     *
     * \return The number of partitions.
     */
    size_t NumPartitions() const {
        return num_partitions_;
    }

    /*!
     * Returns the number of items of a partition.
     *
     * \param partition_id The id of the partition the number of
     *                  items to be returned..
     * \return The number of items in the partitions.
     */
    size_t PartitionNumItems(size_t partition_id) {
        return items_per_partition_[partition_id];
    }

    /*!
     * Sets the maximum number of items of the hash table. We don't want to push 2vt
     * elements before flush happens.
     *
     * \param size The maximal number of items the table may hold.
     */
    void SetMaxNumItems(size_t size) {
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
     * scale factor (num_items_resize_scale_). All items are rehashed as
     * part of the operation.
     */
    void ResizeUp() {
        LOG << "Resizing";
        num_buckets_ *= num_buckets_resize_scale_;
        num_buckets_per_partition_ = num_buckets_ / num_partitions_;
        // reset items_per_partition and table_size
        std::fill(items_per_partition_.begin(), items_per_partition_.end(), 0);
        num_items_ = 0;

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
                    Insert(*bi);
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
     * Removes all items from the table, but does not flush them nor does
     * it resets the table to it's initial size.
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
        num_items_ = 0;
        LOG << "Cleared";
    }

    /*!
     * Removes all items from the table, but does not flush them. However, it does
     * reset the table to it's initial size.
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
        num_items_ = 0;
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
                    log += "item: ";
                    log += std::to_string(i);
                    log += " (";
                    log += std::is_arithmetic<Key>::value || strcmp(typeid(Key).name(), "string")
                           ? std::to_string(bi->first) : "_";
                    log += ", ";
                    log += std::is_arithmetic<Value>::value || strcmp(typeid(Value).name(), "string")
                           ? std::to_string(bi->second) : "_";
                    log += ")\n";
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

protected:
    //! Number of partitions
    size_t num_partitions_;

    //! Scale factor to compute the initial bucket size.
    size_t num_buckets_init_scale_;

    //! Scale factor to compute the number of buckets
    //! during resize relative to current size.
    size_t num_buckets_resize_scale_;

    // Maximal number of items per bucket before resize.
    size_t max_num_items_per_bucket_;

    //! Number of buckets
    //! Product of number of partitions and init scale.
    size_t num_buckets_;

    // Number of buckets per partition.
    size_t num_buckets_per_partition_;

    //! Number of items per partition.
    std::vector<size_t> items_per_partition_;

    //! Keeps the total number of items in the table.
    size_t num_items_ = 0;

    //! Maximal number of items before some items
    //! are flushed (-> partial flush).
    size_t max_num_items_table_;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Set of emitters, one per partition.
    std::vector<data::BlockWriter>& emit_;

    //! Emitter stats.
    std::vector<int> emit_stats_;

    //! Data structure for actually storing the items.
    std::vector<BucketBlock*> vector_;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_PRE_TABLE_HEADER

/******************************************************************************/
