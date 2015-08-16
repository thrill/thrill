/*******************************************************************************
 * c7a/core/reduce_post_table.hpp
 *
 * Hash table with support for reduce.
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_REDUCE_POST_TABLE_HEADER
#define C7A_CORE_REDUCE_POST_TABLE_HEADER

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
class PostReduceByHashKey
{
public:
    PostReduceByHashKey(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePostTable>
    typename ReducePostTable::index_result
    operator () (Key v, ReducePostTable* ht) const {

        using index_result = typename ReducePostTable::index_result;

        size_t hashed = hash_function_(v);

        size_t global_index = hashed % ht->NumBuckets();
        return index_result(global_index);
    }

private:
    HashFunction hash_function_;
};

class PostReduceByIndex
{
public:
    PostReduceByIndex() { }

    template <typename ReducePostTable>
    typename ReducePostTable::index_result
    operator () (size_t key, ReducePostTable* ht) const {

        using index_result = typename ReducePostTable::index_result;

        size_t global_index = (key - ht->BeginLocalIndex()) % ht->NumBuckets();
        return index_result(global_index);
    }
};

class PostReduceFlushToDefault
{
public:
    template <typename ReducePostTable>
    void
    operator () (ReducePostTable* ht) const {

        using BucketBlock = typename ReducePostTable::BucketBlock;

        using KeyValuePair = typename ReducePostTable::KeyValuePair;

        auto& buckets_ = ht->Items();

        for (size_t i = 0; i < ht->NumBuckets(); i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != NULL)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    ht->EmitAll(std::make_pair(bi->first, bi->second));
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }

            buckets_[i] = NULL;
        }

        ht->SetNumItems(0);
    }
};

template <typename Value>
class PostReduceFlushToIndex
{
public:
    template <typename ReducePostTable>
    void
    operator () (ReducePostTable* ht) const {

        using BucketBlock = typename ReducePostTable::BucketBlock;

        using KeyValuePair = typename ReducePostTable::KeyValuePair;

        auto& buckets_ = ht->Items();

        std::vector<Value> elements_to_emit
            (ht->EndLocalIndex() - ht->BeginLocalIndex(), ht->NeutralElement());

        for (size_t i = 0; i < ht->NumBuckets(); i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != NULL)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    elements_to_emit[bi->first - ht->BeginLocalIndex()] =
                        bi->second;
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                current->destroy_items();
                operator delete (current);
                current = next;
            }

            buckets_[i] = NULL;
        }

        size_t index = ht->BeginLocalIndex();
        for (auto element_to_emit : elements_to_emit) {
            ht->EmitAll(std::make_pair(index++, element_to_emit));
        }
        assert(index == ht->EndLocalIndex());

        ht->SetNumItems(0);
    }
};

template <bool, typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl;

template <typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl<true, EmitterType, ValueType, SendType>{
    void EmitElement(ValueType ele, std::vector<EmitterType> emitters) {
        for (auto& emitter : emitters) {
            emitter(ele);
        }
    }
};

template <typename EmitterType, typename ValueType, typename SendType>
struct EmitImpl<false, EmitterType, ValueType, SendType>{
    void EmitElement(ValueType ele, std::vector<EmitterType> emitters) {
        for (auto& emitter : emitters) {
            emitter(ele.second);
        }
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool SendPair = false,
          typename FlushFunction = PostReduceFlushToDefault,
          typename IndexFunction = PostReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          size_t TargetBlockSize = 16*1024
          >
class ReducePostTable
{
    static const bool debug = false;

public:
    typedef std::pair<Key, Value> KeyValuePair;

    typedef std::function<void (const ValueType&)> EmitterFunction;

    EmitImpl<SendPair, EmitterFunction, KeyValuePair, ValueType> emit_impl_;

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

    struct index_result
    {
    public:
        //! index within the whole hashtable
        size_t global_index;

        index_result(size_t g_id) {
            global_index = g_id;
        }
    };

    /**
     * A data structure which takes an arbitrary value and extracts a key using a key extractor
     * function from that value. Afterwards, the value is hashed based on the key into some slot.
     *
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
    ReducePostTable(KeyExtractor key_extractor,
                    ReduceFunction reduce_function,
                    std::vector<EmitterFunction>& emit,
                    const IndexFunction& index_function = IndexFunction(),
                    const FlushFunction& flush_function = FlushFunction(),
                    size_t begin_local_index = 0,
                    size_t end_local_index = 0,
                    Value neutral_element = Value(),
                    size_t num_buckets_init_scale = 10,
                    size_t num_buckets_resize_scale = 2,
                    size_t max_num_items_per_bucket = 256,
                    size_t max_num_items_table = 1048576,
                    const EqualToFunction& equal_to_function = EqualToFunction()
                    )
        :   num_buckets_init_scale_(num_buckets_init_scale),
          num_buckets_resize_scale_(num_buckets_resize_scale),
          max_num_items_per_bucket_(max_num_items_per_bucket),
          max_num_items_table_(max_num_items_table),
          key_extractor_(key_extractor),
          reduce_function_(reduce_function),
          emit_(std::move(emit)),
          index_function_(index_function),
          equal_to_function_(equal_to_function),
          flush_function_(flush_function),
          begin_local_index_(begin_local_index),
          end_local_index_(end_local_index),
          neutral_element_(neutral_element) {
        assert(num_buckets_init_scale > 0);
        assert(num_buckets_resize_scale > 1);
        assert(max_num_items_per_bucket > 0);
        assert(max_num_items_table > 0);

        init();
    }

    //! non-copyable: delete copy-constructor
    ReducePostTable(const ReducePostTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePostTable& operator = (const ReducePostTable&) = delete;

    ~ReducePostTable() {
        // destroy all block chains
        for (BucketBlock* b_block : buckets_)
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

        sLOG << "creating ReducePostTable with" << emit_.size() << "output emitters";

        num_buckets_ = num_buckets_init_scale_;
        buckets_.resize(num_buckets_, NULL);
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

        assert(h.global_index >= 0 && h.global_index < num_buckets_);

        LOG << "key: " << kv.first << " to bucket id: " << h.global_index;

        size_t num_items_bucket = 0;
        BucketBlock* current = buckets_[h.global_index];

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
            // allocate a new block of uninitialized items, postpend to bucket
            current =
                static_cast<BucketBlock*>(operator new (sizeof(BucketBlock)));

            current->size = 0;
            current->next = buckets_[h.global_index];
            buckets_[h.global_index] = current;
        }

        // in-place construct/insert new item in current bucket block
        new (current->items + current->size++)KeyValuePair(kv.first, std::move(kv.second));

        // increase total counter
        num_items_++;

        // increase num items in bucket for inserted item
        num_items_bucket++;

        if (num_items_ > max_num_items_table_)
        {
            LOG << "flush";
            throw std::invalid_argument("Hashtable overflown. No external memory functionality implemented yet");
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
        LOG << "Flushing items";

        flush_function_(this);

        // reset total counter
        num_items_ = 0;

        LOG << "Flushed items";
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const KeyValuePair& element) {
        // void EmitAll(const Key& key, const Value& value) {
        emit_impl_.EmitElement(element, emit_);
    }

    /*!
     * Returns the total num of buckets in the table.
     *
     * @return Number of buckets in the table.
     */
    size_t NumBuckets() const {
        return num_buckets_;
    }

    /*!
     * Returns the total num of items in the table.
     *
     * @return Number of items in the table.
     */
    size_t NumItems() const {
        return num_items_;
    }

    /*!
     * Returns the total num of items in the table.
     *
     * @return Number of items in the table.
     */
    void SetNumItems(size_t num_items) {
        num_items_ = num_items;
    }

    /*!
     * Returns the vector of bucket blocks.
     *
     * @return Vector of bucket blocks.
     */
    std::vector<BucketBlock*> & Items() {
        return buckets_;
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
     * Returns the begin local index.
     *
     * @return Begin local index.
     */
    size_t BeginLocalIndex() {
        return begin_local_index_;
    }

    /*!
     * Returns the end local index.
     *
     * @return End local index.
     */
    size_t EndLocalIndex() {
        return end_local_index_;
    }

    /*!
     * Returns the neutral element.
     *
     * @return Neutral element.
     */
    Value NeutralElement() {
        return neutral_element_;
    }

    /*!
     * Resizes the table by increasing the number of buckets using some
     * scale factor (num_items_resize_scale_). All items are rehashed as
     * part of the operation.
     */
    void ResizeUp() {
        LOG << "Resizing";
        num_buckets_ *= num_buckets_resize_scale_;
        num_items_ = 0;

        // move old hash array
        std::vector<BucketBlock*> buckets_old;
        std::swap(buckets_old, buckets_);

        // init new hash array
        buckets_.resize(num_buckets_, NULL);

        // rehash all items in old array
        for (BucketBlock* b_block : buckets_old)
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

        for (BucketBlock* b_block : buckets_)
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

        num_items_ = 0;
        LOG << "Cleared";
    }

    /*!
     * Removes all items from the table, but does not flush them. However, it does
     * reset the table to it's initial size.
     */
    void Reset() {
        LOG << "Resetting";
        num_buckets_ = num_buckets_init_scale_;

        for (BucketBlock*& b_block : buckets_)
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

        buckets_.resize(num_buckets_, NULL);
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
            if (buckets_[i] == NULL)
            {
                LOG << "bucket id: "
                    << i
                    << " empty";
                continue;
            }

            std::string log = "";

            BucketBlock* current = buckets_[i];
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
    //! Scale factor to compute the initial bucket size.
    size_t num_buckets_init_scale_;

    //! Scale factor to compute the number of buckets
    //! during resize relative to current size.
    size_t num_buckets_resize_scale_;

    // Maximal number of items per bucket before resize.
    size_t max_num_items_per_bucket_;

    //! Maximal number of items before some items
    //! are flushed (-> partial flush).
    size_t max_num_items_table_;

    //! Number of buckets
    size_t num_buckets_;

    //! Keeps the total number of items in the table.
    size_t num_items_ = 0;

    //! Key extractor function for extracting a key from a value.
    KeyExtractor key_extractor_;

    //! Reduce function for reducing two values.
    ReduceFunction reduce_function_;

    //! Set of emitters, one per partition.
    std::vector<EmitterFunction> emit_;

    //! Data structure for actually storing the items.
    std::vector<BucketBlock*> buckets_;

    //! Index Calculation functions: Hash or ByIndex.
    IndexFunction index_function_;

    //! Comparator function for keys.
    EqualToFunction equal_to_function_;

    //! Comparator function for keys.
    FlushFunction flush_function_;

    //! Begin local index (reduce to index)
    size_t begin_local_index_;

    //! End local index (reduce to index)
    size_t end_local_index_;

    //! Neutral element (reduce to index)
    Value neutral_element_;
};

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_REDUCE_POST_TABLE_HEADER

/******************************************************************************/
