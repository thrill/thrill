/*******************************************************************************
 * thrill/core/reduce_pre_probing_table.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER
#define THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/post_probing_reduce_flush.hpp>
#include <thrill/core/post_probing_reduce_flush_to_index.hpp>
#include <thrill/core/reduce_probing_table.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cassert>
#include <climits>
#include <cmath>
#include <functional>
#include <limits>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

template <typename Key, typename HashFunction = std::hash<Key> >
class PreReduceByHashKey
{
public:
    struct IndexResult {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    explicit PreReduceByHashKey(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function) { }

    IndexResult operator () (const Key& k,
                             const size_t& num_partitions,
                             const size_t& num_buckets_per_partition,
                             const size_t& num_buckets_per_table,
                             const size_t& offset) const {

        (void)num_partitions;
        (void)offset;

        size_t global_index = hash_function_(k) % num_buckets_per_table;

        return IndexResult { global_index / num_buckets_per_partition, global_index };
    }

private:
    HashFunction hash_function_;
};

template <typename Key>
class PreReduceByIndex
{
public:
    struct IndexResult {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    size_t size_;

    explicit PreReduceByIndex(size_t size) : size_(size) { }

    IndexResult
    operator () (const Key& k,
                 const size_t& num_partitions,
                 const size_t& num_buckets_per_partition,
                 const size_t& num_buckets_per_table,
                 const size_t& offset) const {

        (void)num_buckets_per_partition;
        (void)offset;

        return IndexResult { k* num_partitions / size_, k* num_buckets_per_table / size_ };
    }
};

template <bool, typename Emitters, typename KeyValuePair>
struct PreProbingEmitImpl;

template <typename Emitters, typename KeyValuePair>
struct PreProbingEmitImpl<true, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p.second);
    }
};

template <typename Emitters, typename KeyValuePair>
struct PreProbingEmitImpl<false, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p);
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename FlushFunction = PostProbingReduceFlush<Key, Value, ReduceFunction>,
          typename IndexFunction = PreReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          const bool FullPreReduce = false>
class ReducePreProbingTable
    : public ReduceProbingTable<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          RobustKey,
          IndexFunction, EqualToFunction,
          ReducePreProbingTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, RobustKey, FlushFunction,
              IndexFunction, EqualToFunction, FullPreReduce>
          >
{
    static const bool debug = true;

    static const size_t flush_mode = 0; // 0... 1-factor, 1... fullest, 4... random

    using Super = ReduceProbingTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction,
              RobustKey,
              IndexFunction, EqualToFunction, ReducePreProbingTable>;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using Emitters = std::vector<data::DynBlockWriter>;

    PreProbingEmitImpl<RobustKey, Emitters, KeyValuePair> emit_impl_;

    /**
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     *
     * \param context Context.
     *
     * \param key_extractor Key extractor function to extract a key from a
     * value.
     *
     * \param reduce_function Reduce function to reduce to values.
     *
     * \param emit A set of BlockWriter to flush items. One BlockWriter per
     * partition.
     *
     * \param sentinel Sentinel element used to flag free slots.
     *
     * \param begin_local_index Begin index for reduce to index.
     *
     * \param index_function Function to be used for computing the slot the item
     * to be inserted.
     *
     * \param flush_function Function to be used for flushing all items in the
     * table.
     *
     * \param end_local_index End index for reduce to index.
     *
     * \param neutral element Neutral element for reduce to index.
     *
     * \param byte_size Maximal size of the table in byte. In case size of table
     * exceeds that value, items are spilled to disk.
     *
     * \param max_partition_fill_rate Maximal number of items per partition
     * relative to number of slots allowed to be filled. It the rate is
     * exceeded, items get spilled to disk.
     *
     * \param partition_rate Rate of number of buckets to number of
     * partitions. There is one file writer per partition.
     *
     * \param equal_to_function Function for checking equality of two keys.
     *
     * \param spill_function Function implementing a strategy to spill items to
     * disk.
     */
    ReducePreProbingTable(
        Context& ctx,
        size_t num_partitions,
        const KeyExtractor& key_extractor,
        const ReduceFunction& reduce_function,
        std::vector<data::DynBlockWriter>& emit,
        const IndexFunction& index_function,
        const FlushFunction& flush_function,
        const Key& sentinel = Key(),
        const Value& neutral_element = Value(),
        size_t byte_size = 1024* 16,
        double max_partition_fill_rate = 0.6,
        const EqualToFunction& equal_to_function = EqualToFunction())
        : Super(num_partitions,
                key_extractor,
                reduce_function,
                index_function,
                equal_to_function),
          ctx_(ctx),
          byte_size_(byte_size),
          emit_(emit),
          flush_function_(flush_function),
          neutral_element_(neutral_element) {

        assert(num_partitions > 0);
        assert(num_partitions == emit.size());
        assert(byte_size >= 0 &&
               "byte_size must be greater than or equal to 0. "
               "a byte size of zero results in exactly one item per partition");

        assert(max_partition_fill_rate >= 0.0 && max_partition_fill_rate <= 1.0
               && "max_partition_fill_rate must be between 0.0 and 1.0. "
               "with a fill rate of 0.0, items are immediately flushed.");

        partition_size_ = std::max<size_t>(
            (size_t)((byte_size_ / static_cast<double>(sizeof(KeyValuePair)))
                     / static_cast<double>(num_partitions_)),
            1);

        size_ = partition_size_ * num_partitions_;

        limit_items_per_partition_ =
            (size_t)(partition_size_ * max_partition_fill_rate);

        assert(num_partitions_ > 0);
        assert(partition_size_ > 0);
        assert(size_ > 0);
        assert(limit_items_per_partition_ >= 0);

        items_per_partition_.resize(num_partitions_, 0);

        total_items_per_partition_.resize(num_partitions, 0);

        for (size_t i = 0; i < num_partitions_; i++) {
            partition_files_.push_back(ctx.GetFile());
        }

        sentinel_ = KeyValuePair(sentinel, Value());
        items_.resize(size_, sentinel_);

        for (size_t i = 0; i < emit.size(); i++) {
            emit_stats_.push_back(0);
        }

        partition_sequence_.resize(num_partitions_, 0);
        if (flush_mode == 0)
        {
            ComputeOneFactor(num_partitions_, ctx_.my_rank());
        }
        else if (flush_mode == 4)
        {
            size_t idx = 0;
            for (size_t i = 0; i < num_partitions_; i++)
            {
                if (i != ctx_.my_rank()) {
                    partition_sequence_[idx++] = i;
                }
            }
            std::random_shuffle(partition_sequence_.begin(), partition_sequence_.end() - 1);
            partition_sequence_[num_partitions_ - 1] = ctx_.my_rank();
        }
    }

    ReducePreProbingTable(
        Context& ctx, size_t num_partitions, KeyExtractor key_extractor,
        ReduceFunction reduce_function, std::vector<data::DynBlockWriter>& emit,
        const Key& sentinel)
        : ReducePreProbingTable(
              ctx, num_partitions, key_extractor, reduce_function,
              emit, sentinel, IndexFunction(),
              FlushFunction(reduce_function))
    { }

    //! non-copyable: delete copy-constructor
    ReducePreProbingTable(const ReducePreProbingTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreProbingTable& operator = (const ReducePreProbingTable&) = delete;

    void SpillOnePartition(size_t partition_id) {

        if (FullPreReduce) {
            this->SpillPartition(partition_id);
        }
        else {
            FlushPartition(partition_id);
        }
    }

    /*!
     * Flush.
     */
    void Flush(bool consume = true) {

        if (flush_mode == 1) {
            size_t idx = 0;
            for (size_t i = 0; i != num_partitions_; i++) {
                if (i != ctx_.my_rank())
                    partition_sequence_[idx++] = i;
            }

            if (FullPreReduce) {
                std::vector<size_t> sum_items_per_partition_;
                sum_items_per_partition_.resize(num_partitions_, 0);
                for (size_t i = 0; i != num_partitions_; ++i) {
                    sum_items_per_partition_[i] += items_per_partition_[i];
                    sum_items_per_partition_[i] += total_items_per_partition_[i];
                    if (consume)
                        total_items_per_partition_[i] = 0;
                }
                std::sort(partition_sequence_.begin(), partition_sequence_.end() - 1,
                          [&](size_t i1, size_t i2) {
                              return sum_items_per_partition_[i1] < sum_items_per_partition_[i2];
                          });
            }
            else {
                std::sort(partition_sequence_.begin(), partition_sequence_.end() - 1,
                          [&](size_t i1, size_t i2) {
                              return items_per_partition_[i1] < items_per_partition_[i2];
                          });
            }

            partition_sequence_[num_partitions_ - 1] = ctx_.my_rank();
        }

        if (FullPreReduce) {
            flush_function_.FlushTable(consume, *this);
        }
        else {
            for (size_t i : partition_sequence_) {
                FlushPartition(i);
            }
        }
    }

    /*!
     * Flushes all items of a partition.
     *
     * \param partition_id The id of the partition to be flushed.
     */
    void FlushPartition(size_t partition_id) {
        LOG << "Flushing items of partition with id: " << partition_id;

        for (size_t i = partition_id * partition_size_;
             i < (partition_id + 1) * partition_size_; i++)
        {
            KeyValuePair& current = items_[i];
            if (current.first != sentinel_.first)
            {
                EmitAll(current, partition_id);

                items_[i] = sentinel_;
            }
        }

        if (flush_mode == 1)
        {
            total_items_per_partition_[partition_id] -= items_per_partition_[partition_id];
        }

        // reset partition specific counter
        items_per_partition_[partition_id] = 0;
        // flush elements pushed into emitter
        emit_[partition_id].Flush();

        LOG << "Flushed items of partition with id: " << partition_id;
    }

    /*!
     * Emits element to all children
     */
    void EmitAll(const KeyValuePair& p, const size_t& partition_id) {
        emit_stats_[partition_id]++;
        emit_impl_.EmitElement(p, partition_id, emit_);
    }

    /*!
     * Returns the size of the table. The size corresponds to the number of
     * slots.  A slot may be free or used.
     *
     * \return Size of the table.
     */
    size_t Size() const { return size_; }

    /*!
     * Returns the total num of items in the table.
     *
     * \return Number of items in the table.
     */
    size_t NumItems() const {

        size_t total_num_items = 0;
        for (size_t num_items : items_per_partition_) {
            total_num_items += num_items;
        }

        return total_num_items;
    }

    //! Returns the number of partitions.
    size_t NumPartitions() const { return num_partitions_; }

    //! Returns the vector of partition files.
    std::vector<data::File> & PartitionFiles() { return partition_files_; }

    //! Returns the vector of number of items per partition.
    std::vector<size_t> & NumItemsPerPartition() { return items_per_partition_; }

    //! Returns the vector of number of items per partition.
    size_t PartitionSize() { return partition_size_;    }

    //! Returns the vector of key/value pairs.
    std::vector<KeyValuePair> & Items() { return items_;    }

    //! Returns the sentinel element.
    KeyValuePair Sentinel() const { return sentinel_; }

    //! Returns the partition size.
    size_t PartitionSize() const { return partition_size_; }

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

    //! Returns the context
    Context & Ctx() { return ctx_; }

    //! Returns the neutral element.
    Value NeutralElement() const { return neutral_element_; }

    //! Returns the local index range.
    common::Range LocalIndex() const { return common::Range(0, size_ - 1); }

    //! Returns the sequence of partition ids to be processed on flush.
    std::vector<size_t> & PartitionSequence() { return partition_sequence_; }

    /*!
    * Closes all emitter.
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
     * Computes the one 1-factor sequence
     */
    void ComputeOneFactor(const size_t& p_raw,
                          const size_t& j) {
        assert(p_raw > 0);
        assert(j >= 0);
        assert(j < p_raw);

        const size_t p = (p_raw % 2 == 0) ? p_raw - 1 : p_raw;
        std::vector<size_t> p_i(p);

        for (size_t i = 0; i < p; i++) {
            if (i == 0) {
                p_i[i] = 0;
                continue;
            }
            p_i[i] = p - i;
        }

        size_t a = 0;
        for (size_t i = 0; i < p; i++) {
            if (p != p_raw && j == p) {
                partition_sequence_[i] = ((p_raw / 2) * i) % (p_raw - 1);
                continue;
            }

            int idx = j - i;
            if (idx < 0) {
                idx = p + (j - i);
            }
            if (p_i[idx] == j) {
                if (p == p_raw) {
                    continue;
                }
                else {
                    partition_sequence_[a++] = p;
                    continue;
                }
            }
            partition_sequence_[a++] = p_i[idx];
        }

        partition_sequence_[p_raw - 1] = j;
    }

private:
    using Super::num_partitions_;
    using Super::key_extractor_;
    using Super::reduce_function_;
    using Super::index_function_;
    using Super::equal_to_function_;
    using Super::size_;
    using Super::items_;
    using Super::limit_items_per_partition_;
    using Super::items_per_partition_;
    using Super::partition_size_;
    using Super::sentinel_;
    using Super::partition_files_;

    //! Context
    Context& ctx_;

    //! Size of the table in bytes
    size_t byte_size_ = 0;

    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& emit_;

    //! Comparator function for keys.
    FlushFunction flush_function_;

    //! Emitter stats.
    std::vector<int> emit_stats_;

    //! Number of items per partition.
    std::vector<size_t> total_items_per_partition_;

    //! Neutral element (reduce to index).
    Value neutral_element_;

    //! Partition Sequence.
    std::vector<size_t> partition_sequence_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_PROBING_TABLE_HEADER

/******************************************************************************/
