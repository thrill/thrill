/*******************************************************************************
 * thrill/core/reduce_pre_bucket_table.hpp
 *
 * Hash table with support for reduce and partitions.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_REDUCE_PRE_BUCKET_TABLE_HEADER
#define THRILL_CORE_REDUCE_PRE_BUCKET_TABLE_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/post_bucket_reduce_flush.hpp>
#include <thrill/core/post_bucket_reduce_flush_to_index.hpp>
#include <thrill/core/reduce_bucket_table.hpp>
#include <thrill/core/reduce_pre_probing_table.hpp>
#include <thrill/data/block_writer.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <functional>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

template <bool, typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl;

template <typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl<true, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p.second);
    }
};

template <typename Emitters, typename KeyValuePair>
struct PreBucketEmitImpl<false, Emitters, KeyValuePair>{
    void EmitElement(const KeyValuePair& p, const size_t& partition_id, Emitters& emit) {
        emit[partition_id].Put(p);
    }
};

template <typename ValueType, typename Key, typename Value,
          typename KeyExtractor, typename ReduceFunction,
          const bool RobustKey = false,
          typename FlushFunction = PostBucketReduceFlush<Key, Value, ReduceFunction>,
          typename IndexFunction = PreReduceByHashKey<Key>,
          typename EqualToFunction = std::equal_to<Key>,
          size_t TargetBlockSize = 16*16,
          const bool FullPreReduce = false>
class ReducePreBucketTable
    : public ReduceBucketTable<
          ValueType, Key, Value,
          KeyExtractor, ReduceFunction,
          RobustKey,
          IndexFunction, EqualToFunction, TargetBlockSize,
          ReducePreBucketTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction, RobustKey, FlushFunction,
              IndexFunction, EqualToFunction, TargetBlockSize, FullPreReduce>
          >
{
    static const bool debug = false;

    static const size_t flush_mode = 0; // 0... 1-factor, 1... fullest, 4... random

    using Super = ReduceBucketTable<
              ValueType, Key, Value,
              KeyExtractor, ReduceFunction,
              RobustKey,
              IndexFunction, EqualToFunction, TargetBlockSize, ReducePreBucketTable>;

public:
    using KeyValuePair = std::pair<Key, Value>;

    using Emitters = std::vector<data::DynBlockWriter>;

    PreBucketEmitImpl<RobustKey, Emitters, KeyValuePair> emit_impl_;

    using typename Super::BucketBlock;
    using Super::block_size_;

    /**
     * A data structure which takes an arbitrary value and extracts a key using
     * a key extractor function from that value. Afterwards, the value is hashed
     * based on the key into some slot.
     *
     * \param num_partitions The number of partitions.
     * \param key_extractor Key extractor function to extract a key from a value.
     * \param reduce_function Reduce function to reduce to values.
     * \param emit A set of BlockWriter to flush items. One BlockWriter per partition.
     * \param byte_size Maximal size of the table in byte. In case size of table exceeds that value, items
     *                  are flushed.
     * \param bucket_rate Ratio of number of blocks to number of buckets in the table.
     * \param max_partition_fill_rate Maximal number of blocks per partition relative to number of slots allowed
     *                                to be filled. It the rate is exceeded, items get flushed.
     * \param index_function Function to be used for computing the bucket the item to be inserted.
     * \param equal_to_function Function for checking equality fo two keys.
     */
    ReducePreBucketTable(Context& ctx,
                         size_t num_partitions,
                         KeyExtractor key_extractor,
                         ReduceFunction reduce_function,
                         std::vector<data::DynBlockWriter>& emit,
                         const IndexFunction& index_function,
                         const FlushFunction& flush_function,
                         const Key& /* sentinel */ = Key(),
                         const Value& neutral_element = Value(),
                         size_t byte_size = 1024* 16,
                         double bucket_rate = 1.0,
                         double max_partition_fill_rate = 0.6,
                         const EqualToFunction& equal_to_function = EqualToFunction())
        : Super(num_partitions,
                key_extractor,
                reduce_function,
                index_function,
                equal_to_function),
          ctx_(ctx),
          emit_(emit),
          byte_size_(byte_size),
          flush_function_(flush_function),
          neutral_element_(neutral_element) {
        sLOG << "creating ReducePreBucketTable with" << emit_.size() << "output emitters";

        assert(num_partitions == emit.size());
        assert(byte_size >= 0 && "byte_size must be greater than or equal to 0. "
               "a byte size of zero results in exactly one item per partition");
        assert(max_partition_fill_rate >= 0.0 && max_partition_fill_rate <= 1.0 && "max_partition_fill_rate "
               "must be between 0.0 and 1.0. with a fill rate of 0.0, items are immediately flushed.");
        assert(bucket_rate >= 0.0 && "bucket_rate must be greater than or equal 0. "
               "a bucket rate of 0.0 causes exacty 1 bucket per partition.");

        max_blocks_per_partition_ =
            std::max<size_t>((size_t)((byte_size_ / num_partitions_)
                                      / static_cast<double>(sizeof(BucketBlock))), 1);

        max_items_per_partition_ = max_blocks_per_partition_ * block_size_;

        limit_items_per_partition_ = (size_t)(static_cast<double>(max_items_per_partition_)
                                              * max_partition_fill_rate);

        num_buckets_per_partition_ =
            std::max<size_t>((size_t)(static_cast<double>(max_blocks_per_partition_)
                                      * bucket_rate), 1);

        // reduce max number of blocks per partition to cope for the memory needed for pointers
        max_blocks_per_partition_ -=
            std::max<size_t>((size_t)(std::ceil(
                                          static_cast<double>(num_buckets_per_partition_ * sizeof(BucketBlock*))
                                          / static_cast<double>(sizeof(BucketBlock)))), 0);

        max_blocks_per_partition_ = std::max<size_t>(max_blocks_per_partition_, 1);

        num_buckets_ = num_buckets_per_partition_ * num_partitions_;
        limit_blocks_ = max_blocks_per_partition_ * num_partitions_;

        assert(max_blocks_per_partition_ > 0);
        assert(max_items_per_partition_ > 0);
        assert(limit_items_per_partition_ >= 0);
        assert(num_buckets_per_partition_ > 0);
        assert(num_buckets_ > 0);
        assert(limit_blocks_ > 0);

        buckets_.resize(num_buckets_, nullptr);

        total_items_per_partition_.resize(num_partitions_, 0);

        for (size_t i = 0; i < num_partitions_; i++) {
            partition_files_.push_back(ctx.GetFile());
        }
        for (size_t i = 0; i < num_partitions_; i++) {
            partition_writers_.push_back(partition_files_[i].GetWriter());
        }

        for (size_t i = 0; i < emit_.size(); i++)
            emit_stats_.push_back(0);

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

    ReducePreBucketTable(Context& ctx, size_t num_partitions, KeyExtractor key_extractor,
                         ReduceFunction reduce_function,
                         std::vector<data::DynBlockWriter>& emit)
        : ReducePreBucketTable(
              ctx, num_partitions, key_extractor, reduce_function, emit, IndexFunction(),
              FlushFunction(reduce_function)) { }

    //! non-copyable: delete copy-constructor
    ReducePreBucketTable(const ReducePreBucketTable&) = delete;
    //! non-copyable: delete assignment operator
    ReducePreBucketTable& operator = (const ReducePreBucketTable&) = delete;

    void SpillAnyPartition(size_t current_id) {
        if (FullPreReduce) {
            SpillOnePartition(current_id);
        }
        else {
            FlushPartition(current_id);
        }
    }

    void SpillPartition(size_t partition_id) {
        if (FullPreReduce) {
            SpillOnePartition(partition_id);
        }
        else {
            FlushPartition(partition_id);
        }
    }

    /*!
     * Spills all items of a partition.
     *
     * \param partition_id The id of the partition to be flushed.
     */
    void SpillOnePartition(size_t partition_id) {

        data::File::Writer& writer = partition_writers_[partition_id];

        for (size_t i = partition_id * num_buckets_per_partition_;
             i < (partition_id + 1) * num_buckets_per_partition_; i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != nullptr)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    writer.Put(*bi);
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                block_pool_.Deallocate(current);
                current = next;
            }

            buckets_[i] = nullptr;
        }

        if (flush_mode == 1)
        {
            total_items_per_partition_[partition_id] += num_items_per_partition_[partition_id];
        }

        // reset partition specific counter
        num_items_per_partition_[partition_id] = 0;
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
                    sum_items_per_partition_[i] += num_items_per_partition_[i];
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
                              return num_items_per_partition_[i1] < num_items_per_partition_[i2];
                          });
            }

            partition_sequence_[num_partitions_ - 1] = ctx_.my_rank();
        }

        if (FullPreReduce) {
            flush_function_.FlushTable(consume, this);
        }
        else {

            for (size_t i : partition_sequence_)
            {
                FlushPartition(i);
            }
        }
    }

    /*!
     * Retrieves all items belonging to the partition
     * having the most items. Retrieved items are then pushed
     * to the provided emitter.
     */
    void FlushLargestPartition() {
        LOG << "Flushing items of largest partition";

        // get partition with max size
        size_t p_size_max = 0;
        size_t p_idx = 0;
        for (size_t i = 0; i < num_partitions_; i++)
        {
            if (num_items_per_partition_[i] > p_size_max)
            {
                p_size_max = num_items_per_partition_[i];
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

        if (p_size_max == 0) {
            return;
        }

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
             i < (partition_id + 1) * num_buckets_per_partition_; i++)
        {
            BucketBlock* current = buckets_[i];

            while (current != nullptr)
            {
                for (KeyValuePair* bi = current->items;
                     bi != current->items + current->size; ++bi)
                {
                    EmitAll(*bi, partition_id);
                }

                // destroy block and advance to next
                BucketBlock* next = current->next;
                block_pool_.Deallocate(current);
                current = next;
            }

            buckets_[i] = nullptr;
        }

        if (flush_mode == 1)
        {
            total_items_per_partition_[partition_id] -= num_items_per_partition_[partition_id];
        }

        // reset partition specific counter
        num_items_per_partition_[partition_id] = 0;
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
     * returns the total num of buckets in the table.
     *
     * \return Number of buckets in the table.
     */
    size_t NumBucketsPerTable() const {
        return num_buckets_;
    }

    /*!
     * Sets the num of blocks in the table.
     */
    void SetNumBlocksPerTable(const size_t num_blocks) {
        num_blocks_ = num_blocks;
    }

    /*!
     * Returns the total num of items in the table.
     *
     * \return Number of items in the table.
     */
    size_t NumItemsPerTable() const {
        size_t total_num_items = 0;
        for (size_t num_items : num_items_per_partition_) {
            total_num_items += num_items;
        }

        return total_num_items;
    }

    /*!
     * Returns the number of buckets per partition.
     *
     * \return Number of buckets per partition.
     */
    size_t NumBucketsPerFrame() const {
        return num_buckets_per_partition_;
    }

    /*!
     * Returns the number of partitions.
     *
     * \return The number of partitions.
     */
    size_t NumFrames() const {
        return num_partitions_;
    }

    /*!
     * Returns the vector of bucket blocks.
     *
     * \return Vector of bucket blocks.
     */
    std::vector<BucketBlock*> & Items() {
        return buckets_;
    }

    /*!
     * Returns the vector of partition_files.
     *
     * \return Vector of partition_files.
     */
    std::vector<data::File> & FrameFiles() {
        return partition_files_;
    }

    /*!
     * Returns the vector of partition_writers.
     *
     * \return Vector of partition_writers.
     */
    std::vector<data::File::Writer> & FrameWriters() {
        return partition_writers_;
    }

    /*!
     * Returns the number of items of a partition.
     *
     * \param partition_id The id of the partition the number of
     *                  blocks to be returned..
     * \return The number of items in the partitions.
     */
    size_t NumItemsPerPartition(size_t partition_id) {
        return num_items_per_partition_[partition_id];
    }

    /*!
     * Returns the vector of number of items per partition_in internal memory.
     *
     * \return Vector of number of items per partition_in internal memory.
     */
    std::vector<size_t> & NumItemsMemPerFrame() {
        return num_items_per_partition_;
    }

    /*!
     * Returns the block size.
     *
     * \return Block size.
     */
    double BlockSize() const {
        return block_size_;
    }

    //! Returns the block pool
    BucketBlockPool<BucketBlock> & BlockPool() {
        return block_pool_;
    }

    /*!
     * Returns the vector of key/value pairs.s
     *
     * \return Vector of key/value pairs.
     */
    Context & Ctx() {
        return ctx_;
    }

    /*!
     * Returns the number of block in the table.
     *
     * \return Number of blocks in the table.
     */
    size_t NumBlocksPerTable() const {
        return num_blocks_;
    }

    /*!
    * Returns the neutral element.
    *
    * \return Neutral element.
    */
    Value NeutralElement() const {
        return neutral_element_;
    }

    /*!
    * Returns the local index range.
    *
    * \return Begin local index.
    */
    common::Range LocalIndex() const {
        return common::Range(0, num_buckets_ - 1);
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

    /*!
     * Returns the sequence of partition_ids to
     * be processed on flush.
     */
    std::vector<size_t> & FrameSequence() {
        return partition_sequence_;
    }

private:
    using Super::buckets_;
    using Super::key_extractor_;
    using Super::reduce_function_;
    using Super::index_function_;
    using Super::equal_to_function_;
    using Super::block_pool_;
    using Super::num_partitions_;
    using Super::num_buckets_;
    using Super::num_buckets_per_partition_;
    using Super::max_items_per_partition_;
    using Super::max_blocks_per_partition_;
    using Super::num_blocks_;
    using Super::limit_blocks_;
    using Super::num_items_per_partition_;
    using Super::limit_items_per_partition_;

    //! Context
    Context& ctx_;

    //! Set of emitters, one per partition.
    std::vector<data::DynBlockWriter>& emit_;

    //! Size of the table in bytes
    size_t byte_size_ = 0;

    //! Flush function.
    FlushFunction flush_function_;

    //! Number of items per partition.
    std::vector<size_t> total_items_per_partition_;

    //! Emitter stats.
    std::vector<size_t> emit_stats_;

    //! Store the files for frames.
    std::vector<data::File> partition_files_;

    //! Store the writers for frames.
    std::vector<data::File::Writer> partition_writers_;

    //! Neutral element (reduce to index).
    Value neutral_element_;

    //! Partition_Sequence.
    std::vector<size_t> partition_sequence_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_PRE_BUCKET_TABLE_HEADER

/******************************************************************************/
