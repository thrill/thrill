/*******************************************************************************
 * thrill/core/reduce_functional.hpp
 *
 * Hash table with support for reduce.
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
#ifndef THRILL_CORE_REDUCE_FUNCTIONAL_HEADER
#define THRILL_CORE_REDUCE_FUNCTIONAL_HEADER

#include <thrill/common/defines.hpp>
#include <thrill/common/hash.hpp>
#include <thrill/common/math.hpp>

namespace thrill {
namespace core {

/*!
 * A reduce index function which returns a hash index and partition. It is used
 * by ReduceByKey.
 */
template <typename Key, typename HashFunction = std::hash<Key> >
class ReduceByHash
{
public:
    struct Result {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! remaining hash bits for local index
        size_t remaining_hash;

        //! calculate local index into a partition containing a hash table of
        //! smaller size
        size_t local_index(size_t size) const {
            return remaining_hash % size;
        }
    };

    explicit ReduceByHash(
        const HashFunction& hash_function = HashFunction())
        : ReduceByHash(/* salt */ 0, hash_function) { }

    explicit ReduceByHash(
        const uint64_t& salt,
        const HashFunction& hash_function = HashFunction())
        : salt_(salt), hash_function_(hash_function) { }

    ReduceByHash(
        const uint64_t& salt, const ReduceByHash& other)
        : salt_(salt), hash_function_(other.hash_function_) { }

    Result operator () (
        const Key& k,
        const size_t& num_partitions,
        const size_t& /* num_buckets_per_partition */,
        const size_t& /* num_buckets_per_table */) const {

        uint64_t hash = common::Hash128to64(salt_, hash_function_(k));

        size_t partition_id = hash % num_partitions;
        size_t remaining_hash = hash / num_partitions;

        return Result { partition_id, remaining_hash };
    }

private:
    uint64_t salt_;
    HashFunction hash_function_;
};

/*!
 * A reduce index function, which determines a bucket depending on the current
 * index range [begin,end). It is used by ReduceToIndex.
 */
template <typename Key>
class ReduceByIndex
{
public:
    struct Result {
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index of the item among all local partition
        size_t global_index;
        //! saved parameter
        size_t num_buckets_per_partition;

        //! calculate local index into a partition containing a hash table of
        //! smaller size
        size_t local_index(size_t size) const {
            return global_index % num_buckets_per_partition
                   * size / num_buckets_per_partition;
        }
    };

    explicit ReduceByIndex(const common::Range& range)
        : range_(range) { }

    explicit ReduceByIndex(size_t begin = 0, size_t end = 0)
        : range_(begin, end) { }

    const common::Range& range() const { return range_; }

    void set_range(const common::Range& range) { range_ = range; }

    Result operator () (
        const Key& k,
        const size_t& /* num_partitions */,
        const size_t& num_buckets_per_partition,
        const size_t& num_buckets) const {

        assert(k >= range_.begin && k < range_.end && "Item out of range.");

        // round bucket number down
        size_t global_index = (k - range_.begin) * num_buckets / range_.size();

        return Result {
                   global_index / num_buckets_per_partition,
                   global_index, num_buckets_per_partition
        };
    }

    //! inverse mapping: takes a bucket index and returns the smallest index
    //! delivered to the bucket.
    size_t inverse(size_t bucket, const size_t& num_buckets) {
        // round inverse key up
        return range_.begin +
               (bucket * range_.size() + num_buckets - 1) / num_buckets;
    }

    //! deliver inverse range mapping of a partition
    common::Range inverse_range(
        size_t partition_id,
        const size_t& num_buckets_per_partition, const size_t& num_buckets) {
        return common::Range(
            inverse(partition_id * num_buckets_per_partition, num_buckets),
            inverse((partition_id + 1) * num_buckets_per_partition, num_buckets));
    }

private:
    common::Range range_;
};

/******************************************************************************/

//! template specialization switch class to convert a Value either to Value
//! (identity) or to a std::pair<Key, Value> with Key generated from Value using
//! a key extractor for VolatileKey implementations.
template <typename Value, typename TableItem, bool VolatileKey>
class ReduceMakeTableItem;

template <typename Value, typename TableItem>
class ReduceMakeTableItem<Value, TableItem, /* VolatileKey */ false>
{
public:
    template <typename KeyExtractor>
    static TableItem Make(const Value& v, KeyExtractor& /* key_extractor */) {
        return v;
    }

    template <typename KeyExtractor>
    static auto GetKey(const TableItem &t, KeyExtractor & key_extractor) {
        return key_extractor(t);
    }

    template <typename ReduceFunction>
    static auto Reduce(const TableItem &a, const TableItem &b,
                       ReduceFunction & reduce_function) {
        return reduce_function(a, b);
    }

    template <typename Emitter>
    static void Put(const TableItem& p, Emitter& emit) {
        emit(p);
    }
};

template <typename Value, typename TableItem>
class ReduceMakeTableItem<Value, TableItem, /* VolatileKey */ true>
{
public:
    template <typename KeyExtractor>
    static TableItem Make(const Value& v, KeyExtractor& key_extractor) {
        return TableItem(key_extractor(v), v);
    }

    template <typename KeyExtractor>
    static auto GetKey(const TableItem &t, KeyExtractor & /* key_extractor */) {
        return t.first;
    }

    template <typename ReduceFunction>
    static auto Reduce(const TableItem &a, const TableItem &b,
                       ReduceFunction & reduce_function) {
        return TableItem(a.first, reduce_function(a.second, b.second));
    }

    template <typename Emitter>
    static void Put(const TableItem& p, Emitter& emit) {
        emit(p.second);
    }
};

//! Emitter implementation to plug into a reduce hash table for
//! collecting/flushing items while reducing. Items flushed in the post-phase
//! are passed to the next DIA node for processing.
template <
    typename TableItem, typename Value, typename Emitter, bool VolatileKey>
class ReducePostPhaseEmitter
{
public:
    explicit ReducePostPhaseEmitter(const Emitter& emit)
        : emit_(emit) { }

    //! output an element into a partition, template specialized for VolatileKey
    //! and non-VolatileKey types
    void Emit(const TableItem& p) {
        ReduceMakeTableItem<Value, TableItem, VolatileKey>::Put(p, emit_);
    }

    //! output an element into a partition, template specialized for VolatileKey
    //! and non-VolatileKey types
    void Emit(const size_t& /* partition_id */, const TableItem& p) {
        Emit(p);
    }

public:
    //! Set of emitters, one per partition.
    Emitter emit_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_FUNCTIONAL_HEADER

/******************************************************************************/
