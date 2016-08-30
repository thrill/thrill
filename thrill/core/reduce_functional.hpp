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
        const uint64_t& salt = 0,
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

//! template specialization switch class to output key+value if SendPair and
//! only value if not SendPair.
template <
    typename KeyValuePair, typename ValueType, typename Emitter, bool SendPair>
class ReducePostPhaseEmitterSwitch;

template <typename KeyValuePair, typename ValueType, typename Emitter>
class ReducePostPhaseEmitterSwitch<
        KeyValuePair, ValueType, Emitter, false>
{
public:
    static void Put(const KeyValuePair& p, Emitter& emit) {
        emit(p.second);
    }
};

template <typename KeyValuePair, typename ValueType, typename Emitter>
class ReducePostPhaseEmitterSwitch<
        KeyValuePair, ValueType, Emitter, true>
{
public:
    static void Put(const KeyValuePair& p, Emitter& emit) {
        emit(p);
    }
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_FUNCTIONAL_HEADER

/******************************************************************************/
