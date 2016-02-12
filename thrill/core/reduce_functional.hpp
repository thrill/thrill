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

#include <thrill/common/math.hpp>

namespace thrill {
namespace core {

// This is the Hash128to64 function from Google's cityhash (available under the
// MIT License).
static inline uint64_t Hash128to64(const uint64_t upper, const uint64_t lower) {
    // Murmur-inspired hashing.
    const uint64_t k = 0x9DDFEA08EB382D69ull;
    uint64_t a = (lower ^ upper) * k;
    a ^= (a >> 47);
    uint64_t b = (upper ^ a) * k;
    b ^= (b >> 47);
    b *= k;
    return b;
}

template <typename Key, typename HashFunction = std::hash<Key> >
class ReduceByHashKey
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    explicit ReduceByHashKey(
        const uint64_t& salt = 0,
        const HashFunction& hash_function = HashFunction())
        : salt_(salt), hash_function_(hash_function) { }

    ReduceByHashKey(
        const uint64_t& salt, const ReduceByHashKey& other)
        : salt_(salt), hash_function_(other.hash_function_) { }

    IndexResult operator () (const Key& k,
                             const size_t& num_partitions,
                             const size_t& num_buckets_per_partition,
                             const size_t& num_buckets_per_table) const {

        (void)num_partitions;

        uint64_t hash = Hash128to64(salt_, hash_function_(k));

        size_t global_index = hash % num_buckets_per_table;
        size_t partition_id = global_index / num_buckets_per_partition;

        return IndexResult { partition_id, global_index };
    }

private:
    uint64_t salt_;
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

    explicit PreReduceByIndex(size_t begin = 0, size_t end = 0)
        : begin_(begin), size_(end - begin) { }

    IndexResult
    operator () (const Key& k,
                 const size_t& num_partitions,
                 const size_t& /* num_buckets_per_partition */,
                 const size_t& num_buckets_per_table) const {

        size_t partition_id = k * num_partitions / size_;
        size_t global_index = k * num_buckets_per_table / size_;

        return IndexResult { partition_id, global_index };
    }

private:
    size_t begin_, size_;
};

template <typename Key>
class ReduceByIndex
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    explicit ReduceByIndex(const common::Range& range)
        : range_(range) { }

    explicit ReduceByIndex(size_t begin = 0, size_t end = 0)
        : range_(begin, end) { }

    const common::Range & range() const { return range_; }

    void set_range(const common::Range& range) { range_ = range; }

    IndexResult operator () (const Key& k,
                             const size_t& /* num_partitions */,
                             const size_t& num_buckets_per_partition,
                             const size_t& num_buckets) const {

        assert(k >= range_.begin && k < range_.end && "Item out of range.");

        // round bucket number down
        size_t global_index = (k - range_.begin) * num_buckets / range_.size();

        return IndexResult { global_index / num_buckets_per_partition, global_index };
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

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_FUNCTIONAL_HEADER

/******************************************************************************/
