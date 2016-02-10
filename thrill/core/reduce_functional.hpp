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

namespace thrill {
namespace core {

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
        const uint64_t& seed = 0,
        const HashFunction& hash_function = HashFunction())
        : seed_(seed), hash_function_(hash_function) { }

    ReduceByHashKey(
        const uint64_t& seed, const ReduceByHashKey& other)
        : seed_(seed), hash_function_(other.hash_function_) { }

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
    uint64_t seed_;
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

template <typename Key>
class PostReduceByIndex
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;
    };

    IndexResult operator () (const Key& k,
                             const size_t& num_partitions,
                             const size_t& num_buckets_per_partition,
                             const size_t& num_buckets_per_table,
                             const size_t& offset) const {

        (void)num_buckets_per_partition;

        size_t result = (k - offset) % num_buckets_per_table;

        return IndexResult { result / num_partitions, result };
    }
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_REDUCE_FUNCTIONAL_HEADER

/******************************************************************************/
