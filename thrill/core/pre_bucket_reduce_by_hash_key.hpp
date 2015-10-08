/*******************************************************************************
 * thrill/core/pre_bucket_reduce_by_hash_key.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_PRE_BUCKET_REDUCE_BY_HASH_KEY_HEADER
#define THRILL_CORE_PRE_BUCKET_REDUCE_BY_HASH_KEY_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_writer.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <functional>
#include <iostream>
#include <limits>
#include <string>
#include <typeinfo>
#include <utility>
#include <vector>

namespace thrill {
namespace core {

template <typename Key, typename HashFunction = std::hash<Key> >
class PreBucketReduceByHashKey
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;

        IndexResult(size_t p_id, size_t g_id) {
            partition_id = p_id;
            global_index = g_id;
        }
    };

    explicit PreBucketReduceByHashKey(const HashFunction& hash_function = HashFunction())
            : hash_function_(hash_function)
    { }

    template <typename Table>
    IndexResult
    operator () (const Key& k, Table* ht) const {

        size_t hashed = hash_function_(k);

        size_t partition_id = hashed % ht->NumPartitions();

        return IndexResult(partition_id,
                           partition_id * ht->NumBucketsPerPartition() + (hashed % ht->NumBucketsPerPartition()));
    }

private:
    HashFunction hash_function_;
};

}
}

#endif //THRILL_CORE_PRE_REDUCE_BY_HASH_KEY_HEADER
