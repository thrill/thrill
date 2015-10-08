/*******************************************************************************
 * thrill/core/pre_bucket_reduce_by_index.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_PRE_BUCKET_REDUCE_BY_INDEX_HEADER
#define THRILL_CORE_PRE_BUCKET_REDUCE_BY_INDEX_HEADER

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

class PreBucketReduceByIndex
{
public:
    size_t size_;

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

    explicit PreBucketReduceByIndex(size_t size)
            : size_(size)
    { }

    template <typename Table>
    IndexResult
    operator () (const size_t k, Table* ht) const {

        return IndexResult(std::min(k * ht->NumFrames() / size_, ht->NumFrames() - 1),
                           std::min(ht->NumBucketsPerTable() - 1, k * ht->NumBucketsPerTable() / size_));
    }
};

}
}

#endif //THRILL_CORE_PRE_BUCKET_REDUCE_BY_INDEX_HEADER
