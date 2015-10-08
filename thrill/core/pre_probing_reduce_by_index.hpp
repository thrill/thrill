/*******************************************************************************
 * thrill/core/pre_probing_reduce_by_index.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_PRE_PROBING_REDUCE_BY_INDEX_HEADER
#define THRILL_PRE_PROBING_REDUCE_BY_INDEX_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>

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
#include <limits.h>
#include <stddef.h>

namespace thrill {
namespace core {

class PreProbingReduceByIndex
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

    size_t size_;

    explicit PreProbingReduceByIndex(size_t size)
            : size_(size)
    { }

    template <typename ReducePreProbingTable>
    IndexResult
    operator () (const size_t& k, ReducePreProbingTable* ht) const {

        return IndexResult(std::min(k * ht->NumPartitions() / size_, ht->NumPartitions()-1),
                           std::min(k * ht->Size() / size_, ht->Size()-1));
    }
};

}
}

#endif //THRILL_PRE_PROBING_REDUCE_BY_INDEX_HEADER
