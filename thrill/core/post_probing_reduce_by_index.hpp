/*******************************************************************************
 * thrill/core/post_probing_reduce_by_index.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_POST_PROBING_REDUCE_BY_INDEX_HEADER
#define THRILL_CORE_POST_PROBING_REDUCE_BY_INDEX_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>

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

class PostProbingReduceByIndex
{
public:
    PostProbingReduceByIndex() { }

    template <typename Table>
    size_t
    operator () (const size_t& k, Table* ht, const size_t& size) const {

        return (k - ht->BeginLocalIndex()) % size;
    }
};

}
}

#endif //THRILL_CORE_POST_PROBING_REDUCE_BY_INDEX_HEADER
