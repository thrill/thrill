/*******************************************************************************
 * thrill/core/post_bucket_reduce_by_hash_key.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_POST_BUCKET_REDUCE_BY_HASH_KEY_HEADER
#define THRILL_CORE_POST_BUCKET_REDUCE_BY_HASH_KEY_HEADER

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

template <typename Key, typename HashFunction = std::hash<Key> >
class PostBucketReduceByHashKey
{
public:
    explicit PostBucketReduceByHashKey(const HashFunction& hash_function = HashFunction())
            : hash_function_(hash_function)
    { }

    template <typename Table>
    size_t
    operator () (const Key& k, Table* ht, const size_t& size) const {

        (void)ht;

        size_t hashed = hash_function_(k);

        return hashed % size;
    }

private:
    HashFunction hash_function_;
};

}
}

#endif //THRILL_CORE_POST_BUCKET_REDUCE_BY_HASH_KEY_HEADER
