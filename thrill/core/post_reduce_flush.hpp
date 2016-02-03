/*******************************************************************************
 * thrill/core/post_reduce_flush.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_POST_REDUCE_FLUSH_HEADER
#define THRILL_CORE_POST_REDUCE_FLUSH_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/core/bucket_block_pool.hpp>
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cassert>
#include <climits>
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

template <typename Key, typename HashFunction>
class PostReduceByHashKey;

template <typename Key,
          typename Value,
          typename ReduceFunction,
          typename IndexFunction = PostReduceByHashKey<Key, std::hash<Key> >,
          typename EqualToFunction = std::equal_to<Key>,
          typename KeyValuePair = std::pair<Key, Value> >
class PostReduceFlush
{
public:
    PostReduceFlush(
        ReduceFunction reduce_function,
        const IndexFunction& index_function = IndexFunction(),
        const EqualToFunction& equal_to_function = EqualToFunction())
        : reduce_function_(reduce_function),
          index_function_(index_function),
          equal_to_function_(equal_to_function) { }

    template <typename Table>
    void FlushTable(bool consume, Table& ht) const {

        std::vector<data::File>& partition_files = ht.table_.partition_files();

        for (size_t id = 0; id < partition_files.size(); ++id) {

            // get the actual reader from the file
            data::File& file = partition_files[id];

            // only if items have been spilled, process a second reduce
            if (file.num_items() > 0) {

                data::File::Reader reader = file.GetReader(consume);

                // this does not work -tb
                abort();
                // Reduce<Table, BucketBlock>(ctx, consume, ht, items, offset,
                //                            length, reader, second_reduce,
                //                            fill_rate_num_items_per_partition,
                //                            partition_id, num_items_mem_per_partition, block_pool,
                //                            max_num_blocks_second_reduce, block_size);

                // no spilled items, just flush already reduced
                // data in primary table in current partition
            }
            else {
                /////
                // emit data
                /////
                ht.table_.FlushPartitionE(
                    id, consume,
                    [&](const size_t& partition_id, const KeyValuePair& bi) {
                        ht.EmitAll(partition_id, bi);
                    });
            }
        }
    }

private:
    ReduceFunction reduce_function_;
    IndexFunction index_function_;
    EqualToFunction equal_to_function_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_POST_REDUCE_FLUSH_HEADER

/******************************************************************************/
