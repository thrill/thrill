/*******************************************************************************
 * thrill/core/post_bucket_reduce_flush.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_POST_BUCKET_REDUCE_FLUSH_HEADER
#define THRILL_CORE_POST_BUCKET_REDUCE_FLUSH_HEADER

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
class PostBucketReduceFlush
{
public:
    PostBucketReduceFlush(
        ReduceFunction reduce_function,
        const IndexFunction& index_function = IndexFunction(),
        const EqualToFunction& equal_to_function = EqualToFunction())
        : reduce_function_(reduce_function),
          index_function_(index_function),
          equal_to_function_(equal_to_function) { }

    template <typename Table>
    void FlushTable(bool consume, Table* ht) const {

        std::vector<size_t>& num_items_mem_per_frame = ht->NumItemsMemPerFrame();

        std::vector<data::File>& frame_files = ht->FrameFiles();

        size_t num_frames = ht->NumFrames();

        std::vector<size_t>& frame_sequence = ht->FrameSequence();

        for (size_t frame_id : frame_sequence) {

            // get the actual reader from the file
            data::File& file = frame_files[frame_id];

            // only if items have been spilled, process a second reduce
            if (file.num_items() > 0) {

                data::File::Reader reader = file.GetReader(consume);

                // this does not work -tb
                abort();
                // Reduce<Table, BucketBlock>(ctx, consume, ht, items, offset,
                //                            length, reader, second_reduce,
                //                            fill_rate_num_items_per_frame,
                //                            frame_id, num_items_mem_per_frame, block_pool,
                //                            max_num_blocks_second_reduce, block_size);

                // no spilled items, just flush already reduced
                // data in primary table in current frame
            }
            else {
                /////
                // emit data
                /////
                ht->FlushPartitionE(
                    frame_id, consume,
                    [&](const size_t& partition_id, const KeyValuePair& bi) {
                        ht->EmitAll(partition_id, bi);
                    });
            }
        }

        // set num blocks for table/items per frame to 0
        if (consume) {
            ht->SetNumBlocksPerTable(0);
            for (size_t frame_id = 0; frame_id < num_frames; frame_id++) {
                num_items_mem_per_frame[frame_id] = 0;
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

#endif // !THRILL_CORE_POST_BUCKET_REDUCE_FLUSH_HEADER

/******************************************************************************/
