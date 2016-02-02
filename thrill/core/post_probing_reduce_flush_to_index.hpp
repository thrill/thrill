/*******************************************************************************
 * thrill/core/post_probing_reduce_flush_to_index.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_POST_PROBING_REDUCE_FLUSH_TO_INDEX_HEADER
#define THRILL_CORE_POST_PROBING_REDUCE_FLUSH_TO_INDEX_HEADER

#include <thrill/api/context.hpp>
#include <thrill/common/function_traits.hpp>
#include <thrill/common/logger.hpp>
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
class PostProbingReduceByHashKey;

template <typename Key,
          typename Value,
          typename ReduceFunction,
          typename IndexFunction = PostProbingReduceByHashKey<Key, std::hash<Key> >,
          typename EqualToFunction = std::equal_to<Key>,
          typename KeyValuePair = std::pair<Key, Value> >
class PostProbingReduceFlushToIndex
{

public:
    PostProbingReduceFlushToIndex(ReduceFunction reduce_function,
                                  const IndexFunction& index_function = IndexFunction(),
                                  const EqualToFunction& equal_to_function = EqualToFunction())
        : reduce_function_(reduce_function),
          index_function_(index_function),
          equal_to_function_(equal_to_function) { }

    template <typename Table>
    void FlushTable(bool consume, Table& ht) const {

        std::vector<KeyValuePair>& items = ht.Items();

        std::vector<KeyValuePair>& second_reduce = ht.SecondTable();

        std::vector<size_t>& num_items_per_partition = ht.NumItemsPerPartition();

        std::vector<data::File>& partition_files = ht.PartitionFiles();

        std::vector<data::File::Writer>& partition_writers = ht.PartitionWriters();

        size_t partition_size = ht.PartitionSize();

        size_t num_partitions = ht.NumPartitions();

        size_t fill_rate_num_items_per_partition = ht.FillRateNumItemsSecondReduce();

        Value neutral_element = ht.NeutralElement();

        std::vector<Value> elements_to_emit(ht.LocalIndex().size(), neutral_element);

        Context& ctx = ht.Ctx();

        KeyValuePair sentinel = ht.Sentinel();

        for (size_t partition_id = 0; partition_id < num_partitions; partition_id++) {
            // get the actual reader from the file
            data::File& file = partition_files[partition_id];
            data::File::Writer& writer = partition_writers[partition_id];
            writer.Close(); // also closes the file

            // compute partition offset of current partition
            size_t offset = partition_id * partition_size;
            size_t length = offset + partition_size;

            // only if items have been spilled, process a second reduce
            if (file.num_items() > 0) {
                data::File::Reader reader = file.GetReader(consume);

                Reduce(ctx, consume, ht, items, offset, length, reader, second_reduce, elements_to_emit,
                       fill_rate_num_items_per_partition, partition_id, num_items_per_partition, sentinel, ht.LocalIndex().begin);

                // no spilled items, just flush already reduced
                // data in primary table in current partition
            }
            else {
                /////
                // emit data
                /////

                ht.FlushPartitionE(
                    partition_id, consume,
                    [&](const size_t& /* partition_id */, const KeyValuePair& p) {
                        elements_to_emit[p.first - ht.LocalIndex().begin] = p.second;
                    });
            }
        }

        // set num items per partition to 0
        if (consume) {
            for (size_t partition_id = 0; partition_id < num_partitions; partition_id++) {
                num_items_per_partition[partition_id] = 0;
            }
        }

        size_t index = ht.LocalIndex().begin;
        for (size_t i = 0; i < elements_to_emit.size(); i++) {
            ht.EmitAll(0, std::make_pair(index++, elements_to_emit[i]));
            elements_to_emit[i] = neutral_element;
        }

        assert(index == ht.LocalIndex().end);
    }

private:
    ReduceFunction reduce_function_;
    IndexFunction index_function_;
    EqualToFunction equal_to_function_;
};

} // namespace core
} // namespace thrill

#endif // !THRILL_CORE_POST_PROBING_REDUCE_FLUSH_TO_INDEX_HEADER

/******************************************************************************/
