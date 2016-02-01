/*******************************************************************************
 * thrill/core/post_probing_reduce_flush.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_CORE_POST_PROBING_REDUCE_FLUSH_HEADER
#define THRILL_CORE_POST_PROBING_REDUCE_FLUSH_HEADER

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
#include <sstream>
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
class PostProbingReduceFlush
{
public:
    PostProbingReduceFlush(ReduceFunction reduce_function,
                           const IndexFunction& index_function = IndexFunction(),
                           const EqualToFunction& equal_to_function = EqualToFunction())
        : reduce_function_(reduce_function),
          index_function_(index_function),
          equal_to_function_(equal_to_function)
    { }

    void Spill(std::vector<KeyValuePair>& second_reduce,
               size_t fr_begin, size_t fr_end,
               data::File::Writer& writer, KeyValuePair& sentinel) const {

        for (size_t idx = fr_begin; idx < fr_end; idx++)
        {
            KeyValuePair& current = second_reduce[idx];
            if (current.first != sentinel.first)
            {
                writer.Put(current);
                second_reduce[idx] = sentinel;
            }
        }
    }

    template <typename Table>
    void Reduce(Context& ctx, bool consume, Table& ht,
                std::vector<KeyValuePair>& items,
                size_t fr_begin, size_t fr_end,
                data::File::Reader& reader,
                std::vector<KeyValuePair>& second_reduce,
                size_t fill_rate_num_items_per_partition,
                size_t partition_id, KeyValuePair& sentinel) const {

        size_t item_count = 0;

        std::vector<data::File> partition_files_;
        std::vector<data::File::Writer> partition_writers_;

        /////
        // reduce data from spilled files
        /////

        // flag used when item is reduced to advance to next item
        bool reduced = false;

        /////
        // reduce data from primary table
        /////
        for (size_t i = fr_begin; i < fr_end; i++)
        {
            KeyValuePair& kv = items[i];
            if (kv.first != sentinel.first)
            {
                typename IndexFunction::IndexResult h = index_function_(
                    kv.first, 1, second_reduce.size(), second_reduce.size(), 0);

                KeyValuePair* initial = &second_reduce[h.global_index];
                KeyValuePair* current = initial;
                KeyValuePair* last_item = &second_reduce[second_reduce.size() - 1];

                while (!equal_to_function_(current->first, sentinel.first))
                {
                    if (equal_to_function_(current->first, kv.first))
                    {
                        current->second = reduce_function_(current->second, kv.second);
                        reduced = true;
                        break;
                    }

                    if (current == last_item)
                    {
                        current -= (second_reduce.size() - 1);
                    }
                    else
                    {
                        ++current;
                    }

                    if (current == initial)
                    {
                        std::cout << "!!!!!!!Overflow!!!!!!!" << std::endl;
                    }
                }

                if (reduced)
                {
                    if (consume)
                    {
                        items[i] = sentinel;
                    }
                    reduced = false;
                    continue;
                }

                // insert new pair
                *current = kv;

                item_count++;

                if (consume)
                    items[i] = sentinel;

                // flush current partition if max partition fill rate reached
                if (item_count > fill_rate_num_items_per_partition)
                {
                    // set up files (if not set up already)
                    if (partition_files_.size() == 0) {
                        for (size_t i = 0; i < 2; i++) {
                            partition_files_.push_back(ctx.GetFile());
                        }
                        for (size_t i = 0; i < 2; i++) {
                            partition_writers_.push_back(partition_files_[i].GetWriter());
                        }
                    }

                    // spill into files
                    Spill(second_reduce,
                          0, second_reduce.size() / 2,
                          partition_writers_[0], sentinel);

                    Spill(second_reduce,
                          second_reduce.size() / 2, second_reduce.size(),
                          partition_writers_[1], sentinel);

                    item_count = 0;
                }
            }
        }

        while (reader.HasNext()) {

            KeyValuePair kv = reader.Next<KeyValuePair>();

            typename IndexFunction::IndexResult h =
                index_function_(kv.first, 1,
                                second_reduce.size(), second_reduce.size(), 0);

            KeyValuePair* initial = &second_reduce[h.global_index];
            KeyValuePair* current = initial;
            KeyValuePair* last_item = &second_reduce[second_reduce.size() - 1];

            while (!equal_to_function_(current->first, sentinel.first))
            {
                if (equal_to_function_(current->first, kv.first))
                {
                    current->second = reduce_function_(current->second, kv.second);
                    reduced = true;
                    break;
                }

                if (current == last_item)
                    current -= (second_reduce.size() - 1);
                else
                    ++current;
            }

            if (reduced)
            {
                reduced = false;
                continue;
            }

            // insert new pair
            *current = kv;

            item_count++;

            // flush current partition if max partition fill rate reached
            if (item_count > fill_rate_num_items_per_partition)
            {
                // set up files (if not set up already)
                if (partition_files_.size() == 0) {
                    for (size_t i = 0; i < 2; i++) {
                        partition_files_.push_back(ctx.GetFile());
                    }
                    for (size_t i = 0; i < 2; i++) {
                        partition_writers_.push_back(partition_files_[i].GetWriter());
                    }
                }

                // spill into files
                Spill(second_reduce,
                      0, second_reduce.size() / 2,
                      partition_writers_[0], sentinel);

                Spill(second_reduce,
                      second_reduce.size() / 2, second_reduce.size(),
                      partition_writers_[1], sentinel);

                item_count = 0;
            }
        }

        /////
        // emit data
        /////

        // nothing spilled in second reduce
        if (partition_files_.size() == 0) {

            for (size_t i = 0; i < second_reduce.size(); i++) {
                KeyValuePair& current = second_reduce[i];
                if (current.first != sentinel.first) {
                    ht.EmitAll(current, partition_id);
                    second_reduce[i] = sentinel;
                }
            }
        }
        // spilling was required, need to reduce again
        else {
            throw std::invalid_argument("recursive spill not working yet");

            // spill into files
            Spill(second_reduce,
                  0, second_reduce.size() / 2,
                  partition_writers_[0], sentinel);

            Spill(second_reduce,
                  second_reduce.size() / 2, second_reduce.size(),
                  partition_writers_[1], sentinel);

            data::File& file0 = partition_files_[0];
            partition_writers_[0].Close();

            data::File::Reader reader0 = file0.GetReader(true);
            Reduce(ctx, false, ht, second_reduce, 0, 0,
                   reader0, second_reduce, fill_rate_num_items_per_partition,
                   partition_id, sentinel);

            data::File& file1 = partition_files_[1];
            partition_writers_[1].Close();

            data::File::Reader reader1 = file1.GetReader(true);
            Reduce(ctx, false, ht, second_reduce, 0, 0,
                   reader1, second_reduce, fill_rate_num_items_per_partition,
                   partition_id, sentinel);
        }
    }

    template <typename Table>
    void FlushTable(bool consume, Table& ht) const {

        std::vector<KeyValuePair>& items = ht.Items();

        std::vector<size_t>& num_items_per_partition = ht.NumItemsPerPartition();

        std::vector<data::File>& partition_files = ht.PartitionFiles();

        std::vector<data::File::Writer>& partition_writers = ht.PartitionWriters();

        size_t partition_size = ht.PartitionSize();

        size_t num_partitions = ht.NumPartitions();

        KeyValuePair sentinel = ht.Sentinel();

        std::vector<size_t>& partition_sequence = ht.PartitionSequence();

        for (size_t partition_id : partition_sequence)
        {
            // get the actual reader from the file
            data::File& file = partition_files[partition_id];
            partition_writers[partition_id].Close();

            // compute partition offset of current partition
            size_t fr_begin = partition_id * partition_size;
            size_t fr_end = (partition_id + 1) * partition_size;

            // only if items have been spilled, process a second reduce
            if (file.num_items() > 0)
            {
                data::File::Reader reader = file.GetReader(consume);

                // this does not work -tb
                abort();

                // Reduce(ctx, consume, ht, items, fr_begin, fr_end, reader, second_reduce,
                //        fill_rate_num_items_per_partition, partition_id, sentinel);

                // no spilled items, just flush already reduced
                // data in primary table in current partition
            }
            else
            {
                /////
                // emit data
                /////
                for (size_t i = fr_begin; i < fr_end; i++)
                {
                    KeyValuePair& current = items[i];
                    if (current.first != sentinel.first)
                    {
                        ht.EmitAll(current, partition_id);

                        if (consume)
                        {
                            items[i] = sentinel;
                        }
                    }
                }
            }
        }

        // set num items per partition to 0
        if (consume) {
            for (size_t partition_id = 0; partition_id < num_partitions; partition_id++) {
                num_items_per_partition[partition_id] = 0;
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

#endif // !THRILL_CORE_POST_PROBING_REDUCE_FLUSH_HEADER

/******************************************************************************/
