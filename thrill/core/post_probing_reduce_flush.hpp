/*******************************************************************************
 * thrill/core/post_probing_reduce_flush.hpp
 *
 * Part of Project Thrill.
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
#include <sstream>

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
    void Spill(std::vector<KeyValuePair>& second_reduce, size_t offset,
               size_t length, data::File::Writer& writer,
               KeyValuePair& sentinel) const
    {
        for (size_t idx = offset; idx < length; idx++)
        {
            KeyValuePair& current = second_reduce[idx];
            if (current.first != sentinel.first)
            {
                writer.PutItem(current);
                second_reduce[idx] = sentinel;
            }
        }
    }

    template <typename Table>
    void Reduce(Context& ctx, bool consume, Table* ht,
                std::vector<KeyValuePair>& items, size_t offset, size_t length,
                data::File::Reader& reader, std::vector<KeyValuePair>& second_reduce,
                size_t fill_rate_num_items_per_frame, size_t frame_id, KeyValuePair& sentinel) const
    {
        size_t item_count = 0;

        std::vector<data::File> frame_files_;
        std::vector<data::File::Writer> frame_writers_;

        /////
        // reduce data from spilled files
        /////

        // flag used when item is reduced to advance to next item
        bool reduced = false;

        /////
        // reduce data from primary table
        /////
        for (size_t i = offset; i < length; i++)
        {
            KeyValuePair& kv = items[i];
            if (kv.first != sentinel.first)
            {
                typename IndexFunction::IndexResult h = index_function_(kv.first, 1, second_reduce.size(), second_reduce.size(), 0);

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
                //current->first = kv.first;
                //current->second = kv.second;

                item_count++;

                if (consume)
                {
                    items[i] = sentinel;
                }

                // flush current partition if max partition fill rate reached
                if (item_count > fill_rate_num_items_per_frame)
                {
                    // set up files (if not set up already)
                    if (frame_files_.size() == 0) {
                        for (size_t i = 0; i < 2; i++) {
                            frame_files_.push_back(ctx.GetFile());
                        }
                        for (size_t i = 0; i < 2; i++) {
                            frame_writers_.push_back(frame_files_[i].GetWriter());
                        }
                    }

                    // spill into files
                    Spill(second_reduce, 0, second_reduce.size() / 2, frame_writers_[0], sentinel);
                    Spill(second_reduce, second_reduce.size() / 2, second_reduce.size(), frame_writers_[1], sentinel);

                    item_count = 0;
                }
            }
        }

        while (reader.HasNext()) {

            KeyValuePair kv = reader.Next<KeyValuePair>();

            typename IndexFunction::IndexResult h = index_function_(kv.first, 1, second_reduce.size(), second_reduce.size(), 0);

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
            }

            if (reduced)
            {
                reduced = false;
                continue;
            }

            // insert new pair
            *current = kv;
            //current->first = kv.first;
            //current->second = kv.second;

            item_count++;

            // flush current partition if max partition fill rate reached
            if (item_count > fill_rate_num_items_per_frame)
            {
                // set up files (if not set up already)
                if (frame_files_.size() == 0) {
                    for (size_t i = 0; i < 2; i++) {
                        frame_files_.push_back(ctx.GetFile());
                    }
                    for (size_t i = 0; i < 2; i++) {
                        frame_writers_.push_back(frame_files_[i].GetWriter());
                    }
                }

                // spill into files
                Spill(second_reduce, 0, second_reduce.size() / 2, frame_writers_[0], sentinel);
                Spill(second_reduce, second_reduce.size() / 2, second_reduce.size(), frame_writers_[1], sentinel);

                item_count = 0;
            }
        }

        /////
        // emit data
        /////

        // nothing spilled in second reduce
        if (frame_files_.size() == 0) {

            for (size_t i = 0; i < second_reduce.size(); i++) {
                KeyValuePair &current = second_reduce[i];
                if (current.first != sentinel.first) {
                    ht->EmitAll(current, frame_id);
                    second_reduce[i].first = sentinel.first;
                    second_reduce[i].second = sentinel.second;
                }
            }
        }

        // spilling was required, need to reduce again
        else {

            // spill into files
            Spill(second_reduce, 0, second_reduce.size() / 2, frame_writers_[0], sentinel);
            Spill(second_reduce, second_reduce.size() / 2, second_reduce.size(), frame_writers_[1], sentinel);

            data::File& file1 = frame_files_[0];
            data::File::Writer& writer1 = frame_writers_[0];
            writer1.Close();
            data::File::Reader reader1 = file1.GetReader(true);
            Reduce(ctx, false, ht, second_reduce, 0, 0, reader1, second_reduce, fill_rate_num_items_per_frame, frame_id, sentinel);

            data::File& file2 = frame_files_[1];
            data::File::Writer& writer2 = frame_writers_[1];
            writer2.Close();
            data::File::Reader reader2 = file2.GetReader(true);
            Reduce(ctx, false, ht, second_reduce, 0, 0, reader2, second_reduce, fill_rate_num_items_per_frame, frame_id, sentinel);
        }
    }

    template <typename Table>
    void
    operator () (bool consume, Table* ht) const {

        std::vector<KeyValuePair>& items = ht->Items();

        std::vector<KeyValuePair>& second_reduce = ht->SecondTable();

        std::vector<size_t>& num_items_per_frame = ht->NumItemsPerFrame();

        std::vector<data::File>& frame_files = ht->FrameFiles();

        std::vector<data::File::Writer>& frame_writers = ht->FrameWriters();

        size_t frame_size = ht->FrameSize();

        size_t fill_rate_num_items_per_frame = ht->FillRateNumItemsSecondReduce();

        size_t num_frames = ht->NumFrames();

        Context& ctx = ht->Ctx();

        KeyValuePair sentinel = ht->Sentinel();

        std::vector<size_t>& frame_sequence = ht->FrameSequence();

        for (size_t frame_id : frame_sequence)
        {
            // get the actual reader from the file
            data::File& file = frame_files[frame_id];
            data::File::Writer& writer = frame_writers[frame_id];
            writer.Close(); // also closes the file

            // compute frame offset of current frame
            size_t offset = frame_id * frame_size;
            size_t length = (frame_id + 1) * frame_size;

            // only if items have been spilled, process a second reduce
            if (file.num_items() > 0)
            {
                data::File::Reader reader = file.GetReader(consume);

                Reduce(ctx, consume, ht, items, offset, length, reader, second_reduce,
                       fill_rate_num_items_per_frame, frame_id, sentinel);

            // no spilled items, just flush already reduced
            // data in primary table in current frame
            }
            else
            {
                /////
                // emit data
                /////
                for (size_t i = offset; i < length; i++)
                {
                    KeyValuePair& current = items[i];
                    if (current.first != sentinel.first)
                    {
                        ht->EmitAll(current, frame_id);

                        if (consume)
                        {
                            items[i] = sentinel;
                        }
                    }
                }
            }
        }

        // set num items per frame to 0
        if (consume) {
            for (size_t frame_id = 0; frame_id < num_frames; frame_id++) {
                num_items_per_frame[frame_id] = 0;
            }
        }
    }

private:
    ReduceFunction reduce_function_;
    IndexFunction index_function_;
    EqualToFunction equal_to_function_;
};

}
}

#endif //THRILL_CORE_POST_PROBING_REDUCE_FLUSH_HEADER
