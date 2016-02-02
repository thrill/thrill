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

        std::vector<size_t>& num_items_per_frame = ht.NumItemsPerFrame();

        std::vector<data::File>& frame_files = ht.FrameFiles();

        std::vector<data::File::Writer>& frame_writers = ht.FrameWriters();

        size_t frame_size = ht.FrameSize();

        size_t num_frames = ht.NumFrames();

        size_t fill_rate_num_items_per_frame = ht.FillRateNumItemsSecondReduce();

        Value neutral_element = ht.NeutralElement();

        std::vector<Value> elements_to_emit(ht.LocalIndex().size(), neutral_element);

        Context& ctx = ht.Ctx();

        KeyValuePair sentinel = ht.Sentinel();

        for (size_t frame_id = 0; frame_id < num_frames; frame_id++) {
            // get the actual reader from the file
            data::File& file = frame_files[frame_id];
            data::File::Writer& writer = frame_writers[frame_id];
            writer.Close(); // also closes the file

            // compute frame offset of current frame
            size_t offset = frame_id * frame_size;
            size_t length = offset + frame_size;

            // only if items have been spilled, process a second reduce
            if (file.num_items() > 0) {
                data::File::Reader reader = file.GetReader(consume);

                Reduce(ctx, consume, ht, items, offset, length, reader, second_reduce, elements_to_emit,
                       fill_rate_num_items_per_frame, frame_id, num_items_per_frame, sentinel, ht.LocalIndex().begin);

                // no spilled items, just flush already reduced
                // data in primary table in current frame
            }
            else {
                /////
                // emit data
                /////
                for (size_t i = offset; i < length; i++) {
                    KeyValuePair& current = items[i];
                    if (current.first != sentinel.first) {
                        elements_to_emit[current.first - ht.LocalIndex().begin] = current.second;

                        if (consume) {
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

        size_t index = ht.LocalIndex().begin;
        for (size_t i = 0; i < elements_to_emit.size(); i++) {
            ht.EmitAll(std::make_pair(index++, elements_to_emit[i]), 0);
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
