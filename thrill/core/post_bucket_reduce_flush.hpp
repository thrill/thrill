/*******************************************************************************
 * thrill/core/post_bucket_reduce_flush.hpp
 *
 * Part of Project Thrill.
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
#include <thrill/data/block_pool.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/file.hpp>
#include <thrill/core/post_bucket_reduce_by_hash_key.hpp>
#include <thrill/core/bucket_block_pool.hpp>

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

template<typename Key,
        typename Value,
        typename ReduceFunction,
        typename IndexFunction = core::PostBucketReduceByHashKey<Key>,
        typename EqualToFunction = std::equal_to<Key>,
        typename KeyValuePair = std::pair<Key, Value> >
class PostBucketReduceFlush {
    static const bool emit = true;

public:
    PostBucketReduceFlush(ReduceFunction reduce_function,
                    const IndexFunction &index_function = IndexFunction(),
                    const EqualToFunction &equal_to_function = EqualToFunction())
            : reduce_function_(reduce_function),
              index_function_(index_function),
              equal_to_function_(equal_to_function) { }

    template<typename Table, typename BucketBlock>
    void Spill(std::vector<BucketBlock *> &second_reduce, size_t offset,
               size_t length, data::File::Writer &writer, BucketBlockPool <BucketBlock> &block_pool) const {
        for (size_t idx = offset; idx < length; idx++) {
            BucketBlock *current = second_reduce[idx];

            while (current != nullptr) {
                for (KeyValuePair *bi = current->items;
                     bi != current->items + current->size; ++bi) {
                    writer.PutItem(*bi);
                }

                // destroy block and advance to next
                BucketBlock *next = current->next;
                block_pool.Deallocate(current);
                current = next;
            }

            second_reduce[idx] = nullptr;
        }
    }

    template<typename Table, typename BucketBlock>
    void Reduce(Context &ctx, bool consume, Table *ht,
                std::vector<BucketBlock *> &items, size_t offset, size_t length,
                data::File::Reader &reader, std::vector<BucketBlock *> &second_reduce,
                size_t fill_rate_num_items_per_frame,
                size_t frame_id, std::vector <size_t> &num_items_per_frame,
                BucketBlockPool <BucketBlock> &block_pool,
                size_t max_num_blocks_second_reduce, size_t block_size) const {
        size_t item_count = 0;

        std::vector <data::File> frame_files_;
        std::vector <data::File::Writer> frame_writers_;

        /////
        // reduce data from spilled files
        /////

        // flag used when item is reduced to advance to next item
        bool reduced = false;

        size_t blocks_secondary_used = 0;

        /////
        // reduce data from primary table
        /////
        for (size_t i = offset; i < length; i++) {
            BucketBlock *current = items[i];

            while (current != nullptr) {
                for (KeyValuePair *from = current->items;
                     from != current->items + current->size; ++from) {
                    // insert in second reduce table
                    size_t global_index = index_function_(from->first, ht, second_reduce.size());
                    BucketBlock *current_second = second_reduce[global_index];
                    while (current_second != nullptr) {
                        // iterate over valid items in a block
                        for (KeyValuePair *bi = current_second->items;
                             bi != current_second->items + current_second->size; ++bi) {
                            // if item and key equals, then reduce.
                            if (equal_to_function_(from->first, bi->first)) {
                                bi->second = reduce_function_(bi->second, from->second);
                                reduced = true;
                                break;
                            }
                        }

                        if (reduced) {
                            break;
                        }

                        current_second = current_second->next;
                    }

                    if (reduced) {
                        reduced = false;
                        continue;
                    }

                    current_second = second_reduce[global_index];

                    //////
                    // have an item that needs to be added.
                    //////

                    if (current_second == nullptr ||
                        current_second->size == block_size) {

                        // spill largest frame if max number of blocks reached
                        if (blocks_secondary_used == max_num_blocks_second_reduce) {
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
                            Spill<Table, BucketBlock>(second_reduce, 0, second_reduce.size() / 2,
                                                                frame_writers_[0], block_pool);
                            Spill<Table, BucketBlock>(second_reduce, second_reduce.size() / 2,
                                                                second_reduce.size(), frame_writers_[1],
                                                                block_pool);

                            blocks_secondary_used = 0;
                        }

                        // allocate a new block of uninitialized items, postpend to bucket
                        current_second = block_pool.GetBlock();
                        blocks_secondary_used++;
                        current_second->next = second_reduce[global_index];
                        second_reduce[global_index] = current_second;
                    }

                    // in-place construct/insert new item in current bucket block
                    new(current_second->items + current_second->size++)KeyValuePair(from->first,
                                                                                    std::move(from->second));

                    item_count++;
                }

                // advance to next
                if (consume) {
                    BucketBlock *next = current->next;
                    block_pool.Deallocate(current);
                    current = next;
                }
                else {
                    current = current->next;
                }
            }

            if (consume) {
                items[i] = nullptr;
            }

            // flush current partition if max partition fill rate reached
            if (item_count > fill_rate_num_items_per_frame) {
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
                Spill<Table, BucketBlock>(second_reduce, 0, second_reduce.size() / 2,
                                                    frame_writers_[0], block_pool);
                Spill<Table, BucketBlock>(second_reduce, second_reduce.size() / 2,
                                                    second_reduce.size(), frame_writers_[1], block_pool);

                item_count = 0;
            }
        }

        // get the items and insert them in secondary
        // table
        while (reader.HasNext()) {

            KeyValuePair kv = reader.Next<KeyValuePair>();

            size_t global_index = index_function_(kv.first, ht, second_reduce.size());

            BucketBlock *current = second_reduce[global_index];
            while (current != nullptr) {
                // iterate over valid items in a block
                for (KeyValuePair *bi = current->items;
                     bi != current->items + current->size; ++bi) {
                    // if item and key equals, then reduce.
                    if (equal_to_function_(kv.first, bi->first)) {
                        bi->second = reduce_function_(bi->second, kv.second);
                        reduced = true;
                        break;
                    }
                }

                if (reduced) {
                    break;
                }

                current = current->next;
            }

            if (reduced) {
                reduced = false;
                continue;
            }

            current = second_reduce[global_index];

            // have an item that needs to be added.
            if (current == nullptr ||
                current->size == block_size) {

                // spill largest frame if max number of blocks reached
                if (blocks_secondary_used == max_num_blocks_second_reduce) {
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
                    Spill<Table, BucketBlock>(second_reduce, 0, second_reduce.size() / 2,
                                                        frame_writers_[0], block_pool);
                    Spill<Table, BucketBlock>(second_reduce, second_reduce.size() / 2,
                                                        second_reduce.size(), frame_writers_[1], block_pool);

                    blocks_secondary_used = 0;
                }

                // allocate a new block of uninitialized items, postpend to bucket
                current = block_pool.GetBlock();
                blocks_secondary_used++;
                current->next = second_reduce[global_index];
                second_reduce[global_index] = current;
            }

            // in-place construct/insert new item in current bucket block
            new(current->items + current->size++)KeyValuePair(kv.first, std::move(kv.second));

            item_count++;

            // flush current partition if max partition fill rate reached
            if (item_count > fill_rate_num_items_per_frame) {
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
                Spill<Table, BucketBlock>(second_reduce, 0, second_reduce.size() / 2,
                                                    frame_writers_[0], block_pool);
                Spill<Table, BucketBlock>(second_reduce, second_reduce.size() / 2,
                                                    second_reduce.size(), frame_writers_[1], block_pool);

                item_count = 0;
            }
        }

        /////
        // emit data
        /////

        // nothing spilled in second reduce
        if (frame_files_.size() == 0) {
            for (size_t i = 0; i < second_reduce.size(); i++) {
                BucketBlock *current = second_reduce[i];

                while (current != nullptr) {
                    for (KeyValuePair *bi = current->items;
                         bi != current->items + current->size; ++bi) {
                        if (emit) {
                            ht->EmitAll(bi->first, bi->second);
                        }
                    }

                    // destroy block and advance to next
                    BucketBlock *next = current->next;
                    block_pool.Deallocate(current);
                    current = next;
                }

                second_reduce[i] = nullptr;
            }
        }

            // spilling was required, need to reduce again
        else {
            // spill into files
            Spill<Table, BucketBlock>(second_reduce, 0, second_reduce.size() / 2,
                                                frame_writers_[0], block_pool);
            Spill<Table, BucketBlock>(second_reduce, second_reduce.size() / 2,
                                                second_reduce.size(), frame_writers_[1], block_pool);

            data::File &file1 = frame_files_[0];
            data::File::Writer &writer1 = frame_writers_[0];
            writer1.Close();
            data::File::Reader reader1 = file1.GetReader(true);
            Reduce(ctx, false, ht, second_reduce, 0, 0, reader1, second_reduce,
                   fill_rate_num_items_per_frame, frame_id, num_items_per_frame,
                   block_pool, max_num_blocks_second_reduce, block_size);

            data::File &file2 = frame_files_[1];
            data::File::Writer &writer2 = frame_writers_[1];
            writer2.Close();
            data::File::Reader reader2 = file2.GetReader(true);
            Reduce(ctx, false, ht, second_reduce, 0, 0, reader2, second_reduce,
                   fill_rate_num_items_per_frame, frame_id, num_items_per_frame,
                   block_pool, max_num_blocks_second_reduce, block_size);
        }
    }

    template<typename Table>
    void
    operator()(bool consume, Table *ht) const {

        using BucketBlock = typename Table::BucketBlock;

        std::vector <BucketBlock *>& items = ht->Items();

        std::vector <BucketBlock *>& second_reduce = ht->SecondTable();

        std::vector <size_t> &num_items_mem_per_frame = ht->NumItemsMemPerFrame();

        std::vector <data::File> &frame_files = ht->FrameFiles();

        std::vector <data::File::Writer> &frame_writers = ht->FrameWriters();

        size_t num_buckets_per_frame = ht->NumBucketsPerFrame();

        size_t num_frames = ht->NumFrames();

        size_t fill_rate_num_items_per_frame = ht->MaxNumItemsSecondReduce();

        size_t max_num_blocks_second_reduce = ht->MaxNumBlocksSecondReduce();

        BucketBlockPool <BucketBlock> &block_pool = ht->BlockPool();

        Context &ctx = ht->Ctx();

        size_t block_size = ht->BlockSize();

        for (size_t frame_id = 0; frame_id < num_frames; frame_id++) {
            // get the actual reader from the file
            data::File &file = frame_files[frame_id];
            data::File::Writer &writer = frame_writers[frame_id];
            writer.Close(); // also closes the file

            // compute frame offset of current frame
            size_t offset = frame_id * num_buckets_per_frame;
            size_t length = offset + num_buckets_per_frame;

            // only if items have been spilled, process a second reduce
            if (file.num_items() > 0) {

                data::File::Reader reader = file.GetReader(consume);

                Reduce<Table, BucketBlock>(ctx, consume, ht, items, offset,
                                                     length, reader, second_reduce,
                                                     fill_rate_num_items_per_frame,
                                                     frame_id, num_items_mem_per_frame, block_pool,
                                                     max_num_blocks_second_reduce, block_size);

                // no spilled items, just flush already reduced
                // data in primary table in current frame
            }
            else {
                /////
                // emit data
                /////
                for (size_t i = offset; i < length; i++) {
                    BucketBlock *current = items[i];

                    while (current != nullptr) {
                        for (KeyValuePair *bi = current->items;
                             bi != current->items + current->size; ++bi) {
                            if (emit) {
                                ht->EmitAll(bi->first, bi->second);
                            }
                        }

                        // advance to next
                        if (consume) {
                            BucketBlock *next = current->next;
                            block_pool.Deallocate(current);
                            current = next;
                        }
                        else {
                            current = current->next;
                        }
                    }

                    if (consume) {
                        items[i] = nullptr;
                    }
                }
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

}
}

#endif //THRILL_CORE_POST_BUCKET_REDUCE_FLUSH_HEADER
