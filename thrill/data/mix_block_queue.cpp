/*******************************************************************************
 * thrill/data/mix_block_queue.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/mix_block_queue.hpp>
#include <thrill/data/mix_stream.hpp>

#include <vector>

namespace thrill {
namespace data {

/******************************************************************************/
// MixBlockQueue

MixBlockQueue::MixBlockQueue(BlockPool& block_pool, size_t num_workers,
                             size_t local_worker_id, size_t dia_id)
    : block_pool_(block_pool),
      local_worker_id_(local_worker_id),
      num_workers_(num_workers),
      write_closed_(num_workers) {
    queues_.reserve(num_workers);
    for (size_t w = 0; w < num_workers; ++w) {
        queues_.emplace_back(block_pool_, local_worker_id, dia_id);
    }
}

void MixBlockQueue::set_dia_id(size_t dia_id) {
    for (size_t i = 0; i < queues_.size(); ++i) {
        queues_[i].set_dia_id(dia_id);
    }
}

void MixBlockQueue::AppendBlock(size_t src, const Block& block) {
    LOG << "MixBlockQueue::AppendBlock"
        << " src=" << src << " block=" << block;
    mix_queue_.enqueue(SrcBlockPair { src, block });
}

void MixBlockQueue::AppendBlock(size_t src, Block&& block) {
    LOG << "MixBlockQueue::AppendBlock"
        << " src=" << src << " block=" << block;
    mix_queue_.enqueue(SrcBlockPair { src, std::move(block) });
}

void MixBlockQueue::Close(size_t src) {
    LOG << "MixBlockQueue::Close()"
        << " src=" << src
        << " local_worker_id_=" << local_worker_id_
        << " --write_open_count_=" << write_open_count_ - 1;
    assert(!write_closed_[src]);
    write_closed_[src] = true;
    --write_open_count_;

    // enqueue a closing Block.
    mix_queue_.enqueue(SrcBlockPair { src, Block() });
}

MixBlockQueue::SrcBlockPair MixBlockQueue::Pop() {
    if (read_open_ == 0)
        return SrcBlockPair {
                   size_t(-1), Block()
        };
    SrcBlockPair b;
    mix_queue_.wait_dequeue(b);
    if (!b.block.IsValid()) {
        LOG << "MixBlockQueue()"
            << " read_open_ " << read_open_ << " -> " << read_open_ - 1;
        --read_open_;
    }
    return b;
}

/******************************************************************************/
// MixBlockQueueSink

MixBlockQueueSink::MixBlockQueueSink(
    MixStream& dst_mix_stream, size_t from_global, size_t from_local)
    : BlockSink(dst_mix_stream.queue_.block_pool(), from_local),
      dst_mix_stream_(dst_mix_stream), dst_mix_queue_(dst_mix_stream.queue_),
      from_global_(from_global)
{ }

void MixBlockQueueSink::AppendBlock(const Block& b, bool /* is_last_block */) {
    LOG << "MixBlockQueueSink::AppendBlock()"
        << " from_global_=" << from_global_ << " b=" << b;
    item_counter_ += b.num_items();
    byte_counter_ += b.size();
    ++block_counter_;
    dst_mix_queue_.AppendBlock(from_global_, b);
}

void MixBlockQueueSink::AppendBlock(Block&& b, bool /* is_last_block */) {
    LOG << "MixBlockQueueSink::AppendBlock()"
        << " from_global_=" << from_global_ << " b=" << b;
    item_counter_ += b.num_items();
    byte_counter_ += b.size();
    ++block_counter_;
    dst_mix_queue_.AppendBlock(from_global_, std::move(b));
}

void MixBlockQueueSink::Close() {
    // enqueue a closing Block.
    LOG << "MixBlockQueueSink::Close()"
        << " from_global_=" << from_global_;
    ++block_counter_;
    dst_mix_queue_.Close(from_global_);
    write_closed_ = true;

    logger()
        << "class" << "StreamSink"
        << "subclass" << "MixBlockQueueSink"
        << "event" << "close"
        << "id" << dst_mix_stream_.id_
        << "peer_host" << dst_mix_stream_.my_host_rank()
        << "src_worker" << from_global_
        << "tgt_worker" << dst_mix_stream_.my_worker_rank()
        << "loopback" << true
        << "items" << item_counter_
        << "bytes" << byte_counter_
        << "blocks" << block_counter_;

    if (src_mix_stream_) {
        src_mix_stream_->tx_int_items_ += item_counter_;
        src_mix_stream_->tx_int_bytes_ += byte_counter_;
        src_mix_stream_->tx_int_blocks_ += block_counter_;
    }

    dst_mix_stream_.rx_int_items_ += item_counter_;
    dst_mix_stream_.rx_int_bytes_ += byte_counter_;
    dst_mix_stream_.rx_int_blocks_ += block_counter_;
}

void MixBlockQueueSink::set_src_mix_stream(MixStream* src_mix_stream) {
    src_mix_stream_ = src_mix_stream;
}

/******************************************************************************/
// MixBlockQueueReader

MixBlockQueueReader::MixBlockQueueReader(
    MixBlockQueue& mix_queue, bool consume, size_t local_worker_id)
    : mix_queue_(mix_queue),
      reread_(mix_queue.read_closed()) {

    if (!reread_) {
        readers_.reserve(mix_queue_.num_workers_);
        available_at_.resize(mix_queue_.num_workers_, 0);

        for (size_t w = 0; w < mix_queue_.num_workers_; ++w) {
            readers_.emplace_back(
                mix_queue_.queues_[w].GetReader(consume, local_worker_id));
        }
    }
    else {
        // construct vector of BlockSources to read from queues_.
        std::vector<DynBlockSource> result;
        for (size_t w = 0; w < mix_queue_.num_workers_; ++w) {
            result.emplace_back(mix_queue_.queues_[w].GetBlockSource(
                                    consume, local_worker_id));
        }
        // move BlockQueueSources into concatenation BlockSource, and to Reader.
        cat_reader_ = CatBlockReader(CatBlockSource(std::move(result)));
    }
}

MixBlockQueueReader::~MixBlockQueueReader() {
    // TODO(tb)
}

bool MixBlockQueueReader::PullBlock() {
    // no full item available: get next block from mix queue
    while (available_ == 0) {

        MixBlockQueue::SrcBlockPair src_blk = mix_queue_.Pop();
        LOG << "MixBlockQueueReader::PullBlock()"
            << " still open_=" << open_
            << " src=" << src_blk.src << " block=" << src_blk.block
            << " selected_=" << selected_
            << " available_=" << available_
            << " available_at_[src]=" << available_at_[src_blk.src];

        assert(src_blk.src < readers_.size());

        if (src_blk.block.IsValid()) {
            // block for this reader.
            selected_ = src_blk.src;

            size_t num_items = src_blk.block.num_items();

            // save block with data for reader
            mix_queue_.queues_[src_blk.src].AppendBlock(
                std::move(src_blk.block), /* is_last_block */ false);

            // add available items: one less than in the blocks.
            available_at_[src_blk.src] += num_items;
            available_ = available_at_[src_blk.src] - 1;
            available_at_[src_blk.src] -= available_;
        }
        else {
            // close block received: maybe get last item
            assert(open_ > 0);
            --open_;

            // save block with data for reader
            mix_queue_.queues_[src_blk.src].AppendBlock(
                std::move(src_blk.block), /* is_last_block */ false);

            // check if we can still read the last item
            if (available_at_[src_blk.src]) {
                assert(available_at_[src_blk.src] == 1);
                selected_ = src_blk.src;
                available_ = available_at_[src_blk.src];
                available_at_[src_blk.src] -= available_;
            }
            else if (open_ == 0) return false;
        }

        LOG << "MixBlockQueueReader::PullBlock() afterwards"
            << " selected_=" << selected_
            << " available_=" << available_
            << " available_at_[src]=" << available_at_[src_blk.src];
    }
    return true;
}

} // namespace data
} // namespace thrill

/******************************************************************************/
