/*******************************************************************************
 * thrill/data/block_queue.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/data/block_queue.hpp>

namespace thrill {
namespace data {

/******************************************************************************/
// BlockQueue

BlockQueue::BlockQueue(BlockPool& block_pool, size_t local_worker_id,
                       size_t dia_id,
                       const CloseCallback& close_callback)
    : BlockSink(block_pool, local_worker_id),
      file_(block_pool, local_worker_id, dia_id),
      close_callback_(close_callback) {
    assert(local_worker_id < block_pool.workers_per_host());
}

void BlockQueue::Close() {
    assert(!write_closed_);
    write_closed_ = true;

    block_counter_++;

    // enqueue a closing Block.
    queue_.enqueue(Block());

    if (close_callback_) close_callback_(*this);
}

BlockQueue::ConsumeReader BlockQueue::GetConsumeReader(size_t local_worker_id) {
    assert(!read_closed_);
    return ConsumeReader(ConsumeBlockQueueSource(*this, local_worker_id));
}

DynBlockSource BlockQueue::GetBlockSource(bool consume, size_t local_worker_id) {
    if (consume && !read_closed_) {
        // set to consume, and BlockQueue has not been read.
        sLOG << "BlockQueue::GetBlockSource() consume, from queue.";
        return ConstructDynBlockSource<ConsumeBlockQueueSource>(
            *this, local_worker_id);
    }
    else if (consume && read_closed_) {
        // consume the File, BlockQueue was already read.
        sLOG << "BlockQueue::GetBlockSource() consume, from cache:"
             << file_.num_items();
        return ConstructDynBlockSource<ConsumeFileBlockSource>(
            &file_, local_worker_id);
    }
    else if (!consume && !read_closed_) {
        // non-consumer but the BlockQueue has not been read.
        sLOG << "BlockQueue::GetBlockSource() non-consume, from queue.";
        return ConstructDynBlockSource<CacheBlockQueueSource>(
            this, local_worker_id);
    }
    else if (!consume && read_closed_) {
        // non-consumer: reread the file that was cached.
        sLOG << "BlockQueue::GetBlockSource() non-consume, from cache:"
             << file_.num_items();
        return ConstructDynBlockSource<KeepFileBlockSource>(
            file_, local_worker_id);
    }
    else {
        // impossible
        abort();
    }
}

BlockQueue::Reader BlockQueue::GetReader(bool consume, size_t local_worker_id) {
    return DynBlockReader(GetBlockSource(consume, local_worker_id));
}

/******************************************************************************/
// ConsumeBlockQueueSource

ConsumeBlockQueueSource::ConsumeBlockQueueSource(
    BlockQueue& queue, size_t local_worker_id)
    : queue_(queue), local_worker_id_(local_worker_id) { }

void ConsumeBlockQueueSource::Prefetch(size_t /* prefetch */) {
    // not supported yet. TODO(tb)
}

PinnedBlock ConsumeBlockQueueSource::NextBlock() {
    Block b = queue_.Pop();
    LOG << "ConsumeBlockQueueSource::NextBlock() " << b;

    if (!b.IsValid()) return PinnedBlock();
    return b.PinWait(local_worker_id_);
}

/******************************************************************************/
// CacheBlockQueueSource

CacheBlockQueueSource::CacheBlockQueueSource(BlockQueue* queue, size_t local_worker_id)
    : queue_(queue), local_worker_id_(local_worker_id) { }

//! move-constructor: default
CacheBlockQueueSource::CacheBlockQueueSource(CacheBlockQueueSource&& s)
    : queue_(s.queue_), local_worker_id_(s.local_worker_id_) {
    s.queue_ = nullptr;
}

void CacheBlockQueueSource::Prefetch(size_t /* prefetch */) {
    // not supported yet. TODO(tb)
}

PinnedBlock CacheBlockQueueSource::NextBlock() {
    LOG << "CacheBlockQueueSource[" << this << "]::NextBlock() closed " << queue_->read_closed();
    Block b = queue_->Pop();
    LOG << "CacheBlockQueueSource[" << this << "]::NextBlock() " << b;

    // cache block in file_ (but not the termination block from the queue)
    if (b.IsValid())
        queue_->file_.AppendBlock(b);

    if (!b.IsValid())
        return PinnedBlock();

    return b.PinWait(local_worker_id_);
}

CacheBlockQueueSource::~CacheBlockQueueSource() {
    if (queue_ && !queue_->read_closed()) {
        while (NextBlock().IsValid()) { }
    }
}

} // namespace data
} // namespace thrill

/******************************************************************************/
