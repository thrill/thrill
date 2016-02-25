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

} // namespace data
} // namespace thrill

/******************************************************************************/
