/*******************************************************************************
 * thrill/data/block_pool.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/logger.hpp>
#include <thrill/data/block_pool.hpp>

#include <sys/mman.h>

#include <limits>

namespace thrill {
namespace data {

PinnedByteBlockPtr BlockPool::AllocateByteBlock(size_t size, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);
    std::lock_guard<std::mutex> lock(mutex_);

#if 0
    // uint32_t swap_token;

    Byte* block_memory;
    if (swapable) {
        block_memory = static_cast<Byte*>(page_mapper_.Allocate(swap_token));
    }
    else {
        // malloc should use mmap internally if required
        block_memory = static_cast<Byte*>(malloc(block_size));
        swap_token = std::numeric_limits<uint32_t>::max();
    }

    ByteBlock* block = new ByteBlock(block_memory, block_size, this, pinned, swap_token);
    ByteBlockPtr result(block);

    // we store a raw pointer --> does not increase ref count
    if (!pinned) {
        unpinned_blocks_.push_back(block); //implicit converts to raw
        LOG << "allocating unpinned block @" << block;
    }
    else {
        LOG << "allocating pinned block @" << block;
        num_pinned_blocks_++;
    }
#endif

    RequestInternalMemory(size);

    // allocate block memory.
    Byte* data = static_cast<Byte*>(malloc(size));

    // create counting ptr, no need for special make_shared-equivalent
    PinnedByteBlockPtr result
        = PinnedByteBlockPtr::Acquire(new ByteBlock(data, size, this), local_worker_id);

    ++total_pinned_blocks_;
    ++num_pinned_blocks_[local_worker_id];

    LOG << "BlockPool::AllocateBlock() size=" << size
        << " local_worker_id=" << local_worker_id
        << " total_count=" << block_count()
        << " total_size=" << mem_manager_.total();

    return result;
}

#if 0
//! Pins a block by swapping it in if required.
common::Future<Pin> BlockPool::PinBlock(ByteBlock* block_ptr) {
    pin_mutex_.lock();
    LOG << "BlockPool::PinBlock block_ptr=" << block_ptr;

#if 0

    // first check, then increment
    if ((block_ptr->pin_count_)++ > 0) {
        sLOG << "already pinned - return ptr";

        pin_mutex_.unlock();
        callback();
        return;
    }
    num_pinned_blocks_++;

    // in memory & not pinned -> is in unpinned
    if (block_ptr->in_memory()) {
        unpinned_blocks_.erase(std::find(unpinned_blocks_.begin(), unpinned_blocks_.end(), block_ptr));

        pin_mutex_.unlock();
        callback();
        return;
    }
    else {      //we need to swap it in
        pin_mutex_.unlock();
        tasks_.Enqueue(
            [&]() {
                // maybe blocking call until memory is available
                RequestInternalMemory(block_ptr->size());

                // use the memory
                page_mapper_.SwapIn(block_ptr->swap_token_);

                // we must aqcuire the lock for the background thread
                std::lock_guard<std::mutex> lock(list_mutex_);
                num_swapped_blocks_--;
                ext_mem_manager_.subtract(block_ptr->size());
                callback();
            });
    }
#endif
}

void BlockPool::UnpinBlock(ByteBlock* block_ptr) {
    return; // -tb: disable for now
    std::lock_guard<std::mutex> lock(pin_mutex_);
    LOG << "unpinning block @" << block_ptr;
    if (--(block_ptr->pin_count_) == 0) {
        sLOG << "unpinned block reached ping-count 0";
        // pin count reached 0 --> move to unpinned list
        std::lock_guard<std::mutex> lock(list_mutex_);
        unpinned_blocks_.push_back(block_ptr);
        num_pinned_blocks_--;
    }
}
#endif

void BlockPool::UnpinBlock(ByteBlock* block_ptr, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);

    std::lock_guard<std::mutex> lock(mutex_);
    assert(block_ptr->pin_count() == 0);
    unpinned_blocks_.push_back(block_ptr);

    assert(total_pinned_blocks_ > 0);
    --total_pinned_blocks_;

    assert(num_pinned_blocks_[local_worker_id] > 0);
    --num_pinned_blocks_[local_worker_id];
}

size_t BlockPool::block_count() const noexcept {
    return total_pinned_blocks_ + unpinned_blocks_.size() + num_swapped_blocks_;
}

void BlockPool::DestroyBlock(ByteBlock* block) {
    std::lock_guard<std::mutex> lock(mutex_);

    // pinned blocks cannot be destroyed since they are always unpinned first
    assert(block->pin_count_ == 0);

    LOG << "BlockPool::DestroyBlock() block=" << block;

    // we have one reference, but we decreased that by hand
    if (!block->in_memory()) {
        abort();
        num_swapped_blocks_--;
        // ext_mem_manager_.subtract(block->size());
    }
#if 0
    else if (!block->swapable()) {
        // there is no mmap'ed region - simply free the memory
        ReleaseInternalMemory(block->size());
        free(static_cast<void*>(block->data_));
        block->data_ = nullptr;
    }
#endif
    else {
        // unpinned block in memory, remove from list
        auto pos = std::find(unpinned_blocks_.begin(), unpinned_blocks_.end(), block);
        assert(pos != unpinned_blocks_.end());
        unpinned_blocks_.erase(pos);
        // page_mapper_.SwapOut(block->data_, false);
        // page_mapper_.ReleaseToken(block->swap_token_);
        ReleaseInternalMemory(block->size());
    }
}

void BlockPool::RequestInternalMemory(size_t amount) {
    // allocate memory, then check limits and maybe block
    mem_manager_.add(amount);

    return; // -tb later.
#if 0
    if (!hard_memory_limit_ && !soft_memory_limit_) return;

    if (hard_memory_limit_ && mem_manager_.total() > hard_memory_limit_) {
        sLOG << "hard memory limit is reached";
        std::unique_lock<std::mutex> lock(memory_mutex_);
        memory_change_.wait(lock, [&]() { return mem_manager_.total() < hard_memory_limit_; });
    }
    if (soft_memory_limit_ && mem_manager_.total() > soft_memory_limit_) {
        sLOG << "soft memory limit is reached";
        // kill first page in unpinned list
        tasks_.Enqueue([&]() {
                           std::lock_guard<std::mutex> lock(mutex_);
                           sLOG << "removing a unpinned block";
                           if (unpinned_blocks_.empty()) return;
                           page_mapper_.SwapOut(unpinned_blocks_.front()->data_);
                           unpinned_blocks_.pop_front();
                       });
    }
#endif
}

void BlockPool::ReleaseInternalMemory(size_t amount) {
    mem_manager_.subtract(amount);

    return; // -tb later.

    if (hard_memory_limit_ && mem_manager_.total() < hard_memory_limit_)
        memory_change_.notify_all();
}

} // namespace data
} // namespace thrill

/******************************************************************************/
