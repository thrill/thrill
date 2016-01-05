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
#include <thrill/data/block.hpp>
#include <thrill/data/block_pool.hpp>

#include <sys/mman.h>

#include <limits>

namespace thrill {
namespace data {

BlockPool::~BlockPool() {
    assert(total_pins_ == 0);
    for (size_t& p : num_pinned_blocks_) {
        assert(p == 0);
    }
}

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

    // create counting ptr, no need for special make_shared()-equivalent
    PinnedByteBlockPtr result(new ByteBlock(data, size, this), local_worker_id);
    IncBlockPinCountNoLock(result.get(), local_worker_id);

    ++total_pins_;
    ++num_pinned_blocks_[local_worker_id];

    LOG << "BlockPool::AllocateBlock() size=" << size
        << " local_worker_id=" << local_worker_id
        << " total_count=" << block_count()
        << " total_size=" << mem_manager_.total()
        << " ++total_pins_=" << total_pins_;

    return result;
}

//! Pins a block by swapping it in if required.
std::future<PinnedBlock> BlockPool::PinBlock(const Block& block, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);
    std::unique_lock<std::mutex> lock(mutex_);

    std::promise<PinnedBlock> result;

    if (block.byte_block()->pin_count(local_worker_id) > 0)
    {
        // We may get a Block who's underlying is already pinned, since
        // PinnedBlock become Blocks when transfered between Files or delivered
        // via GetItemRange() or Scatter().

        IncBlockPinCountNoLock(block.byte_block(), local_worker_id);
        result.set_value(PinnedBlock(block, local_worker_id));

        LOG << "BlockPool::PinBlock block=" << &block
            << " already pinned by thread"
            << " ++total_pins_=" << total_pins_;

        return result.get_future();
    }

    ++total_pins_;
    ++num_pinned_blocks_[local_worker_id];

    IncBlockPinCountNoLock(block.byte_block(), local_worker_id);
    result.set_value(PinnedBlock(block, local_worker_id));

    LOG << "BlockPool::PinBlock block=" << &block
        << " ++total_pins_=" << total_pins_;

    return result.get_future();

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

//! Increment a ByteBlock's pin count
void BlockPool::IncBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    assert(block_ptr->pin_count_[local_worker_id] > 0);
    return IncBlockPinCountNoLock(block_ptr, local_worker_id);
}

//! Increment a ByteBlock's pin count
void BlockPool::IncBlockPinCountNoLock(ByteBlock* block_ptr, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);

    size_t p = ++block_ptr->pin_count_[local_worker_id];
    size_t tp = ++block_ptr->total_pins_;

    LOG << "BlockPool::IncBlockPinCount()"
        << " ++pin_count[" << local_worker_id << "]=" << p
        << " ++total_pins_=" << tp
        << " local_worker_id=" << local_worker_id;
}

void BlockPool::DecBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    assert(local_worker_id < workers_per_host_);
    assert(block_ptr->pin_count_[local_worker_id] > 0);
    assert(block_ptr->total_pins_ > 0);

    size_t p = --block_ptr->pin_count_[local_worker_id];
    size_t tp = --block_ptr->total_pins_;

    LOG << "BlockPool::DecBlockPinCount()"
        << " --pin_count[" << local_worker_id << "]=" << p
        << " --total_pins_=" << tp
        << " local_worker_id=" << local_worker_id;

    if (p == 0)
        UnpinBlock(block_ptr, local_worker_id);
}

void BlockPool::UnpinBlock(ByteBlock* block_ptr, size_t local_worker_id) {
    assert(local_worker_id < workers_per_host_);

    // decrease per-thread total pin count (memory locked by thread)
    assert(block_ptr->pin_count(local_worker_id) == 0);
    assert(num_pinned_blocks_[local_worker_id] > 0);
    --num_pinned_blocks_[local_worker_id];

    if (block_ptr->total_pins_ != 0) {
        LOG << "BlockPool::UnpinBlock()"
            << " --Block::total_pins_=" << block_ptr->total_pins_;
        return;
    }

    // if all per-thread pins are zero, allow this Block to be swapped out.
    assert(total_pins_ > 0);
    --total_pins_;

    unpinned_blocks_.push_back(block_ptr);

    LOG << "BlockPool::UnpinBlock()"
        << " --total_pins_=" << block_ptr->total_pins_
        << " allow swap out.";
}

size_t BlockPool::block_count() const noexcept {
    return total_pins_ + unpinned_blocks_.size() + num_swapped_blocks_;
}

void BlockPool::DestroyBlock(ByteBlock* block) {
    std::lock_guard<std::mutex> lock(mutex_);

    // pinned blocks cannot be destroyed since they are always unpinned first
    assert(block->pin_count_total() == 0);

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
