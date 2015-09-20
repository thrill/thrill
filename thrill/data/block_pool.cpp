/*******************************************************************************
 * thrill/data/block_pool.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <sys/mman.h>

#include <thrill/common/logger.hpp>
#include <thrill/data/block_pool.hpp>
#include <limits>

namespace thrill {
namespace data {

ByteBlockPtr BlockPool::AllocateBlock(size_t block_size, bool swapable, bool pinned) {
    assert(block_size <= default_block_size);
    uint32_t swap_token;
    std::lock_guard<std::mutex> lock(list_mutex_);

    Byte* block_memory;
    if (swapable) {
        block_memory = static_cast<Byte*>(page_mapper_.Allocate(swap_token));
    } else {
        //malloc should use mmap internally if required
        block_memory = static_cast<Byte*>(malloc(block_size));
        swap_token = std::numeric_limits<uint32_t>::max();
    }

    ByteBlock* block = new ByteBlock(block_memory, block_size, this, pinned, swap_token);
    ByteBlockPtr result(block);

    RequestInternalMemory(block_size);
    // we store a raw pointer --> does not increase ref count
    if (!pinned) {
        victim_blocks_.push_back(block); //implicit converts to raw
        LOG << "allocating unpinned block @" << block;
    }
    else {
        LOG << "allocating pinned block @" << block;
        num_pinned_blocks_++;
    }

    LOG << "AllocateBlock() total_count=" << block_count()
        << " total_size=" << mem_manager_.total();

    // pack and ship as ref-counting pointer
    return result;
}

void BlockPool::UnpinBlock(ByteBlock* block_ptr) {
    std::lock_guard<std::mutex> lock(list_mutex_);
    LOG << "unpinning block @" << block_ptr;
    if (--(block_ptr->pin_count_) == 0) {
        sLOG << "unpinned block reached ping-count 0";
        // pin count reached 0 --> move to victim list
        victim_blocks_.push_back(block_ptr);
        num_pinned_blocks_--;
    }
}

//! Pins a block by swapping it in if required.
void BlockPool::PinBlock(ByteBlock* block_ptr, common::delegate<void()>&& callback) {
    std::lock_guard<std::mutex> lock(list_mutex_);
    LOG << "pinning block @" << block_ptr;

    // first check, then increment
    if ((block_ptr->pin_count_)++ > 0) {
        sLOG << "already pinned - return ptr";

        callback();
        return;
    }
    num_pinned_blocks_++;

    // in memory & not pinned -> is in victim
    if (block_ptr->in_memory()) {
        victim_blocks_.erase(std::find(victim_blocks_.begin(), victim_blocks_.end(), block_ptr));

        callback();
        return;
    }
    else {      //we need to swap it in
        tasks_.Enqueue([&]() {
            //maybe blocking call until memory is available
            RequestInternalMemory(block_ptr->size());

            //use the memory
            page_mapper_.SwapIn(block_ptr->swap_token_);

            // we must aqcuire the lock for the background thread
            std::lock_guard<std::mutex> lock(list_mutex_);
            num_swapped_blocks_--;
            ext_mem_manager_.subtract(block_ptr->size());
            callback();
        });
    }
}

size_t BlockPool::block_count() const noexcept {
    return num_pinned_blocks_ + victim_blocks_.size() + num_swapped_blocks_;
}

void BlockPool::DestroyBlock(ByteBlock* block) {
    std::lock_guard<std::mutex> lock(list_mutex_);

    // pinned blocks cannot be destroyed since they are always unpinned first
    assert(block->pin_count_ == 0);

    LOG << "destroying block @" << block;

    // we have one reference, but we decreased that by hand
    if (!block->in_memory()) {
        num_swapped_blocks_--;
        ext_mem_manager_.subtract(block->size());
    } else if (!block->swapable()) {
        //there is no mmap'ed region - simply free the memory
        ReleaseInternalMemory(block->size());
        free(static_cast<void*>(block->data_));
        block->data_ = nullptr;
    }
    else {
        const auto pos = std::find(victim_blocks_.begin(), victim_blocks_.end(), block);
        assert(pos != victim_blocks_.end());
        victim_blocks_.erase(pos);
        page_mapper_.SwapOut(block->data_, false);
        page_mapper_.ReleaseToken(block->swap_token_);
        ReleaseInternalMemory(block->size());
    }
}

    void BlockPool::RequestInternalMemory(size_t amount) {
        //allocate memory, then check limits and maybe block
        mem_manager_.add(amount);

        if (!hard_memory_limit_ && !soft_memory_limit_) return;

        if (hard_memory_limit_ && mem_manager_.total() > hard_memory_limit_) {
            sLOG << "hard memory limit is reached";
            std::unique_lock<std::mutex> lock(memory_mutex_);
            memory_change_.wait(lock, [&](){ return mem_manager_.total() < hard_memory_limit_; });
        }
        if (soft_memory_limit_ && mem_manager_.total() > soft_memory_limit_) {
            sLOG << "soft memory limit is reached";
            //kill first page in victim list
            tasks_.Enqueue([&](){
                std::lock_guard<std::mutex> lock(list_mutex_);
                sLOG << "removing a victim block";
                if (victim_blocks_.empty()) return;
                page_mapper_.SwapOut(victim_blocks_.front()->data_);
                victim_blocks_.pop_front();
            });
        }
    }

    void BlockPool::ReleaseInternalMemory(size_t amount) {
        mem_manager_.subtract(amount);
        if (hard_memory_limit_ && mem_manager_.total() < hard_memory_limit_)
            memory_change_.notify_all();
    }

} // namespace data
} // namespace thrill

/******************************************************************************/
