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

namespace thrill {
namespace data {

ByteBlockPtr BlockPool::AllocateBlock(size_t block_size, bool pinned) {
    size_t swap_token;
    std::lock_guard<std::mutex> lock(list_mutex_);

    Byte* block_memory = static_cast<Byte*>(page_mapper_.Allocate(swap_token));
    ByteBlock* block = new ByteBlock(block_memory, block_size, this, pinned, swap_token);
    ByteBlockPtr result(block);

    mem_manager_.add(block_size);
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

void BlockPool::UnpinBlock(const ByteBlockPtr& block_ptr) {
    std::lock_guard<std::mutex> lock(list_mutex_);
    LOG << "unpinning block @" << &*block_ptr;
    if (--(block_ptr->pin_count_) == 0) {
        sLOG << "unpinned block reached ping-count 0";
        // pin count reached 0 --> move to victim list
        victim_blocks_.push_back(block_ptr);
        num_pinned_blocks_--;
    }
}

// TODO make this a future + Async
//! Pins a block by swapping it in if required.
ByteBlockPtr BlockPool::PinBlock(const ByteBlockPtr& block_ptr) {
    std::lock_guard<std::mutex> lock(list_mutex_);
    LOG << "pinning block @" << &*block_ptr;

    // first check, then increment
    if ((block_ptr->pin_count_)++ > 0) {
        sLOG << "already pinned - return ptr";
        return block_ptr;
    }
    num_pinned_blocks_++;

    // in memory & not pinned -> is in victim
    if (block_ptr->in_memory()) {
        victim_blocks_.erase(std::find(victim_blocks_.begin(), victim_blocks_.end(), block_ptr));
    }
    else {      //we need to swap it in
        page_mapper_.SwapIn(block_ptr->swap_token_);
        num_swapped_blocks_--;
        ext_mem_manager_.subtract(block_ptr->size());
        mem_manager_.add(block_ptr->size());
    }

    return block_ptr;
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
    }
    else {
        const auto pos = std::find(victim_blocks_.begin(), victim_blocks_.end(), block);
        assert(pos != victim_blocks_.end());
        victim_blocks_.erase(pos);
        mem_manager_.subtract(block->size());

        // 'free' block's data and release token
        page_mapper_.SwapOut(block->data_, false);
        page_mapper_.ReleaseToken(block->swap_token_);
    }
}

} // namespace data
} // namespace thrill

/******************************************************************************/
