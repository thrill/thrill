/*******************************************************************************
 * thrill/data/block_pool.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <sys/mman.h>

#include <thrill/common/logger.hpp>
#include <thrill/data/block_pool.hpp>

namespace thrill {
namespace data {

ByteBlockPtr BlockPool::AllocateBlock(size_t block_size, bool pinned) {
    ByteBlock* block = nullptr;

    if (block_size == default_block_size) {
        //allocate backed memory region for block
        sLOG << "allocating block with size" << block_size << " with disk backing";
        block = reinterpret_cast<ByteBlock*>(page_mapper_.Allocate());

        //call ctor of ByteBlock to initialize
        size_t actual_size = block_size - sizeof(common::ReferenceCount) - sizeof(ByteBlock::head);
        new (block)ByteBlock(actual_size, this, pinned);
    } else {
        //fallback to normal allocate
        sLOG << "allocating block with size" << block_size << " without disk backing";
        block = ByteBlock::Allocate(block_size, this, pinned);
    }

    std::lock_guard<std::mutex> lock(list_mutex_);
    mem_manager_.add(block_size);
    ByteBlockPtr result(block);
    // we store a raw pointer --> does not increase ref count
    if (pinned || block_size != default_block_size) {
        pinned_blocks_.push_back(block);
        pinned_blocks_.back()->head.pin_count_++;
        block->head.pin_count_++;
        LOG << "allocating pinned block @" << block;
    }
    else {
        victim_blocks_.push_back(block);
        LOG << "allocating unpinned block @" << block;
    }

    LOG << "AllocateBlock() total_count=" << block_count()
        << " total_size=" << mem_manager_.total();

    //pack and ship as ref-counting pointer
    return result;
}

void BlockPool::UnpinBlock(const ByteBlockPtr& block_ptr) {
    std::lock_guard<std::mutex> lock(list_mutex_);
    LOG << "unpinning block @" << &*block_ptr;
    if (--(block_ptr->head.pin_count_) == 0) {
        sLOG << "unpinned block reached ping-count 0";
        // pin count reached 0 --> move to victim list
        pinned_blocks_.erase(std::find(pinned_blocks_.begin(), pinned_blocks_.end(), block_ptr));
        victim_blocks_.push_back(block_ptr);
    }
}

// TODO make this a future + Async
//! Pins a block by swapping it in if required.
ByteBlockPtr BlockPool::PinBlock(const ByteBlockPtr& block_ptr) {
    std::lock_guard<std::mutex> lock(list_mutex_);
    LOG << "pinning block @" << &*block_ptr;
    if (block_ptr->head.pin_count_ > 0) {
        sLOG << "already pinned - return ptr";
        return block_ptr;
    }
    SwapBlockIn(block_ptr);
    block_ptr->head.pin_count_++;

    // update lists
    if (block_ptr->head.swapped_out_)
        swapped_blocks_.erase(std::find(swapped_blocks_.begin(), swapped_blocks_.end(), block_ptr));
    else
        victim_blocks_.erase(std::find(victim_blocks_.begin(), victim_blocks_.end(), block_ptr));
    pinned_blocks_.push_back(block_ptr);

    return block_ptr;
}

size_t BlockPool::block_count() const noexcept {
    return pinned_blocks_.size() + victim_blocks_.size() + swapped_blocks_.size();
}
void BlockPool::ClaimBlockMemory(size_t block_size) {
    mem_manager_.add(block_size);

    sLOG << "freeblock() total_count=" << block_count()
         << " total_size=" << mem_manager_.total();
}

//! Called by ByteBlock when it is deleted or swapped
void BlockPool::FreeBlockMemory(size_t block_size) {
    mem_manager_.subtract(block_size);

    sLOG << "freeblock() total_count=" << block_count() <<
        " total_size=" << mem_manager_.total();
}

//! Mechanism to swap block to disk. No changes to pool or block state
//! are made. Blocking call.
void BlockPool::SwapBlockOut(const ByteBlockPtr& block_ptr) {
    // TODO implement this
    ext_mem_manager_.add(block_ptr->size());
    mem_manager_.subtract(block_ptr->size());
}

//! Mechanism to swap block from disk. No changes to pool or block state
//! are made. Blocking call.
void BlockPool::SwapBlockIn(const ByteBlockPtr& block_ptr) {
    // TODO implement this
    ext_mem_manager_.subtract(block_ptr->size());
    mem_manager_.add(block_ptr->size());
}

void BlockPool::DestroyBlock(ByteBlock* block) {
    std::lock_guard<std::mutex> lock(list_mutex_);

    // pinned blocks cannot be destroyed since they are always unpinned first
    assert(block->head.pin_count_ == 0);

    LOG << "destroying block @" << block;

    // we have one reference, but we decreased that by hand
    if (block->head.swapped_out_) {
        const auto pos = std::find(swapped_blocks_.begin(), swapped_blocks_.end(), block);
        assert(pos != swapped_blocks_.end());
        swapped_blocks_.erase(pos);
        ext_mem_manager_.subtract(block->size());
    }
    else {
        const auto pos = std::find(victim_blocks_.begin(), victim_blocks_.end(), block);
        assert(pos != victim_blocks_.end());
        victim_blocks_.erase(pos);
        mem_manager_.subtract(block->size());
    }
}

} // namespace data
} // namespace thrill

/******************************************************************************/
