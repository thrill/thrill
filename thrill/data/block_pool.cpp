/*******************************************************************************
 * thrill/data/block_pool.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <thrill/data/block_pool.hpp>
#include <thrill/common/logger.hpp>

namespace thrill {
namespace data {

    ByteBlockPtr BlockPool::AllocateBlock(size_t block_size, bool pinned) {
        std::lock_guard<std::mutex>lock (list_mutex_);

        ByteBlockPtr result = ByteBlock::Allocate(block_size, this);

        //we store a raw pointer --> does not increase ref count
        if (pinned) {
            pinned_blocks_.push_back(result);
            pinned_blocks_.back()->head.pin_count_++;
        } else {
            victim_blocks_.push_back(result);
        }

        LOG << "AllocateBlock() total_count=" << block_count()
            << " total_size=" << mem_manager_.total();
        return result;
    }

    void BlockPool::UnpinBlock(const ByteBlockPtr& block_ptr) {
        std::lock_guard<std::mutex>lock (list_mutex_);
        if(--block_ptr->head.pin_count_ == 0) {
            //pin count reached 0 --> move to victim list
            pinned_blocks_.erase(std::find(pinned_blocks_.begin(), pinned_blocks_.end(), block_ptr));
            victim_blocks_.push_back(block_ptr);
        }
    }

    //TODO make this a future + Async
    //! Pins a block by swapping it in if required.
    ByteBlockPtr BlockPool::PinBlock(const ByteBlockPtr& block_ptr) {
        std::lock_guard<std::mutex>lock (list_mutex_);
        if(block_ptr->head.pin_count_ > 0)
            return block_ptr;
        SwapBlockIn(block_ptr);
        block_ptr->head.pin_count_++;

        //update lists
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

    //! Mechanism to swap block to disk. No changes to pool or block state are made. Blocking call.
    void BlockPool::SwapBlockOut(const ByteBlockPtr& block_ptr) const {
        //TODO implement this
    }

    //! Mechanism to swap block from disk. No changes to pool or block state are made. Blocking call.
    void BlockPool::SwapBlockIn(const ByteBlockPtr& block_ptr) {
        //TODO implement this
    }

    void BlockPool::DestroyBlock(ByteBlock* block) {
        assert(block->head.pin_count_ == 0);

        //we have one reference, but we decreased that by hand
        if (block->head.swapped_out_) {
            assert(std::find(swapped_blocks_.begin(), swapped_blocks_.end(), block) != swapped_blocks_.end());
            swapped_blocks_.erase(std::find(swapped_blocks_.begin(), swapped_blocks_.end(), block));
        } else {
            assert(std::find(victim_blocks_.begin(), victim_blocks_.end(), block) != victim_blocks_.end());
            victim_blocks_.erase(std::find(victim_blocks_.begin(), victim_blocks_.end(), block));
        }
    }

} // namespace data
} // namespace thrill


/******************************************************************************/
