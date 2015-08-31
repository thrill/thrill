/*******************************************************************************
 * thrill/data/block_pool.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_POOL_HEADER
#define THRILL_DATA_BLOCK_POOL_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/mem/manager.hpp>
#include <thrill/data/block.hpp>

namespace thrill {
namespace data {

/*!
 * Pool to allocate, keep, swap out/in, and free all ByteBlocks on the host.
 */
class BlockPool
{
    static const bool debug = false;

public:
    explicit BlockPool(mem::Manager* mem_manager)
        : mem_manager_(mem_manager, "BlockPool")
    { }

    ByteBlockPtr AllocateBlock(size_t block_size, bool pinned = true) {
        mem_manager_.add(block_size);
        std::lock_guard<std::mutex>(list_mutex_);

        if (pinned) {
            pinned_blocks_.emplace_back(ByteBlock::Allocate(block_size, *this));
            assert(pinned_blocks_.back().DecReference());
            pinned_blocks_.back().pin_count_.IncReference();
        } else {
            victim_blocks_.emplace_back(ByteBlock::Allocate(block_size, *this));
            assert(victim_blocks_.back().DecReference());
        }

        LOG << "AllocateBlock() total_count=" << block_count()
            << " total_size=" << mem_manager_.total();
    }

    void UnpinBlock(const ByteBlockPtr& block_ptr) {
        std::lock_guard<std::mutex>(list_mutex_);
        if(block_ptr->pin_count_.DecReference()) {
            //pin count reached 0 --> move to victim list
            pinned_blocks_.remove(block_ptr);
            victim_list_.push_back(block_ptr);
        }
    }

    //TODO make this a future + Async
    //! Pins a block by swapping it in if required.
    ByteBlockPtr PinBlock(const ByteBlockPtr& block_ptr) {
        std::lock_guard<std::mutex>(list_mutex_);
        if(block_ptr->pin_count_ > 0)
            return block_ptr;
        SwapBlockIn(block_ptr);
        block_ptr.pin_count_.IncReference();

        //update lists
        if (block_ptr->swapped_out_)
            swapped_blocks_.remove(block_ptr);
        else
            victim_list_.remove(block_ptr);
        pinned_blocks_.push_back(block_ptr);

        return block_ptr;
    }


    size_t block_count() const {
        return pinned_blocks_.size() + victim_blocks_.size() + swapped_blocks_.size();
    }

protected:
    //! local Manager counting only ByteBlock allocations.
    mem::Manager mem_manager_;

    //list of all blocks that are no victims & not swapped
    std::deque<ByteBlockPtr> pinned_blocks_;

    //list of all blocks that are not swapped but are not pinned
    std::deque<ByteBlockPtr> victim_blocks_;

    //list of all blocks that are swapped out and not pinned
    std::deque<ByteBlockPtr> swapped_blocks_;

    std::mutex list_mutex_;

    void ClaimBlockMemory(size_t block_size) {
        mem_manager_.increase(block_size);

        log << "freeblock() total_count=" << block_count()
            << " total_size=" << mem_manager_.total();
    }

    //! Called by ByteBlock when it is deleted or swapped
    void FreeBlockMemory(size_t block_size) {
        mem_manager_.subtract(block_size);

        log << "freeblock() total_count=" << block_count()
            << " total_size=" << mem_manager_.total();
    }

    //! Mechanism to swap block to disk. No changes to pool or block state are made. Blocking call.
    void ByteBlockPtr SwapBlockOut(const ByteBlockPtr& block_ptr) const {
        //TODO implement this
        return block_ptr;
    }

    //! Mechanism to swap block from disk. No changes to pool or block state are made. Blocking call.
    void ByteBlockPtr SwapBlockIn(const ByteBlockPtr& block_ptr) {
        //TODO implement this
        return block_ptr;
    }

    void DestroyBlock(ByteBlock& block) {
        assert(block.pin_count_ == 0);

        //we have one reference, but we decreased that by hand
        if (block->swaped_out_)
            swapped_blocks_.remove(block);
        else
            victim_blocks_.remove(block);
    }
};

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
