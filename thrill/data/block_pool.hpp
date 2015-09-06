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

#include <thrill/data/byte_block.hpp>
#include <thrill/mem/manager.hpp>

#include <deque>
#include <mutex>

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

    ByteBlockPtr AllocateBlock(size_t block_size, bool pinned = false);

    void UnpinBlock(const ByteBlockPtr& block_ptr);

    //TODO make this a future + Async
    //! Pins a block by swapping it in if required.
    ByteBlockPtr PinBlock(const ByteBlockPtr& block_ptr);

    size_t block_count() const noexcept;

protected:
    //! local Manager counting only ByteBlock allocations.
    mem::Manager mem_manager_;

    //list of all blocks that are no victims & not swapped
    std::deque<ByteBlock*> pinned_blocks_;

    //list of all blocks that are not swapped but are not pinned
    std::deque<ByteBlock*> victim_blocks_;

    //list of all blocks that are swapped out and not pinned
    std::deque<ByteBlock*> swapped_blocks_;

    std::mutex list_mutex_;

    friend class ByteBlock;

    void ClaimBlockMemory(size_t block_size);

    //! Called by ByteBlock when it is deleted or swapped
    void FreeBlockMemory(size_t block_size);

    //! Mechanism to swap block to disk. No changes to pool or block state are made. Blocking call.
    void SwapBlockOut(const ByteBlockPtr& block_ptr) const;

    //! Mechanism to swap block from disk. No changes to pool or block state are made. Blocking call.
    void SwapBlockIn(const ByteBlockPtr& block_ptr);

    void DestroyBlock(ByteBlock* block);
};

} // namespace data
} // namespace thrill


/******************************************************************************/
