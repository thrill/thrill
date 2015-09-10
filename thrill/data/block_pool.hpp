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

#include <thrill/data/byte_block.hpp>
#include <thrill/mem/manager.hpp>
#include <thrill/mem/page_mapper.hpp>

#include <deque>
#include <mutex>

namespace thrill {
namespace data {

/*!
 * Pool to allocate, keep, swap out/in, and free all ByteBlocks on the host.
 */
class BlockPool
{
    static const bool debug = true;

public:
    explicit BlockPool(mem::Manager* mem_manager, mem::Manager* mem_manager_external)
        : mem_manager_(mem_manager, "BlockPool"),
          ext_mem_manager_(mem_manager_external, "BlockPool")
    { }

    ByteBlockPtr AllocateBlock(size_t block_size, bool pinned = false);

    //TODO make this async
    void UnpinBlock(const ByteBlockPtr& block_ptr);

    // TODO make this a future + Async
    //! Pins a block by swapping it in if required.
    ByteBlockPtr PinBlock(const ByteBlockPtr& block_ptr);

    size_t block_count() const noexcept;

protected:
    //! local Manager counting only ByteBlock allocations in internal memory.
    mem::Manager mem_manager_;

    //! local Manager counting only ByteBlocks in external memory.
    mem::Manager ext_mem_manager_;

    //! PageMapper used for swapping-in/-out blocks
    mem::PageMapper<default_block_size> page_mapper_;

    // list of all blocks that are not swapped but are not pinned
    std::deque<ByteBlock*> victim_blocks_;


    size_t num_swapped_blocks_ = { 0 };
    size_t num_pinned_blocks_ = { 0 };

    std::mutex list_mutex_;

    friend class ByteBlock;


    //! Mechanism to swap block to disk. Blocking call.
    void SwapBlockOut(const ByteBlockPtr& block_ptr);

    //! Mechanism to swap block from disk. Blocking call.
    void SwapBlockIn(const ByteBlockPtr& block_ptr);

    void DestroyBlock(ByteBlock* block);
};

} // namespace data
} // namespace thrill
#endif // !THRILL_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
