/*******************************************************************************
 * c7a/data/block_pool.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_POOL_HEADER
#define C7A_DATA_BLOCK_POOL_HEADER

#include <c7a/common/allocator.hpp>
#include <c7a/common/logger.hpp>

namespace c7a {
namespace data {

/*!
 * Pool to allocate, keep, swap out/in, and free all ByteBlocks on the host.
 */
class BlockPool
{
    static const bool debug = false;

public:
    BlockPool(common::MemoryManager* memory_manager)
        : memory_manager_(memory_manager)
    { }

    void AllocateBlock(size_t block_size) {
        memory_manager_.add(block_size);
        ++block_count_;

        LOG << "AllocateBlock() total_count=" << block_count_
            << " total_size=" << memory_manager_.total();
    }

    void FreeBlock(size_t block_size) {
        memory_manager_.subtract(block_size);
        --block_count_;

        LOG << "FreeBlock() total_count=" << block_count_
            << " total_size=" << memory_manager_.total();
    }

protected:
    //! local MemoryManager counting only ByteBlock allocations.
    common::MemoryManager memory_manager_;

    //! total number of blocks in system
    std::atomic<size_t> block_count_ { 0 };
};

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
