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
        : mem_manager_(mem_manager)
    { }

    void AllocateBlock(size_t block_size) {
        mem_manager_.add(block_size);
        ++block_count_;

        LOG << "AllocateBlock() total_count=" << block_count_
            << " total_size=" << mem_manager_.total();
    }

    void FreeBlock(size_t block_size) {
        mem_manager_.subtract(block_size);
        --block_count_;

        LOG << "FreeBlock() total_count=" << block_count_
            << " total_size=" << mem_manager_.total();
    }

protected:
    //! local Manager counting only ByteBlock allocations.
    mem::Manager mem_manager_;

    //! total number of blocks in system
    std::atomic<size_t> block_count_ { 0 };
};

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
