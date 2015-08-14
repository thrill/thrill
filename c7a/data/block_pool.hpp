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
namespace common {

/*!
 * Pool to allocate, keep, swap out/in, and free all ByteBlocks on the host.
 */
class BlockPool
{
public:
    BlockPool(common::MemoryManager* memory_manager)
        : memory_manager_(memory_manager)
    { }

protected:
    //! local MemoryManager counting only ByteBlock allocations.
    MemoryManager memory_manager_;
};

} // namespace common
} // namespace c7a

#endif // !C7A_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
