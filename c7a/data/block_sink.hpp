/*******************************************************************************
 * c7a/data/block_sink.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_SINK_HEADER
#define C7A_DATA_BLOCK_SINK_HEADER

#include <c7a/data/block.hpp>
#include <c7a/data/block_pool.hpp>
#include <memory>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * Pure virtual base class for all things that can receive Blocks from a
 * BlockWriter.
 */
class BlockSink
{
public:
    //! constructor with reference to BlockPool
    BlockSink(BlockPool& block_pool)
        : block_pool_(block_pool)
    { }

    //! required virtual destructor
    virtual ~BlockSink() { }

    //! Allocate a ByteBlock with n bytes backing memory. If returned
    //! ByteBlockPtr is a nullptr, then memory of this BlockSink is exhausted.
    virtual ByteBlockPtr AllocateByteBlock(size_t block_size) {
        return ByteBlock::Allocate(block_pool_, block_size);
    }

    //! Release an unused ByteBlock with n bytes backing memory.
    virtual void ReleaseByteBlock(ByteBlockPtr& block) {
        block = nullptr;
    }

    //! Closes the sink. Must not be called multiple times
    virtual void Close() = 0;

    //! Appends the Block, moving it out.
    virtual void AppendBlock(const Block& b) = 0;

    //! Appends the Block and detaches it afterwards.
    void AppendBlock(const ByteBlockPtr& byte_block, size_t begin, size_t end,
                     size_t first_item, size_t nitems) {
        return AppendBlock(Block(byte_block, begin, end, first_item, nitems));
    }

protected:
    //! reference to BlockPool for allocation and deallocation.
    BlockPool& block_pool_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_SINK_HEADER

/******************************************************************************/
