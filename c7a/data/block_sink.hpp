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
#include <memory>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * Pure virtual base class for all things that can receive Blocks from a
 * BlockWriter.
 */
template <size_t BlockSize>
class BlockSink
{
public:
    using Block = data::Block<BlockSize>;
    using BlockPtr = std::shared_ptr<Block>;
    using VirtualBlock = data::VirtualBlock<BlockSize>;

    //! Closes the sink. Must not be called multiple times
    virtual void Close() = 0;

    //! Appends the VirtualBlock, moving it out.
    virtual void AppendBlock(const VirtualBlock& vb) = 0;

    //! Appends the VirtualBlock and detaches it afterwards.
    void AppendBlock(const BlockPtr& block, size_t begin, size_t end,
                     size_t first_item, size_t nitems) {
        return AppendBlock(VirtualBlock(block, begin, end, first_item, nitems));
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_SINK_HEADER

/******************************************************************************/
