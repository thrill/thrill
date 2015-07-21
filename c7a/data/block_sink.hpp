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
    virtual void Append(VirtualBlock&& vb) = 0;

    //! Appends the VirtualBlock and detaches it afterwards.
    void Append(const BlockPtr& block, size_t block_used,
                size_t nitems, size_t first) {
        return Append(VirtualBlock(block, block_used, nitems, first));
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_SINK_HEADER

/******************************************************************************/
