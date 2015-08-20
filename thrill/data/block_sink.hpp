/*******************************************************************************
 * thrill/data/block_sink.hpp
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

#include <thrill/data/block.hpp>
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
    //! required virtual destructor
    virtual ~BlockSink() { }

    //! Closes the sink. Must not be called multiple times
    virtual void Close() = 0;

    //! Appends the Block, moving it out.
    virtual void AppendBlock(const Block& b) = 0;

    //! Appends the Block and detaches it afterwards.
    void AppendBlock(const ByteBlockPtr& byte_block, size_t begin, size_t end,
                     size_t first_item, size_t nitems) {
        return AppendBlock(Block(byte_block, begin, end, first_item, nitems));
    }
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_SINK_HEADER

/******************************************************************************/
