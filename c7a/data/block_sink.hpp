/*******************************************************************************
 * c7a/data/block_sink.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Tobias Sturm  <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_SINK_HEADER
#define C7A_DATA_BLOCK_SINK_HEADER

#include <c7a/data/block.hpp>
#include <memory>
#include <mutex>

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

/*!
 * ForwardingBlockSink is used when multiple block producers need to write to
 * the same BlockSink (workers push blocks to the same queue).
 * Close can be called multiple times and the actial destination BlockSink is
 * only closed when the expected number of close operations is reached.
 */
class ForwardingBlockSink : public BlockSink
{
public:

    ForwardingBlockSink(BlockSink& destination, size_t num_sources = 1)
        : destination_(destination), expected_closed_(num_sources) {
            assert(num_sources > 0);
        }

    //! Closes the sink. Can not be called multiple times
    void Close() final {
        std::lock_guard<std::mutex> lock(mutex_);
        if (++closed_ == expected_closed_)
            destination_.Close();
    }

    //! Appends the Block, moving it out.
    void AppendBlock(const Block& b) final {
        std::lock_guard<std::mutex> lock(mutex_);
        destination_.AppendBlock(b);
    }

private:
    BlockSink& destination_;
    size_t expected_closed_;
    size_t closed_ = 0;
    std::mutex mutex_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_SINK_HEADER

/******************************************************************************/
