/*******************************************************************************
 * c7a/data/discard_sink.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_DISCARD_SINK_HEADER
#define C7A_DATA_DISCARD_SINK_HEADER

#include <c7a/data/block.hpp>
#include <c7a/data/block_sink.hpp>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * DiscardSink is an BlockSink that discards all Blocks delivered to it. Use it
 * for benchmarking!
 */
template <size_t BlockSize>
class DiscardSinkBase : public BlockSink<BlockSize>
{
public:
    using Block = data::Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const Block>;
    using VirtualBlock = data::VirtualBlock<BlockSize>;
    using ChannelId = size_t;

    //! Create discarding BlockSink.
    DiscardSinkBase() { }

    //! Discards VirtualBlock.
    void AppendBlock(const VirtualBlock&) override { }

    //! Closes the sink
    void Close() override {
        assert(!closed_);
        closed_ = true;
    }

    //! return close flag
    bool closed() const { return closed_; }

protected:
    bool closed_ = false;
};

//! DiscardSinkBase with default block size.
using DiscardSink = DiscardSinkBase<data::default_block_size>;

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_DISCARD_SINK_HEADER

/******************************************************************************/
