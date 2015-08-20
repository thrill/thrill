/*******************************************************************************
 * thrill/data/discard_sink.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_DISCARD_SINK_HEADER
#define THRILL_DATA_DISCARD_SINK_HEADER

#include <thrill/data/block.hpp>
#include <thrill/data/block_sink.hpp>
#include <thrill/data/block_writer.hpp>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

/*!
 * DiscardSink is an BlockSink that discards all Blocks delivered to it. Use it
 * for benchmarking!
 */
class DiscardSink : public BlockSink
{
public:
    using Writer = BlockWriter;

    //! Create discarding BlockSink.
    DiscardSink() { }

    //! Discards Block.
    void AppendBlock(const Block&) final { }

    //! Closes the sink
    void Close() final {
        assert(!closed_);
        closed_ = true;
    }

    //! return close flag
    bool closed() const { return closed_; }

    //! Return a BlockWriter delivering to this BlockSink.
    Writer GetWriter(size_t block_size = default_block_size) {
        return Writer(this, block_size);
    }

protected:
    bool closed_ = false;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_DISCARD_SINK_HEADER

/******************************************************************************/
