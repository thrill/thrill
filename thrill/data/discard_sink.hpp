/*******************************************************************************
 * thrill/data/discard_sink.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
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
class DiscardSink final : public BlockSink
{
public:
    using Writer = BlockWriter<DiscardSink>;
    using DynWriter = DynBlockWriter;

    //! Create discarding BlockSink.
    explicit DiscardSink(BlockPool& block_pool, size_t local_worker_id)
        : BlockSink(block_pool, local_worker_id)
    { }

    //! Discards a Block.
    void AppendBlock(const Block&) final { }

    //! Discards a Block.
    void AppendBlock(Block&&) final { }

    //! Closes the sink
    void Close() final {
        assert(!closed_);
        closed_ = true;
    }

    //! return close flag
    bool closed() const { return closed_; }

    //! boolean flag whether to check if AllocateByteBlock can fail in any
    //! subclass (if false: accelerate BlockWriter to not be able to cope with
    //! nullptr).
    static constexpr bool allocate_can_fail_ = false;

    //! Return a BlockWriter delivering to this BlockSink.
    Writer GetWriter(size_t block_size = default_block_size) {
        return Writer(this, block_size);
    }

    //! Return a BlockWriter delivering to this BlockSink.
    DynWriter GetDynWriter(size_t block_size = default_block_size) {
        return DynWriter(this, block_size);
    }

private:
    bool closed_ = false;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_DISCARD_SINK_HEADER

/******************************************************************************/
