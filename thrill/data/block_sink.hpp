/*******************************************************************************
 * thrill/data/block_sink.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_SINK_HEADER
#define THRILL_DATA_BLOCK_SINK_HEADER

#include <thrill/data/block.hpp>
#include <thrill/data/block_pool.hpp>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

/*!
 * Pure virtual base class for all things that can receive Blocks from a
 * BlockWriter.
 */
class BlockSink
{
public:
    //! constructor with reference to BlockPool
    explicit BlockSink(BlockPool& block_pool, size_t local_worker_id)
        : block_pool_(&block_pool), local_worker_id_(local_worker_id)
    { }

    //! default copy-constructor
    BlockSink(const BlockSink&) = default;
    //! default assignment operator
    BlockSink& operator = (const BlockSink&) = default;
    //! move-constructor: default
    BlockSink(BlockSink&&) = default;
    //! move-assignment operator: default
    BlockSink& operator = (BlockSink&&) = default;

    //! required virtual destructor
    virtual ~BlockSink() { }

    //! Allocate a ByteBlock with n bytes backing memory. If returned
    //! ByteBlockPtr is a nullptr, then memory of this BlockSink is exhausted.
    virtual PinnedByteBlockPtr
    AllocateByteBlock(size_t block_size) {
        return block_pool_->AllocateByteBlock(block_size, local_worker_id_);
    }

    //! Release an unused ByteBlock with n bytes backing memory.
    virtual void ReleaseByteBlock(ByteBlockPtr& block) {
        block = nullptr;
    }

    //! Returns BlockPool.logger_
    common::JsonLogger& logger() { return block_pool_->logger(); }

    //! Returns block_pool_
    BlockPool * block_pool() const { return block_pool_; }

    //! return number of workers per host
    size_t workers_per_host() const { return block_pool_->workers_per_host(); }

    //! boolean flag whether to check if AllocateByteBlock can fail in any
    //! subclass (if false: accelerate BlockWriter to not be able to cope with
    //! nullptr).
    static constexpr bool allocate_can_fail_ = false;

    //! Closes the sink. Must not be called multiple times
    virtual void Close() = 0;

    //! Appends the (unpinned) Block
    virtual void AppendBlock(const Block& b, bool is_last_block) = 0;

    //! Appends the (unpinned) Block
    virtual void AppendBlock(Block&& b, bool is_last_block) = 0;

    //! Appends the PinnedBlock
    virtual void AppendPinnedBlock(const PinnedBlock& b, bool is_last_block) {
        return AppendBlock(b.ToBlock(), is_last_block);
    }

    //! Appends the PinnedBlock
    virtual void AppendPinnedBlock(PinnedBlock&& b, bool is_last_block) {
        return AppendBlock(std::move(b).MoveToBlock(), is_last_block);
    }

    //! local worker id to associate pinned block with
    size_t local_worker_id() const { return local_worker_id_; }

private:
    //! reference to BlockPool for allocation and deallocation. (ptr to make
    //! BlockSink movable).
    BlockPool* block_pool_;

protected:
    //! local worker id to associate pinned block with
    size_t local_worker_id_;
};

/*!
 * Derivative BlockSink which counts and limits how many bytes it has delivered
 * as ByteBlocks for writing.
 */
class BoundedBlockSink : public virtual BlockSink
{
public:
    //! constructor with reference to BlockPool
    BoundedBlockSink(BlockPool& block_pool, size_t local_worker_id, size_t max_size)
        : BlockSink(block_pool, local_worker_id),
          max_size_(max_size), available_(max_size)
    { }

    PinnedByteBlockPtr AllocateByteBlock(size_t block_size) final {
        if (available_ < block_size) return PinnedByteBlockPtr();
        available_ -= block_size;
        return BlockSink::AllocateByteBlock(block_size);
    }

    void ReleaseByteBlock(ByteBlockPtr& block) final {
        if (block)
            available_ += block->size();
        block = nullptr;
    }

    size_t max_size() const { return max_size_; }

    static constexpr bool allocate_can_fail_ = true;

private:
    //! maximum allocation of ByteBlock for this BlockSink
    size_t max_size_;

    //! currently allocated ByteBlock for this BlockSink.
    size_t available_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_SINK_HEADER

/******************************************************************************/
