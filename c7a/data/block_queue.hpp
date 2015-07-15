/*******************************************************************************
 * c7a/data/block_queue.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <tobias.sturm@student.kit.edu>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_DATA_BLOCK_QUEUE_HEADER
#define C7A_DATA_BLOCK_QUEUE_HEADER

#include <c7a/data/block.hpp>
#include <c7a/data/block_reader.hpp>
#include <c7a/data/block_writer.hpp>
#include <c7a/common/concurrent_bounded_queue.hpp>

#include <atomic>
#include <memory>

namespace c7a {
namespace data {

template <size_t BlockSize>
class BlockQueueSource;

template <typename Block>
class DynBlockSink;

//! A BlockQueue is used to hand-over blocks between threads. It fulfills the
//same interface as \ref c7a::data::Stream and \ref c7a::data::File
template <size_t BlockSize = default_block_size>
class BlockQueue
{
public:
    using Block = data::Block<BlockSize>;
    using BlockPtr = std::shared_ptr<Block>;

    using Writer = BlockWriter<Block, BlockQueue&>;
    using Reader = BlockReader<BlockQueueSource<BlockSize> >;

    using DynWriter = BlockWriter<Block, DynBlockSink<Block> >;

    void Append(const BlockPtr& block, size_t block_used,
                size_t nitems, size_t first) {
        queue_.emplace(block, block_used, nitems, first);
    }

    void Close() {
        assert(!closed_); //racing condition tolerated
        closed_ = true;

        // enqueue a closing VirtualBlock.
        queue_.emplace(nullptr, 0, 0, 0);
    }

    VirtualBlock<BlockSize> Pop() {
        VirtualBlock<BlockSize> vb;
        queue_.pop(vb);
        return std::move(vb);
    }

    bool closed() const { return closed_; }

    bool empty() const { return queue_.empty(); }

    //! return number of block in the queue. Use this ONLY for DEBUGGING!
    size_t size() { return queue_.size(); }

    //! Return a BlockWriter delivering to this BlockQueue.
    Writer GetWriter() { return Writer(*this); }

    //! Return a BlockReader fetching blocks from this BlockQueue.
    Reader GetReader();

    //! Return a dynamic polymorphic BlockWriter delivering to this BlockQueue.
    DynWriter GetDynWriter() {
        return DynWriter(DynBlockSink<Block>(this));
    }

private:
    common::ConcurrentBoundedQueue<VirtualBlock<BlockSize> > queue_;
    std::atomic<bool> closed_ = { false };
};

//! A BlockSource to read Blocks from a BlockQueue using a BlockReader.
template <size_t BlockSize>
class BlockQueueSource
{
public:
    using Byte = unsigned char;

    using Block = data::Block<BlockSize>;
    using BlockQueueType = BlockQueue<BlockSize>;
    using BlockCPtr = std::shared_ptr<const Block>;

    using VirtualBlock = data::VirtualBlock<BlockSize>;

    //! Start reading from a BlockQueue
    BlockQueueSource(BlockQueueType& queue)
        : queue_(queue)
    { }

    //! Initialize the first block to be read by BlockReader
    void Initialize(const Byte** out_current, const Byte** out_end) {
        if (!NextBlock(out_current, out_end))
            *out_current = *out_end = nullptr;
    }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    bool NextBlock(const Byte** out_current, const Byte** out_end) {
        VirtualBlock vb = queue_.Pop();
        block_ = vb.block;

        if (block_) {
            *out_current = block_->begin();
            *out_end = block_->begin() + vb.block_used;
            return true;
        }
        else {
            // termination block received.
            return false;
        }
    }

protected:
    //! BlockQueue that blocks are retrieved from
    BlockQueueType& queue_;

    //! The current block being read.
    BlockCPtr block_;
};

template <size_t BlockSize>
typename BlockQueue<BlockSize>::Reader BlockQueue<BlockSize>::GetReader() {
    return BlockQueue<BlockSize>::Reader(*this);
}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
