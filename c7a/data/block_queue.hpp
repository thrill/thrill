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

#include <c7a/common/concurrent_bounded_queue.hpp>
#include <c7a/data/block.hpp>
#include <c7a/data/block_reader.hpp>
#include <c7a/data/block_writer.hpp>

#include <atomic>
#include <memory>

namespace c7a {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

template <size_t BlockSize>
class BlockQueueSource;

/*!
 * A BlockQueue is a thread-safe queue used to hand-over VirtualBlock objects
 * between threads. It is currently used by the ChannelMultiplexer to queue
 * received Blocks and deliver them (later) to their destination.
 *
 * The BlockQueue itself is also a BlockSink (so one can attach a BlockWriter to
 * it). To read items from the queue, one needs to use a BlockReader
 * instantiated with a BlockQueueSource.  Both are easily available via
 * GetWriter() and GetReader().  Each block is available only *once* via the
 * BlockQueueSource.
 */
template <size_t BlockSize = default_block_size>
class BlockQueue : public BlockSink<BlockSize>
{
public:
    using Block = data::Block<BlockSize>;
    using BlockPtr = std::shared_ptr<Block>;

    using VirtualBlock = data::VirtualBlock<BlockSize>;

    using BlockSource = BlockQueueSource<BlockSize>;
    using Writer = BlockWriterBase<BlockSize>;
    using Reader = BlockReader<BlockSource>;

    void Append(VirtualBlock&& vb) override {
        queue_.emplace(std::move(vb));
    }

    void Close() override {
        assert(!closed_); // racing condition tolerated
        closed_ = true;

        // enqueue a closing VirtualBlock.
        queue_.emplace(nullptr, 0, 0, 0);
    }

    VirtualBlock Pop() {
        VirtualBlock vb;
        queue_.pop(vb);
        return std::move(vb);
    }

    bool closed() const { return closed_; }

    bool empty() const { return queue_.empty(); }

    //! return number of block in the queue. Use this ONLY for DEBUGGING!
    size_t size() { return queue_.size() - (closed() ? 1 : 0); }

    //! Return a BlockWriter delivering to this BlockQueue.
    Writer GetWriter() { return Writer(this); }

    Reader GetReader();

private:
    common::ConcurrentBoundedQueue<VirtualBlock> queue_;
    std::atomic<bool> closed_ = { false };
};

/*!
 * A BlockSource to read Block from a BlockQueue using a BlockReader. Each Block
 * is *taken* from the BlockQueue, hence the BlockQueue can be read only once!
 */
template <size_t BlockSize>
class BlockQueueSource
{
public:
    using Byte = unsigned char;

    using Block = data::Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const Block>;
    using BlockQueue = data::BlockQueue<BlockSize>;
    using VirtualBlock = data::VirtualBlock<BlockSize>;

    //! Start reading from a BlockQueue
    explicit BlockQueueSource(BlockQueue& queue)
        : queue_(queue)
    { }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    bool NextBlock(const Byte** out_current, const Byte** out_end) {
        VirtualBlock vb = queue_.Pop();
        block_ = vb.block;

        if (block_) {
            *out_current = block_->begin();
            *out_end = block_->begin() + vb.bytes_used;
            return true;
        }
        else {
            // termination block received.
            return false;
        }
    }

    bool closed() const {
        return queue_.closed();
    }

protected:
    //! BlockQueue that blocks are retrieved from
    BlockQueue& queue_;

    //! The current block being read.
    BlockCPtr block_;
};

template <size_t BlockSize>
typename BlockQueue<BlockSize>::Reader BlockQueue<BlockSize>::GetReader() {
    return BlockQueue<BlockSize>::Reader(BlockQueueSource<BlockSize>(*this));
}

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
