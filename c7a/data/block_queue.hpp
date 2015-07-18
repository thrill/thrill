/*******************************************************************************
 * c7a/data/block_queue.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <tobias.sturm@student.kit.edu>
 *
 * This file has no license. Only Chuck Norris can compile it.
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

template <size_t BlockSize = default_block_size>
class ReadableBlockQueue
{
public:
    using Reader = BlockReader<BlockQueueSource<BlockSize> >;
    using VirtualBlock = data::VirtualBlock<BlockSize>;
    virtual VirtualBlock Pop() = 0;

    virtual bool closed() const = 0;

    virtual bool empty() const = 0;

    //! return number of block in the queue. Use this ONLY for DEBUGGING!
    virtual size_t size() = 0;

    //! Return a BlockReader fetching blocks from this BlockQueue.
    //! This call blocks until the queue has at least one block
    virtual Reader GetReader() = 0;
};

//! A BlockQueue is used to hand-over blocks between threads. It fulfills the
//same interface as \ref c7a::data::Stream and \ref c7a::data::File
template <size_t BlockSize = default_block_size>
class BlockQueue : public BlockSink<BlockSize>, public ReadableBlockQueue<BlockSize>
{
public:
    using Block = data::Block<BlockSize>;
    using BlockPtr = std::shared_ptr<Block>;

    using VirtualBlock = data::VirtualBlock<BlockSize>;

    using Writer = BlockWriterBase<BlockSize>;
    using Reader = BlockReader<BlockQueueSource<BlockSize> >;

    void Append(VirtualBlock&& vb) override {
        queue_.emplace(std::move(vb));
    }

    void Close() override {
        assert(!closed_); //racing condition tolerated
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

    //! see \ref ReadableBlockQueue
    Reader GetReader();

private:
    common::ConcurrentBoundedQueue<VirtualBlock> queue_;
    std::atomic<bool> closed_ = { false };
};

//! An OrderedMultiBlockQueue holds multiple BlockQueues and unifies them.
//! Pop will consume queue after queue, each until it is exhausted
//! The order of the queue consumption is always ascending
//! The OrderedMultiBlockQueue is read-only / no sink
template <size_t BlockSize = default_block_size>
class OrderedMultiBlockQueue : public ReadableBlockQueue<BlockSize>
{
    using Block = data::Block<BlockSize>;
    using BlockQueue = data::BlockQueue<BlockSize>;
    using BlockPtr = std::shared_ptr<Block>;
    using VirtualBlock = data::VirtualBlock<BlockSize>;
    using Reader = BlockReader<BlockQueueSource<BlockSize> >;

public:
    OrderedMultiBlockQueue(const std::vector<std::reference_wrapper<BlockQueue> >& queues) : queues_(queues) { }

    VirtualBlock Pop() {
        VirtualBlock vblock(nullptr, 0, 0, 0);
        //proceed to queue that has elements or waits for elements (aka not closed)
        while (vblock.IsEndBlock()) {
            while (
                current_queue_ < queues_.size() &&
                queues_[current_queue_].get().empty() &&
                queues_[current_queue_].get().closed()
                ) { current_queue_++; }

            if (current_queue_ == queues_.size()) //end of all queues
                return VirtualBlock(nullptr, 0, 0, 0);
            vblock = queues_[current_queue_].get().Pop();
        }
        return vblock;
    }

    bool closed() const {
        bool closed = true;
        for (const BlockQueue& q : queues_)
            closed = closed & q.closed();
        return closed;
    }

    bool empty() const {
        bool empty = true;
        for (const BlockQueue& q : queues_)
            empty = empty & q.empty();
        return empty;
    }

    //! return number of block in the queue. Use this ONLY for DEBUGGING!
    size_t size() {
        size_t result = 0;
        for (BlockQueue& q : queues_)
            result += q.size();
        return result;
    }

    //! see \ref ReadableBlockQueue
    Reader GetReader();

private:
    std::vector<std::reference_wrapper<BlockQueue> > queues_;
    size_t current_queue_ = 0;
};

//! A BlockSource to read Blocks from a BlockQueue using a BlockReader.
template <size_t BlockSize>
class BlockQueueSource
{
public:
    using Byte = unsigned char;

    using Block = data::Block<BlockSize>;
    using BlockCPtr = std::shared_ptr<const Block>;

    using VirtualBlock = data::VirtualBlock<BlockSize>;

    //! Start reading from a BlockQueue
    explicit BlockQueueSource(ReadableBlockQueue<BlockSize>& queue)
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
    ReadableBlockQueue<BlockSize>& queue_;

    //! The current block being read.
    BlockCPtr block_;
};

template <size_t BlockSize>
typename BlockQueue<BlockSize>::Reader BlockQueue<BlockSize>::GetReader() {
    return BlockQueue<BlockSize>::Reader(BlockQueueSource<BlockSize>(*this));
}
template <size_t BlockSize>
typename OrderedMultiBlockQueue<BlockSize>::Reader OrderedMultiBlockQueue<BlockSize>::GetReader() {
    return OrderedMultiBlockQueue<BlockSize>::Reader(BlockQueueSource<BlockSize>(*this));
}
} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
