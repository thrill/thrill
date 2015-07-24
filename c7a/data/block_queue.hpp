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
#include <c7a/data/file.hpp>

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

    void AppendBlock(const VirtualBlock& vb) override {
        queue_.emplace(vb);
    }

    //! Close called by BlockWriter.
    void Close() override {
        assert(!closed_); // racing condition tolerated
        closed_ = true;

        // enqueue a closing VirtualBlock.
        queue_.emplace();
    }

    VirtualBlock Pop() {
        assert(!read_closed_);
        VirtualBlock vb;
        queue_.pop(vb);
        read_closed_ = !vb.IsValid();
        return vb;
    }

    //! check if writer side Close() was called.
    bool closed() const { return closed_; }

    bool empty() const { return queue_.empty(); }

    //! check if reader side has returned a closing sentinel block
    bool read_closed() const { return read_closed_; }

    //! return number of block in the queue. Use this ONLY for DEBUGGING!
    size_t size() { return queue_.size() - (closed() ? 1 : 0); }

    //! Return a BlockWriter delivering to this BlockQueue.
    Writer GetWriter() { return Writer(this); }

    Reader GetReader();

private:
    common::ConcurrentBoundedQueue<VirtualBlock> queue_;

    std::atomic<bool> closed_ = { false };

    //! whether Pop() has returned a closing VirtualBlock.
    bool read_closed_ = false;
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
    VirtualBlock NextBlock() {
        return queue_.Pop();
    }

    bool closed() const {
        return queue_.read_closed();
    }

protected:
    //! BlockQueue that blocks are retrieved from
    BlockQueue& queue_;
};

template <size_t BlockSize>
typename BlockQueue<BlockSize>::Reader BlockQueue<BlockSize>::GetReader() {
    return BlockQueue<BlockSize>::Reader(BlockQueueSource<BlockSize>(*this));
}

/*!
 * A BlockSource to read Blocks from a BlockQueue using a BlockReader, and at
 * the same time CACHE all items received. All Blocks read from the BlockQueue
 * are saved in the cache File. If the cache BlockQueue is initially already
 * closed, then Blocks are read from the File instead.
 */
template <size_t BlockSize>
class CachingBlockQueueSource
{
public:
    using Byte = unsigned char;

    using Block = data::Block<BlockSize>;
    using VirtualBlock = data::VirtualBlock<BlockSize>;
    using BlockQueue = data::BlockQueue<BlockSize>;
    using BlockQueueSource = data::BlockQueueSource<BlockSize>;
    using FileBlockSource = data::FileBlockSource<BlockSize>;

    //! Start reading from a BlockQueue
    CachingBlockQueueSource(BlockQueue& queue, File& file)
        : queue_src_(queue), file_src_(file), file_(file) {
        // determinate whether we read from the Queue or from the File.
        from_queue_ = !queue_src_.closed();
    }

    //! Return next virtual block for BlockReader.
    VirtualBlock NextBlock() {
        if (from_queue_) {
            VirtualBlock vb = queue_src_.NextBlock();
            // cache block in file_
            if (vb.IsValid())
                file_.AppendBlock(vb);
            return vb;
        }
        else {
            return file_src_.NextBlock();
        }
    }

    bool closed() const {
        return from_queue_ ? queue_src_.closed() : file_src_.closed();
    }

protected:
    //! whether we read from BlockQueue or from the File.
    bool from_queue_;

    //! BlockQueueSource
    BlockQueueSource queue_src_;

    //! FileBlockSource if the queue was already read.
    FileBlockSource file_src_;

    //! Reference to file for caching Blocks
    File& file_;
};

//! \}

} // namespace data
} // namespace c7a

#endif // !C7A_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
