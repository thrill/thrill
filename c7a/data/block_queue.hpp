/*******************************************************************************
 * c7a/data/block_queue.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
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

class BlockQueueSource;

/*!
 * A BlockQueue is a thread-safe queue used to hand-over Block objects
 * between threads. It is currently used by the ChannelMultiplexer to queue
 * received Blocks and deliver them (later) to their destination.
 *
 * The BlockQueue itself is also a BlockSink (so one can attach a BlockWriter to
 * it). To read items from the queue, one needs to use a BlockReader
 * instantiated with a BlockQueueSource.  Both are easily available via
 * GetWriter() and GetReader().  Each block is available only *once* via the
 * BlockQueueSource.
 */
class BlockQueue : public BlockSink
{
public:
    using BlockSource = BlockQueueSource;
    using Writer = BlockWriter;
    using Reader = BlockReader<BlockQueueSource>;

    void AppendBlock(const Block& b) override {
        queue_.emplace(b);
    }

    //! Close called by BlockWriter.
    void Close() override {
        assert(!write_closed_); // racing condition tolerated
        write_closed_ = true;

        // enqueue a closing Block.
        queue_.emplace();
    }

    Block Pop() {
        assert(!read_closed_);
        Block b;
        queue_.pop(b);
        read_closed_ = !b.IsValid();
        return b;
    }

    //! check if writer side Close() was called.
    bool write_closed() const { return write_closed_; }

    bool empty() const { return queue_.empty(); }

    //! check if reader side has returned a closing sentinel block
    bool read_closed() const { return read_closed_; }

    //! return number of block in the queue. Use this ONLY for DEBUGGING!
    size_t size() { return queue_.size() - (write_closed() ? 1 : 0); }

    //! Return a BlockWriter delivering to this BlockQueue.
    Writer GetWriter(size_t block_size = default_block_size) {
        return Writer(this, block_size);
    }

    Reader GetReader();

private:
    common::ConcurrentBoundedQueue<Block> queue_;

    std::atomic<bool> write_closed_ = { false };

    //! whether Pop() has returned a closing Block.
    bool read_closed_ = false;
};

/*!
 * A BlockSource to read Block from a BlockQueue using a BlockReader. Each Block
 * is *taken* from the BlockQueue, hence the BlockQueue can be read only once!
 */
class BlockQueueSource
{
public:
    //! Start reading from a BlockQueue
    explicit BlockQueueSource(BlockQueue& queue)
        : queue_(queue)
    { }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    Block NextBlock() {
        return queue_.Pop();
    }

    bool closed() const {
        return queue_.read_closed();
    }

protected:
    //! BlockQueue that blocks are retrieved from
    BlockQueue& queue_;
};

inline
typename BlockQueue::Reader BlockQueue::GetReader() {
    return BlockQueue::Reader(BlockQueueSource(*this));
}

/*!
 * A BlockSource to read Blocks from a BlockQueue using a BlockReader, and at
 * the same time CACHE all items received. All Blocks read from the BlockQueue
 * are saved in the cache File. If the cache BlockQueue is initially already
 * closed, then Blocks are read from the File instead.
 */
class CachingBlockQueueSource
{
public:
    //! Start reading from a BlockQueue
    CachingBlockQueueSource(BlockQueue& queue, File& file)
        : queue_src_(queue), file_src_(file), file_(file) {
        // determinate whether we read from the Queue or from the File.
        from_queue_ = !queue_src_.closed();
    }

    //! Return next block for BlockReader.
    Block NextBlock() {
        if (from_queue_) {
            Block b = queue_src_.NextBlock();
            // cache block in file_
            if (b.IsValid())
                file_.AppendBlock(b);
            return b;
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
