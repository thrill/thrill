/*******************************************************************************
 * thrill/data/block_queue.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_QUEUE_HEADER
#define THRILL_DATA_BLOCK_QUEUE_HEADER

#include <thrill/common/atomic_movable.hpp>
#include <thrill/common/concurrent_bounded_queue.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/data/block_writer.hpp>
#include <thrill/data/dyn_block_reader.hpp>
#include <thrill/data/file.hpp>

#include <atomic>
#include <memory>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

class ConsumeBlockQueueSource;

/*!
 * A BlockQueue is a thread-safe queue used to hand-over Block objects between
 * threads. It is currently used by the Multiplexer to queue received Blocks and
 * deliver them (later) to their destination.
 *
 * The BlockQueue itself is also a BlockSink (so one can attach a BlockWriter to
 * it). To read items from the queue, one needs to use a BlockReader
 * instantiated with a BlockQueueSource.  Both are easily available via
 * GetWriter() and GetReader().  Each block is available only *once* via the
 * BlockQueueSource.
 */
class BlockQueue final : public BlockSink
{
    static const bool debug = false;

public:
    using Writer = BlockWriter<BlockQueue>;
    using Reader = DynBlockReader;
    using ConsumeReader = BlockReader<ConsumeBlockQueueSource>;

    //! Constructor from BlockPool
    explicit BlockQueue(BlockPool& block_pool)
        : BlockSink(block_pool), file_(block_pool)
    { }

    //! non-copyable: delete copy-constructor
    BlockQueue(const BlockQueue&) = delete;
    //! non-copyable: delete assignment operator
    BlockQueue& operator = (const BlockQueue&) = delete;
    //! move-constructor: default
    BlockQueue(BlockQueue&&) = default;
    //! move-assignment operator: default
    BlockQueue& operator = (BlockQueue&&) = default;

    void AppendBlock(const Block& b) final {
        queue_.emplace(b);
    }

    //! Close called by BlockWriter.
    void Close() final {
        assert(!write_closed_); // racing condition tolerated
        write_closed_ = true;

        // enqueue a closing Block.
        queue_.emplace();
    }

    enum { allocate_can_fail_ = false };

    Block Pop() {
        if (read_closed_) return Block();
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

    //! return BlockReader specifically for a BlockQueue
    ConsumeReader GetConsumeReader();

    //! return polymorphic BlockSource variant
    DynBlockSource GetBlockSource(bool consume);

    //! return polymorphic BlockReader variant
    Reader GetReader(bool consume);

private:
    common::ConcurrentBoundedQueue<Block> queue_;

    common::AtomicMovable<bool> write_closed_ = { false };

    //! whether Pop() has returned a closing Block; hence, if we received the
    //! close message from the writer
    bool read_closed_ = false;

    //! File to cache blocks for implementing ConstBlockQueueSource.
    File file_;

    //! for access to file_
    friend class CacheBlockQueueSource;
};

/*!
 * A BlockSource to read Block from a BlockQueue using a BlockReader. Each Block
 * is *taken* from the BlockQueue, hence the BlockQueue can be read only once!
 */
class ConsumeBlockQueueSource
{
public:
    //! Start reading from a BlockQueue
    explicit ConsumeBlockQueueSource(BlockQueue& queue)
        : queue_(queue)
    { }

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    Block NextBlock() {
        return queue_.Pop();
    }

private:
    //! BlockQueue that blocks are retrieved from
    BlockQueue& queue_;
};

inline
BlockQueue::ConsumeReader BlockQueue::GetConsumeReader() {
    assert(!read_closed_);
    return ConsumeReader(ConsumeBlockQueueSource(*this));
}

/*!
 * A BlockSource to read Blocks from a BlockQueue using a BlockReader, and at
 * the same time CACHE all items received. All Blocks read from the BlockQueue
 * are saved in the cache File. If the cache BlockQueue is initially already
 * closed, then Blocks are read from the File instead.
 */
class CacheBlockQueueSource
{
public:
    //! Start reading from a BlockQueue
    explicit CacheBlockQueueSource(BlockQueue* queue)
        : queue_(queue) { }

    //! non-copyable: delete copy-constructor
    CacheBlockQueueSource(const CacheBlockQueueSource&) = delete;
    //! non-copyable: delete assignment operator
    CacheBlockQueueSource& operator = (const CacheBlockQueueSource&) = delete;
    //! move-constructor: default
    CacheBlockQueueSource(CacheBlockQueueSource&& s)
        : queue_(s.queue_) { s.queue_ = nullptr; }

    //! Return next block for BlockQueue, store into caching File and return it.
    Block NextBlock() {
        Block b = queue_->Pop();

        // cache block in file_ (but not the termination block from the queue)
        if (b.IsValid())
            queue_->file_.AppendBlock(b);

        return b;
    }

    //! Consume remaining blocks and cache them in the File.
    ~CacheBlockQueueSource() {
        if (queue_ && !queue_->read_closed()) {
            while (NextBlock().IsValid()) { }
        }
    }

private:
    //! Reference to BlockQueue
    BlockQueue* queue_;
};

inline
DynBlockSource BlockQueue::GetBlockSource(bool consume) {
    if (consume && !read_closed_) {
        // set to consume, and BlockQueue has not been read.
        sLOG << "BlockQueue::GetBlockSource() consume, from queue.";
        return ConstructDynBlockSource<ConsumeBlockQueueSource>(*this);
    }
    else if (consume && read_closed_) {
        // consume the File, BlockQueue was already read.
        sLOG << "BlockQueue::GetBlockSource() consume, from cache:"
             << file_.num_items();
        return ConstructDynBlockSource<ConsumeFileBlockSource>(&file_);
    }
    else if (!consume && !read_closed_) {
        // non-consumer but the BlockQueue has not been read.
        sLOG << "BlockQueue::GetBlockSource() non-consume, from queue.";
        return ConstructDynBlockSource<CacheBlockQueueSource>(this);
    }
    else if (!consume && read_closed_) {
        // non-consumer: reread the file that was cached.
        sLOG << "BlockQueue::GetBlockSource() non-consume, from cache:"
             << file_.num_items();
        return ConstructDynBlockSource<KeepFileBlockSource>(file_, 0);
    }
    else {
        // impossible
        abort();
    }
}

inline
BlockQueue::Reader BlockQueue::GetReader(bool consume) {
    return DynBlockReader(GetBlockSource(consume));
}

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
