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
#include <thrill/common/stats_timer.hpp>
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

    using CloseCallback = common::delegate<void(BlockQueue&)>;

    //! Constructor from BlockPool
    explicit BlockQueue(BlockPool& block_pool, size_t local_worker_id,
                        const CloseCallback& close_callback = CloseCallback())
        : BlockSink(block_pool, local_worker_id),
          file_(block_pool, local_worker_id),
          close_callback_(close_callback) {
        assert(local_worker_id < block_pool.workers_per_host());
    }

    //! non-copyable: delete copy-constructor
    BlockQueue(const BlockQueue&) = delete;
    //! non-copyable: delete assignment operator
    BlockQueue& operator = (const BlockQueue&) = delete;
    //! move-constructor: default
    BlockQueue(BlockQueue&&) = default;
    //! move-assignment operator: default
    BlockQueue& operator = (BlockQueue&&) = default;

    void AppendBlock(const PinnedBlock& b) final {
        byte_counter_ += b.size();
        block_counter_++;
        queue_.emplace(b);
    }
    void AppendBlock(PinnedBlock&& b) {
        byte_counter_ += b.size();
        block_counter_++;
        queue_.emplace(std::move(b));
    }

    //! Close called by BlockWriter.
    void Close() final {
        assert(!write_closed_);
        write_closed_ = true;

        block_counter_++;

        // enqueue a closing Block.
        queue_.emplace();

        if (close_callback_) close_callback_(*this);
    }

    enum { allocate_can_fail_ = false };

    PinnedBlock Pop() {
        if (read_closed_) return PinnedBlock();
        PinnedBlock b;
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

    //! Returns byte_counter_
    size_t byte_counter() const { return byte_counter_; }
    //! Returns block_counter_
    size_t block_counter() const { return block_counter_; }
    //! Returns timespan_
    const common::StatsTimer & timespan() const { return timespan_; }

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
    common::ConcurrentBoundedQueue<PinnedBlock> queue_;

    common::AtomicMovable<bool> write_closed_ = { false };

    //! whether Pop() has returned a closing Block; hence, if we received the
    //! close message from the writer
    bool read_closed_ = false;

    //! number of bytes transfered by the Queue
    size_t byte_counter_ = 0;
    //! number of blocks transfered by the Queue
    size_t block_counter_ = 0;
    //! timespan of existance
    common::StatsTimerStart timespan_;

    //! File to cache blocks for implementing ConstBlockQueueSource.
    File file_;

    //! callback to issue when the writer closes the Queue -- for delivering stats
    CloseCallback close_callback_;

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
    PinnedBlock NextBlock() {
        return queue_.Pop();
    }

private:
    //! BlockQueue that blocks are retrieved from
    BlockQueue& queue_;
};

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
    PinnedBlock NextBlock() {
        PinnedBlock b = queue_->Pop();

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

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
