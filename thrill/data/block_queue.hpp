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

namespace thrill {
namespace data {

//! \addtogroup data_layer
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
public:
    static constexpr bool debug = false;

    using Writer = BlockWriter<BlockQueue>;
    using Reader = DynBlockReader;
    using ConsumeReader = BlockReader<ConsumeBlockQueueSource>;

    using CloseCallback = tlx::delegate<void(BlockQueue&)>;

    //! Constructor from BlockPool
    BlockQueue(BlockPool& block_pool, size_t local_worker_id,
               size_t dia_id,
               const CloseCallback& close_callback = CloseCallback());

    //! non-copyable: delete copy-constructor
    BlockQueue(const BlockQueue&) = delete;
    //! non-copyable: delete assignment operator
    BlockQueue& operator = (const BlockQueue&) = delete;
    //! move-constructor: default
    BlockQueue(BlockQueue&&) = default;
    //! move-assignment operator: default
    BlockQueue& operator = (BlockQueue&&) = default;

    void AppendBlock(const Block& b, bool /* is_last_block */) final {
        LOG << "BlockQueue::AppendBlock() " << b;
        item_counter_ += b.num_items();
        byte_counter_ += b.size();
        block_counter_++;
        queue_.enqueue(b);
    }
    void AppendBlock(Block&& b, bool /* is_last_block */) final {
        LOG << "BlockQueue::AppendBlock() move " << b;
        item_counter_ += b.num_items();
        byte_counter_ += b.size();
        block_counter_++;
        queue_.enqueue(std::move(b));
    }

    //! Close called by BlockWriter.
    void Close() final;

    static constexpr bool allocate_can_fail_ = false;

    Block Pop() {
        if (read_closed_) return Block();
        Block b;
        queue_.wait_dequeue(b);
        read_closed_ = !b.IsValid();
        return b;
    }

    //! change dia_id after construction (needed because it may be unknown at
    //! construction)
    void set_dia_id(size_t dia_id) {
        file_.set_dia_id(dia_id);
    }

    //! set the close callback
    void set_close_callback(const CloseCallback& cb) {
        close_callback_ = cb;
    }

    //! check if writer side Close() was called.
    bool write_closed() const { return write_closed_; }

    bool empty() const { return queue_.size_approx() == 0; }

    //! check if reader side has returned a closing sentinel block
    bool read_closed() const { return read_closed_; }

    //! return number of block in the queue. Use this ONLY for DEBUGGING!
    size_t size() { return queue_.size_approx() - (write_closed() ? 1 : 0); }

    //! Returns item_counter_
    size_t item_counter() const { return item_counter_; }
    //! Returns byte_counter_
    size_t byte_counter() const { return byte_counter_; }
    //! Returns block_counter_
    size_t block_counter() const { return block_counter_; }
    //! Returns timespan_
    const common::StatsTimer& timespan() const { return timespan_; }

    //! Return a BlockWriter delivering to this BlockQueue.
    Writer GetWriter(size_t block_size = default_block_size) {
        return Writer(this, block_size);
    }

    //! return BlockReader specifically for a BlockQueue
    ConsumeReader GetConsumeReader(size_t local_worker_id);

    //! return polymorphic BlockSource variant
    DynBlockSource GetBlockSource(bool consume, size_t local_worker_id);

    //! return polymorphic BlockReader variant
    Reader GetReader(bool consume, size_t local_worker_id);

    //! Returns source_
    void * source() const { return source_; }

    //! set opaque source pointer
    void set_source(void* source) { source_ = source; }

private:
    common::ConcurrentBoundedQueue<Block> queue_;

    common::AtomicMovable<bool> write_closed_ = { false };

    //! whether Pop() has returned a closing Block; hence, if we received the
    //! close message from the writer
    bool read_closed_ = false;

    //! number of items transfered by the Queue
    size_t item_counter_ = 0;
    //! number of bytes transfered by the Queue
    size_t byte_counter_ = 0;
    //! number of blocks transfered by the Queue
    size_t block_counter_ = 0;
    //! timespan of existance
    common::StatsTimerStart timespan_;

    //! File to cache blocks for implementing CacheBlockQueueSource.
    File file_;

    //! callback to issue when the writer closes the Queue -- for delivering
    //! stats
    CloseCallback close_callback_;

    //! opaque pointer to the source (used by close_callback_ if needed).
    void* source_ = nullptr;

    //! for access to file_
    friend class CacheBlockQueueSource;
};

/*!
 * A BlockSource to read Block from a BlockQueue using a BlockReader. Each Block
 * is *taken* from the BlockQueue, hence the BlockQueue can be read only once!
 */
class ConsumeBlockQueueSource
{
    static constexpr bool debug = BlockQueue::debug;

public:
    //! Start reading from a BlockQueue
    explicit ConsumeBlockQueueSource(BlockQueue& queue, size_t local_worker_id);

    void Prefetch(size_t /* prefetch */);

    //! Advance to next block of file, delivers current_ and end_ for
    //! BlockReader. Returns false if the source is empty.
    PinnedBlock NextBlock();

private:
    //! BlockQueue that blocks are retrieved from
    BlockQueue& queue_;

    //! local worker id of the thread _reading_ the BlockQueue
    size_t local_worker_id_;
};

/*!
 * A BlockSource to read Blocks from a BlockQueue using a BlockReader, and at
 * the same time CACHE all items received. All Blocks read from the BlockQueue
 * are saved in the cache File. If the cache BlockQueue is initially already
 * closed, then Blocks are read from the File instead.
 */
class CacheBlockQueueSource
{
    static constexpr bool debug = BlockQueue::debug;

public:
    //! Start reading from a BlockQueue
    explicit CacheBlockQueueSource(BlockQueue* queue, size_t local_worker_id);

    //! non-copyable: delete copy-constructor
    CacheBlockQueueSource(const CacheBlockQueueSource&) = delete;
    //! non-copyable: delete assignment operator
    CacheBlockQueueSource& operator = (const CacheBlockQueueSource&) = delete;
    //! move-constructor: default
    CacheBlockQueueSource(CacheBlockQueueSource&& s);

    void Prefetch(size_t /* prefetch */);

    //! Return next block for BlockQueue, store into caching File and return it.
    PinnedBlock NextBlock();

    //! Consume remaining blocks and cache them in the File.
    ~CacheBlockQueueSource();

private:
    //! Reference to BlockQueue
    BlockQueue* queue_;

    //! local worker id of the thread _reading_ the BlockQueue
    size_t local_worker_id_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_QUEUE_HEADER

/******************************************************************************/
