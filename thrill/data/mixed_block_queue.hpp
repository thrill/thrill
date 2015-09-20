/*******************************************************************************
 * thrill/data/mixed_block_queue.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_MIXED_BLOCK_QUEUE_HEADER
#define THRILL_DATA_MIXED_BLOCK_QUEUE_HEADER

#include <thrill/common/atomic_movable.hpp>
#include <thrill/common/concurrent_bounded_queue.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/data/dyn_block_reader.hpp>
#include <thrill/data/file.hpp>

#include <memory>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data Data Subsystem
//! \{

class MixedBlockQueueReader;

/*!
 * Implements reading an unordered sequence of items from multiple workers,
 * which sends Blocks. This class is mainly used to implement MixedChannel.
 *
 * When Blocks arrive from the net, the Multiplexer pushes (src, Blocks) pairs
 * to MixedChannel, which pushes them into a MixedBlockQueue. The
 * MixedBlockQueue stores these in a ConcurrentBoundedQueue for atomic reading.
 *
 * When the MixedChannel should be read, MixedBlockQueueReader is used, which
 * retrieves Blocks from the queue. The Reader contains one complete BlockReader
 * for each inbound worker, and these BlockReaders are attached to BlockQueue
 * instances inside the MixedBlockQueue.
 *
 * To enable unordered reading from multiple workers, the only remaining thing
 * to do is to fetch Blocks from the main mixed queue and put them into the
 * right BlockQueue for the sub-readers to consume. By taking the Blocks from
 * the main mix queue, the Reader only blocks when no inbound Blocks are
 * available.
 *
 * To enable switching between items from different workers, the
 * MixedBlockQueueReader keeps track of how many _whole_ items are available on
 * each reader. This number is simply -1 of the number of items known to start
 * in the received blocks. The last item _may_ span further Blocks, and cannot
 * be fetched without infinitely blocking the sub-reader, since no thread will
 * deliver the next Block.
 */
class MixedBlockQueue
{
    static const bool debug = false;

public:
    //! pair of (source worker, Block) stored in the main mix queue.
    struct SrcBlockPair {
        size_t src;
        Block  block;
    };

    using Reader = MixedBlockQueueReader;

    //! Constructor from BlockPool
    explicit MixedBlockQueue(BlockPool& block_pool, size_t num_workers)
        : block_pool_(block_pool),
          num_workers_(num_workers),
          write_closed_(num_workers) {
        queues_.reserve(num_workers);
        for (size_t w = 0; w < num_workers; ++w) {
            queues_.emplace_back(block_pool_);
        }
    }

    //! non-copyable: delete copy-constructor
    MixedBlockQueue(const MixedBlockQueue&) = delete;
    //! non-copyable: delete assignment operator
    MixedBlockQueue& operator = (const MixedBlockQueue&) = delete;
    //! move-constructor: default
    MixedBlockQueue(MixedBlockQueue&&) = default;
    //! move-assignment operator: default
    MixedBlockQueue& operator = (MixedBlockQueue&&) = default;

    //! return block pool
    BlockPool & block_pool() { return block_pool_; }

    //! append block delivered via the network from src.
    void AppendBlock(size_t src, const Block& block) {
        LOG << "MixedBlockQueue::AppendBlock"
            << " src=" << src << " block=" << block;
        mix_queue_.emplace(SrcBlockPair { src, block });
    }

    //! append closing sentinel block from src (also delivered via the network).
    void Close(size_t src) {
        LOG << "MixedBlockQueue::Close" << " src=" << src;
        assert(!write_closed_[src]);
        write_closed_[src] = true;
        --write_open_count_;

        // enqueue a closing Block.
        mix_queue_.emplace(SrcBlockPair { src, Block() });
    }

    //! Blocking retrieval of a (source,block) pair.
    SrcBlockPair Pop() {
        if (read_open_ == 0) return SrcBlockPair { size_t(-1), Block() };
        SrcBlockPair b;
        mix_queue_.pop(b);
        if (!b.block.IsValid()) --read_open_;
        return b;
    }

    //! check if writer side Close() was called.
    bool write_closed() const { return write_open_count_ == 0; }

    //! check if reader side has returned a closing sentinel block
    bool read_closed() const { return read_open_ == 0; }

protected:
    BlockPool& block_pool_;

    //! the main mix queue, containing the block in the reception order.
    common::ConcurrentBoundedQueue<SrcBlockPair> mix_queue_;

    //! total number of workers in system.
    size_t num_workers_;

    //! counter on number of writers still open.
    common::AtomicMovable<size_t> write_open_count_ { num_workers_ };

    //! flag to test for closed sources
    std::vector<unsigned char> write_closed_;

    //! number of times Pop() has not yet returned a closing Block; hence, if we
    //! received the close message from the writer.
    size_t read_open_ = num_workers_;

    //! BlockQueues to deliver blocks to from mixed queue.
    std::vector<BlockQueue> queues_;

    //! for access to queues_ and other internals.
    friend class MixedBlockQueueReader;
};

/*!
 * Implementation of BlockSink which forward Blocks to a mixed queue with a
 * fixed source worker tag. Used to implement loopback sinks in MixedChannel.
 */
class MixedBlockQueueSink final : public BlockSink
{
    static const bool debug = false;

public:
    MixedBlockQueueSink(MixedBlockQueue& mixed_queue, size_t from)
        : BlockSink(mixed_queue.block_pool()),
          mixed_queue_(mixed_queue), from_(from)
    { }

    void AppendBlock(const Block& b) final {
        LOG << "MixedBlockQueueSink::AppendBlock()"
            << " from_=" << from_ << " b=" << b;
        mixed_queue_.AppendBlock(from_, b);
    }

    void Close() final {
        // enqueue a closing Block.
        LOG << "MixedBlockQueueSink::Close()"
            << " from_=" << from_;
        mixed_queue_.Close(from_);
        write_closed_ = true;
    }

    enum { allocate_can_fail_ = false };

    //! check if writer side Close() was called.
    bool write_closed() const { return write_closed_; }

protected:
    //! destination mixed queue
    MixedBlockQueue& mixed_queue_;

    //! close flag
    common::AtomicMovable<bool> write_closed_ = { false };

    //! fixed source worker id
    size_t from_;
};

/*!
 * Reader to retrieve items in unordered sequence from a MixedBlockQueue. This
 * is not a full implementation of _all_ methods available in a normal
 * BlockReader. Mainly, this is because only retrieval of _whole_ items are
 * possible. Due to the unordered sequence, these probably have to be all of
 * equal type as well.
 *
 * The Reader supports all combinations of consuming and keeping. However, do
 * not assume that the second round of reading delivers items in the same order
 * as the first. This is because once items are cached inside the BlockQueues of
 * MixedBlockQueue, we use a plain CatReader to deliver them again (which is
 * probably faster as it has a sequential access pattern).
 *
 * See \ref MixedBlockQueue for more information on how items are read.
 */
class MixedBlockQueueReader
{
    static const bool debug = false;

public:
    using CatBlockSource = data::CatBlockSource<DynBlockSource>;
    using CatBlockReader = BlockReader<CatBlockSource>;

    MixedBlockQueueReader(MixedBlockQueue& mix_queue, bool consume)
        : mix_queue_(mix_queue),
          consume_(consume), reread_(mix_queue.read_closed()) {

        if (!reread_) {
            readers_.reserve(mix_queue_.num_workers_);
            available_at_.resize(mix_queue_.num_workers_, 0);

            for (size_t w = 0; w < mix_queue_.num_workers_; ++w) {
                readers_.emplace_back(
                    mix_queue_.queues_[w].GetReader(consume));
            }
        }
        else {
            // construct vector of BlockSources to read from queues_.
            std::vector<DynBlockSource> result;
            for (size_t w = 0; w < mix_queue_.num_workers_; ++w) {
                result.emplace_back(mix_queue_.queues_[w].GetBlockSource(consume));
            }
            // move BlockQueueSources into concatenation BlockSource, and to Reader.
            cat_reader_ = CatBlockReader(CatBlockSource(std::move(result)));
        }
    }

    //! non-copyable: delete copy-constructor
    MixedBlockQueueReader(const MixedBlockQueueReader&) = delete;
    //! non-copyable: delete assignment operator
    MixedBlockQueueReader& operator = (const MixedBlockQueueReader&) = delete;
    //! move-constructor: default
    MixedBlockQueueReader(MixedBlockQueueReader&&) = default;
    //! move-assignment operator: default
    MixedBlockQueueReader& operator = (MixedBlockQueueReader&&) = default;

    //! Possibly consume unread blocks.
    ~MixedBlockQueueReader() {
        // TODO(tb)
    }

    //! HasNext() returns true if at least one more item is available.
    bool HasNext() {
        if (reread_) return cat_reader_.HasNext();

        if (available_) return true;
        if (open_ == 0) return false;

        return PullBlock();
    }

    //! Next() reads a complete item T
    template <typename T>
    T Next() {
        assert(HasNext());

        if (reread_) {
            return cat_reader_.template Next<T>();
        }
        else {
            if (!available_) {
                if (!PullBlock())
                    throw std::runtime_error(
                              "Data underflow in MixedBlockQueueReader.");
            }

            assert(available_ > 0);
            assert(selected_ < readers_.size());

            --available_;
            return readers_[selected_].template Next<T>();
        }
    }

protected:
    //! reference to mix queue
    MixedBlockQueue& mix_queue_;

    //! flag whether to consume the input
    bool consume_;

    //! flat whether we are rereading the mixed queue by reading the files using
    //! a cat_reader_.
    bool reread_;

    //! \name Attributes for Mixed Reading
    //! \{

    //! sub-readers for each block queue in mixed queue
    std::vector<BlockQueue::Reader> readers_;

    //! reader currently selected
    size_t selected_ = size_t(-1);

    //! number of available items on the selected reader
    size_t available_ = 0;

    //! number of additional items available at reader (excluding current
    //! available_)
    std::vector<size_t> available_at_;

    //! number of readers still open
    size_t open_ = mix_queue_.num_workers_;

    //! \}

    //! for rereading the mixed queue: use a cat reader on the embedded
    //! BlockQueue's files.
    CatBlockReader cat_reader_ { CatBlockSource() };

    bool PullBlock() {
        // no full item available: get next block from mixed queue
        while (available_ == 0) {
            LOG << "still open_=" << open_;

            MixedBlockQueue::SrcBlockPair src_blk = mix_queue_.Pop();
            LOG << "MixedBlockQueueReader::PullBlock()"
                << " src=" << src_blk.src << " block=" << src_blk.block;

            assert(src_blk.src < readers_.size());

            if (src_blk.block.IsValid()) {
                // block for this reader.
                selected_ = src_blk.src;

                // save block with data for reader
                mix_queue_.queues_[src_blk.src].AppendBlock(src_blk.block);

                // add available items: one less than in the blocks.
                available_at_[src_blk.src] += src_blk.block.num_items();
                available_ = available_at_[src_blk.src] - 1;
                available_at_[src_blk.src] -= available_;
            }
            else {
                // close block received: maybe get last item
                assert(open_ > 0);
                --open_;

                // save block with data for reader
                mix_queue_.queues_[src_blk.src].AppendBlock(src_blk.block);

                // check if we can still read the last item
                if (available_at_[src_blk.src]) {
                    assert(available_at_[src_blk.src] == 1);
                    selected_ = src_blk.src;
                    available_ = available_at_[src_blk.src];
                    available_at_[src_blk.src] -= available_;
                }
                else if (open_ == 0) return false;
            }
        }

        return true;
    }
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MIXED_BLOCK_QUEUE_HEADER

/******************************************************************************/
