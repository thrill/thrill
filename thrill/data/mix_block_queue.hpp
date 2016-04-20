/*******************************************************************************
 * thrill/data/mix_block_queue.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_MIX_BLOCK_QUEUE_HEADER
#define THRILL_DATA_MIX_BLOCK_QUEUE_HEADER

#include <thrill/common/atomic_movable.hpp>
#include <thrill/common/concurrent_bounded_queue.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/block_queue.hpp>
#include <thrill/data/block_reader.hpp>
#include <thrill/data/cat_block_source.hpp>
#include <thrill/data/dyn_block_reader.hpp>
#include <thrill/data/file.hpp>

#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

class MixStream;
class MixBlockQueueReader;

/*!
 * Implements reading an unordered sequence of items from multiple workers,
 * which sends Blocks. This class is mainly used to implement MixChannel.
 *
 * When Blocks arrive from the net, the Multiplexer pushes (src, Blocks) pairs
 * to MixChannel, which pushes them into a MixBlockQueue. The
 * MixBlockQueue stores these in a ConcurrentBoundedQueue for atomic reading.
 *
 * When the MixChannel should be read, MixBlockQueueReader is used, which
 * retrieves Blocks from the queue. The Reader contains one complete BlockReader
 * for each inbound worker, and these BlockReaders are attached to BlockQueue
 * instances inside the MixBlockQueue.
 *
 * To enable unordered reading from multiple workers, the only remaining thing
 * to do is to fetch Blocks from the main mix queue and put them into the
 * right BlockQueue for the sub-readers to consume. By taking the Blocks from
 * the main mix queue, the Reader only blocks when no inbound Blocks are
 * available.
 *
 * To enable switching between items from different workers, the
 * MixBlockQueueReader keeps track of how many _whole_ items are available on
 * each reader. This number is simply -1 of the number of items known to start
 * in the received blocks. The last item _may_ span further Blocks, and cannot
 * be fetched without infinitely blocking the sub-reader, since no thread will
 * deliver the next Block.
 */
class MixBlockQueue
{
    static constexpr bool debug = false;

public:
    //! pair of (source worker, Block) stored in the main mix queue.
    struct SrcBlockPair {
        size_t src;
        Block  block;
    };

    using Reader = MixBlockQueueReader;

    //! Constructor from BlockPool
    MixBlockQueue(BlockPool& block_pool, size_t num_workers,
                  size_t local_worker_id, size_t dia_id);

    //! non-copyable: delete copy-constructor
    MixBlockQueue(const MixBlockQueue&) = delete;
    //! non-copyable: delete assignment operator
    MixBlockQueue& operator = (const MixBlockQueue&) = delete;
    //! move-constructor: default
    MixBlockQueue(MixBlockQueue&&) = default;
    //! move-assignment operator: default
    MixBlockQueue& operator = (MixBlockQueue&&) = default;

    //! change dia_id after construction (needed because it may be unknown at
    //! construction)
    void set_dia_id(size_t dia_id);

    //! return block pool
    BlockPool& block_pool() { return block_pool_; }

    //! append block delivered via the network from src.
    void AppendBlock(size_t src, const Block& block);

    //! append block delivered via the network from src.
    void AppendBlock(size_t src, Block&& block);

    //! append closing sentinel block from src (also delivered via the network).
    void Close(size_t src);

    //! Blocking retrieval of a (source,block) pair.
    SrcBlockPair Pop();

    //! check if writer side Close() was called.
    bool write_closed() const { return write_open_count_ == 0; }

    //! check if reader side has returned a closing sentinel block
    bool read_closed() const { return read_open_ == 0; }

private:
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

    //! BlockQueues to deliver blocks to from mix queue.
    std::vector<BlockQueue> queues_;

    //! for access to queues_ and other internals.
    friend class MixBlockQueueReader;
};

/*!
 * Implementation of BlockSink which forward Blocks to a mix queue with a
 * fixed source worker tag. Used to implement loopback sinks in MixChannel.
 */
class MixBlockQueueSink final : public BlockSink
{
    static constexpr bool debug = false;

public:
    MixBlockQueueSink(MixStream& mix_stream,
                      size_t from_global, size_t from_local);

    void AppendBlock(const Block& b) final;

    void AppendBlock(Block&& b) final;

    void Close() final;

    static constexpr bool allocate_can_fail_ = false;

    //! check if writer side Close() was called.
    bool write_closed() const { return write_closed_; }

    //! source mix stream instance
    void set_src_mix_stream(MixStream* src_mix_stream);

private:
    //! destination mix stream
    MixStream& mix_stream_;

    //! destination mix queue
    MixBlockQueue& mix_queue_;

    //! source mix stream instance
    MixStream* src_mix_stream_ = nullptr;

    //! close flag
    common::AtomicMovable<bool> write_closed_ = { false };

    //! fixed global source worker id
    size_t from_global_;

    size_t item_counter_ = 0;
    size_t byte_counter_ = 0;
    size_t block_counter_ = 0;
};

/*!
 * Reader to retrieve items in unordered sequence from a MixBlockQueue. This
 * is not a full implementation of _all_ methods available in a normal
 * BlockReader. Mainly, this is because only retrieval of _whole_ items are
 * possible. Due to the unordered sequence, these probably have to be all of
 * equal type as well.
 *
 * The Reader supports all combinations of consuming and keeping. However, do
 * not assume that the second round of reading delivers items in the same order
 * as the first. This is because once items are cached inside the BlockQueues of
 * MixBlockQueue, we use a plain CatReader to deliver them again (which is
 * probably faster as it has a sequential access pattern).
 *
 * See \ref MixBlockQueue for more information on how items are read.
 */
class MixBlockQueueReader
{
    static constexpr bool debug = false;

public:
    using CatBlockSource = data::CatBlockSource<DynBlockSource>;
    using CatBlockReader = BlockReader<CatBlockSource>;

    MixBlockQueueReader(MixBlockQueue& mix_queue,
                        bool consume, size_t local_worker_id);

    //! non-copyable: delete copy-constructor
    MixBlockQueueReader(const MixBlockQueueReader&) = delete;
    //! non-copyable: delete assignment operator
    MixBlockQueueReader& operator = (const MixBlockQueueReader&) = delete;
    //! move-constructor: default
    MixBlockQueueReader(MixBlockQueueReader&&) = default;
    //! move-assignment operator: default
    MixBlockQueueReader& operator = (MixBlockQueueReader&&) = default;

    //! Possibly consume unread blocks.
    ~MixBlockQueueReader();

    //! HasNext() returns true if at least one more item is available.
    bool HasNext()  {
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
                if (!PullBlock()) {
                    throw std::runtime_error(
                              "Data underflow in MixBlockQueueReader.");
                }
            }

            assert(available_ > 0);
            assert(selected_ < readers_.size());

            --available_;
            return readers_[selected_].template Next<T>();
        }
    }

private:
    //! reference to mix queue
    MixBlockQueue& mix_queue_;

    //! flag whether we are rereading the mix queue by reading the files using
    //! a cat_reader_.
    const bool reread_;

    //! \name Attributes for Mix Reading
    //! \{

    //! sub-readers for each block queue in mix queue
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

    //! for rereading the mix queue: use a cat reader on the embedded
    //! BlockQueue's files.
    CatBlockReader cat_reader_ { CatBlockSource() };

    bool PullBlock();
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_MIX_BLOCK_QUEUE_HEADER

/******************************************************************************/
