/*******************************************************************************
 * thrill/data/block_pool.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_POOL_HEADER
#define THRILL_DATA_BLOCK_POOL_HEADER

#include <thrill/common/json_logger.hpp>
#include <thrill/common/profile_task.hpp>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/byte_block.hpp>
#include <thrill/io/block_manager.hpp>
#include <thrill/io/request.hpp>
#include <thrill/mem/aligned_allocator.hpp>
#include <thrill/mem/manager.hpp>
#include <thrill/mem/pool.hpp>

#include <algorithm>
#include <deque>
#include <functional>
#include <future>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace thrill {
namespace data {

//! \addtogroup data_layer
//! \{

/*!
 * Pool to allocate, keep, swap out/in, and free all ByteBlocks on the host.
 * Starts a backgroud thread which is responsible for disk I/O
 */
class BlockPool : public common::ProfileTask
{
    static constexpr bool debug = false;

public:
    /*!
     * Creates a simple BlockPool for tests: allows only one thread, enforces no
     * memory limitations, never swaps to disk.
     */
    explicit BlockPool(size_t workers_per_host = 1);

    //! non-copyable: delete copy-constructor
    BlockPool(const BlockPool&) = delete;
    //! non-copyable: delete assignment operator
    BlockPool& operator = (const BlockPool&) = delete;

    /*!
     * Creates a BlockPool with given memory constrains
     *
     * \param soft_ram_limit limit (bytes) that causes the BlockPool to swap
     * out victim pages. Enter 0 for no soft limit
     *
     * \param hard_ram_limit limit (bytes) that causes the BlockPool to block
     * new allocations until some blocks are free'd. Enter 0 for no hard limit.
     *
     * \param logger Pointer to logger for output.
     *
     * \param mem_manager Memory Manager that tracks amount of RAM
     * allocated. the BlockPool will create a child manager.
     *
     * \param workers_per_host number of workers on this host.
     */
    BlockPool(size_t soft_ram_limit, size_t hard_ram_limit,
              common::JsonLogger* logger,
              mem::Manager* mem_manager,
              size_t workers_per_host);

    //! Checks that all blocks were freed
    ~BlockPool();

    //! return number of workers per host
    size_t workers_per_host() const { return workers_per_host_; }

    //! Returns logger_
    common::JsonLogger& logger() { return logger_; }

    //! return next unique File id
    size_t next_file_id() { return ++next_file_id_; }

    //! Updates the memory manager for internal memory. If the hard limit is
    //! reached, the call is blocked intil memory is free'd
    void RequestInternalMemory(size_t size);

    //! Updates the memory manager for the internal memory, wakes up waiting
    //! BlockPool::RequestInternalMemory calls
    void ReleaseInternalMemory(size_t size);

    //! Advice the block pool to free up memory in anticipation of a large
    //! future request.
    void AdviseFree(size_t size);

    //! Return any currently being written block (for waiting on completion)
    io::RequestPtr GetAnyWriting();

    //! Evict a Block from the LRU chain into external memory. This can return
    //! nullptr if no blocks available, or if the Block was not dirty.
    io::RequestPtr EvictBlockLRU();

    //! Allocates a byte block with the request size. May block this thread if
    //! the hard memory limit is reached, until memory is freed by another
    //! thread.  The returned Block is allocated in RAM, but with a zero pin
    //! count.
    PinnedByteBlockPtr AllocateByteBlock(size_t size, size_t local_worker_id);

    //! Allocate a byte block from an external file, used to directly map system
    //! files to data::File.
    ByteBlockPtr MapExternalBlock(
        const io::FileBasePtr& file, int64_t offset, size_t size);

    //! Increment a ByteBlock's pin count, requires the pin count to be > 0.
    void IncBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id);

    //! Decrement a ByteBlock's pin count and possibly unpin it.
    void DecBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id);

    //! Destroys the block. Called by ByteBlockPtr's deleter.
    void DestroyBlock(ByteBlock* block_ptr);

    //! Evict a block into external memory. The block must be unpinned and not
    //! swapped.
    void EvictBlock(ByteBlock* block_ptr);

    //! \name Block Statistics
    //! \{

    //! Total number of allocated blocks of this block pool
    size_t total_blocks() noexcept;

    //! Total number of bytes allocated in blocks of this block pool
    size_t total_bytes() noexcept;

    //! Maximum total number of bytes allocated in blocks of this block pool
    size_t max_total_bytes() noexcept;

    //! Total number of pinned blocks of this block pool
    size_t pinned_blocks() noexcept;

    //! Total number of unpinned blocks in memory of this block pool
    size_t unpinned_blocks() noexcept;

    //! Total number of blocks currently begin written.
    size_t writing_blocks() noexcept;

    //! Total number of swapped blocks
    size_t swapped_blocks() noexcept;

    //! Total number of blocks currently begin read from EM.
    size_t reading_blocks() noexcept;

    //! \}

    //! \name Methods for ProfileTask
    //! \{

    void RunTask(const std::chrono::steady_clock::time_point& tp) final;

    //! \}

    //! Pins a block by swapping it in if required.
    PinRequestPtr PinBlock(const Block& block, size_t local_worker_id);

private:
    //! locked before internal state is changed
    std::mutex mutex_;

    //! For waiting on hard memory limit
    std::condition_variable cv_memory_change_;

    //! For waiting on read/pin requests to finish (we use only one
    //! condition_variable for all read requests).
    std::condition_variable cv_read_complete_;

    //! reference to HostContext's logger or a null sink
    common::JsonLogger logger_;

    //! local Manager counting only ByteBlock allocations in internal memory.
    mem::Manager mem_manager_;

    //! Allocator for ByteBlocks such that they are aligned for faster
    //! I/O. Allocations are counted via mem_manager_.
    mem::AlignedAllocator<Byte, mem::Allocator<char> > aligned_alloc_ {
        mem::Allocator<char>(mem_manager_)
    };

    //! reference to io block manager
    io::BlockManager* bm_;

    //! number of workers per host
    size_t workers_per_host_;

    //! next unique File id
    std::atomic<size_t> next_file_id_ { 0 };

    //! a counter pair where one value is held as the max until written to stats
    struct Counter {
        //! current counter value
        size_t value = 0;
        //! maximum counter value since last stats read
        size_t hmax = 0;

        operator size_t () const { return value; }

        Counter& operator += (size_t v) {
            value += v;
            hmax = std::max(hmax, value);
            return *this;
        }
        Counter& operator -= (size_t v) {
            value -= v;
            return *this;
        }

        //! get last held max value and update to current
        size_t hmax_update() {
            size_t m = hmax;
            hmax = value;
            return m;
        }
    };

    struct PinCount {
        //! current total number of pins, where each thread pin counts
        //! individually.
        size_t              total_pins_ = 0;

        //! total number of bytes pinned.
        Counter             total_pinned_bytes_;

        //! maximum number of total pins
        size_t              max_pins = 0;

        //! maximum number of pinned bytes
        size_t              max_pinned_bytes = 0;

        //! number of pinned blocks per local worker id - this is used to count
        //! the amount of memory locked per thread.
        std::vector<size_t> pin_count_;

        //! number of bytes pinned per local worker id.
        std::vector<size_t> pinned_bytes_;

        //! ctor: initializes vectors to correct size.
        explicit PinCount(size_t workers_per_host);

        //! increment pin counter for thread_id by given size in bytes
        void                Increment(size_t local_worker_id, size_t size);

        //! decrement pin counter for thread_id by given size in bytes
        void                Decrement(size_t local_worker_id, size_t size);

        //! assert that it is zero.
        void                AssertZero() const;
    };

    //! number of unpinned bytes
    Counter unpinned_bytes_;

    //! pin counter class
    PinCount pin_count_;

    //! pimpl data structure
    struct Data;

    //! pimpl data structure
    std::unique_ptr<Data> d_;

    //! number of bytes currently begin requested from RAM.
    size_t requested_bytes_ = 0;

    //! number of bytes currently being written to EM.
    Counter writing_bytes_;

    //! total number of bytes in swapped blocks
    Counter swapped_bytes_;

    //! number of bytes currently being read from to EM.
    Counter reading_bytes_;

    //! total number of ByteBlocks allocated
    size_t total_byte_blocks_ = 0;

    //! condition variable to wait on for ByteBlock deallocation
    std::condition_variable cv_total_byte_blocks_;

    //! total number of bytes in all ByteBlocks (in memory or swapped)
    Counter total_bytes_;

    //! maximum number of bytes in all ByteBlocks (in memory or swapped)
    size_t max_total_bytes_ = 0;

    //! total number of bytes used in RAM by pinned and unpinned blocks, and
    //! also additionally reserved memory via BlockPoolMemoryHolder.
    Counter total_ram_bytes_;

    //! Soft limit for the block pool, blocks will be written to disk if this
    //! limit is reached. 0 for no limit.
    size_t soft_ram_limit_;

    //! Hard limit for the block pool, memory requests will block if this limit
    //! is reached. 0 for no limit.
    size_t hard_ram_limit_;

    //! last time statistics where outputted
    std::chrono::steady_clock::time_point tp_last_;

    //! Updates the memory manager for internal memory. If the hard limit is
    //! reached, the call is blocked intil memory is free'd
    void IntRequestInternalMemory(std::unique_lock<std::mutex>& lock, size_t size);

    //! Updates the memory manager for the internal memory, wakes up waiting
    //! BlockPool::RequestInternalMemory calls
    void IntReleaseInternalMemory(size_t size);

    //! Increment a ByteBlock's pin count - without locking the mutex
    void IntIncBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id);

    //! Unpins a block. If all pins are removed, the block might be swapped.
    //! Returns immediately. Actual unpinning is async.
    void IntUnpinBlock(ByteBlock* block_ptr, size_t local_worker_id);

    //! callback for async write of blocks during eviction
    void OnWriteComplete(ByteBlock* block_ptr, io::Request* req, bool success);

    //! callback for async read of blocks for pin requests
    void OnReadComplete(PinRequest* read, io::Request* req, bool success);

    //! Evict a block from the lru list into external memory
    io::RequestPtr IntEvictBlockLRU();

    //! Evict a block into external memory. The block must be unpinned and not
    //! swapped.
    io::RequestPtr IntEvictBlock(ByteBlock* block_ptr);

    //! make ostream-able
    friend std::ostream& operator << (std::ostream& os, const PinCount& p);

    //! for calling OnWriteComplete
    friend class ByteBlock;

    //! for calling OnReadComplete and access to mutex and cvs
    friend class PinRequest;

    //! \name Block Statistics
    //! \{

    //! Total number of allocated blocks of this block pool
    size_t int_total_blocks()  noexcept;

    //! Total number of bytes allocated in blocks of this block pool
    size_t int_total_bytes()  noexcept;

    //! \}
};

/*!
 * RAII class for allocating memory from a BlockPool
 */
class BlockPoolMemoryHolder
{
public:
    BlockPoolMemoryHolder(BlockPool& block_pool, size_t size)
        : block_pool_(block_pool), size_(size) {
        if (size)
            block_pool_.RequestInternalMemory(size);
    }

    //! non-copyable: delete copy-constructor
    BlockPoolMemoryHolder(const BlockPoolMemoryHolder&) = delete;
    //! non-copyable: delete assignment operator
    BlockPoolMemoryHolder& operator = (const BlockPoolMemoryHolder&) = delete;

    ~BlockPoolMemoryHolder() {
        if (size_)
            block_pool_.ReleaseInternalMemory(size_);
    }

private:
    BlockPool& block_pool_;
    size_t size_;
};

//! \}

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
