/*******************************************************************************
 * thrill/data/block_pool.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_POOL_HEADER
#define THRILL_DATA_BLOCK_POOL_HEADER

#include <thrill/common/lru_cache.hpp>
#include <thrill/common/signal.hpp>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/block.hpp>
#include <thrill/data/byte_block.hpp>
#include <thrill/io/block_manager.hpp>
#include <thrill/io/request.hpp>
#include <thrill/mem/manager.hpp>

#include <deque>
#include <future>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

namespace thrill {
namespace data {

// forward declarations
class Block;
class PinnedBlock;

/*!
 * Pool to allocate, keep, swap out/in, and free all ByteBlocks on the host.
 * Starts a backgroud thread which is responsible for disk I/O
 */
class BlockPool
{
    static const bool debug = false;

public:
    /*!
     * Creates a simple BlockPool for tests: allows only one thread, enforces no
     * memory limitations, never swaps to disk.
     */
    explicit BlockPool(size_t workers_per_host = 1);

    /*!
     * Creates a BlockPool with given memory constrains
     *
     * \param soft_ram_limit limit (bytes) that causes the BlockPool to swap
     * out victim pages. Enter 0 for no soft limit
     *
     * \param hard_ram_limit limit (bytes) that causes the BlockPool to block
     * new allocations until some blocks are free'd. Enter 0 for no hard limit.
     *
     * \param mem_manager Memory Manager that tracks amount of RAM
     * allocated. the BlockPool will create a child manager.
     *
     * \param workers_per_host number of workers on this host.
     */
    BlockPool(size_t soft_ram_limit, size_t hard_ram_limit,
              mem::Manager* mem_manager,
              size_t workers_per_host);

    //! Checks that all blocks were freed
    ~BlockPool();

    //! return number of workers per host
    size_t workers_per_host() const { return workers_per_host_; }

    //! Allocates a byte block with the request size. May block this thread if
    //! the hard memory limit is reached, until memory is freed by another
    //! thread.  The returned Block is allocated in RAM, but with a zero pin
    //! count.
    PinnedByteBlockPtr AllocateByteBlock(size_t size, size_t local_worker_id);

    //! Pins a block by swapping it in if required.
    std::future<PinnedBlock> PinBlock(const Block& block, size_t local_worker_id);

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
    size_t total_blocks()  noexcept;

    //! Total number of bytes allocated in blocks of this block pool
    size_t total_bytes()  noexcept;

    //! Total number of pinned blocks of this block pool
    size_t pinned_blocks()  noexcept;

    //! Total number of unpinned blocks in memory of this block pool
    size_t unpinned_blocks()  noexcept;

    //! Total number of blocks currently begin written.
    size_t writing_blocks()  noexcept;

    //! Total number of swapped blocks
    size_t swapped_blocks()  noexcept;

    //! Total number of blocks currently begin read from EM.
    size_t reading_blocks()  noexcept;

    //! \}

private:
    //! locked before internal state is changed
    std::mutex mutex_;

    //! For waiting on hard memory limit
    std::condition_variable cv_memory_change_;

    //! local Manager counting only ByteBlock allocations in internal memory.
    mem::Manager mem_manager_;
    //! reference to io block manager
    io::BlockManager* bm_;

    //! number of workers per host
    size_t workers_per_host_;

    //! list of all blocks that are _in_memory_ but are _not_ pinned.
    common::LruCacheSet<ByteBlock*> unpinned_blocks_;

    //! number of unpinned bytes
    size_t unpinned_bytes_ = 0;

    struct PinCount
    {
        //! current total number of pins, where each thread pin counts
        //! individually.
        size_t              total_pins_ = 0;

        //! total number of bytes pinned.
        size_t              total_pinned_bytes_ = 0;

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

    //! pin counter class
    PinCount pin_count_;

    //! set of ByteBlocks currently begin written to EM.
    std::unordered_map<ByteBlock*, io::RequestPtr> writing_;

    //! number of bytes currently begin requested from RAM.
    size_t requested_bytes_ = 0;

    //! number of bytes currently being written to EM.
    size_t writing_bytes_ = 0;

    //! set of ByteBlock currently in EM.
    std::unordered_set<ByteBlock*> swapped_;

    //! total number of bytes in swapped blocks
    size_t swapped_bytes_ = 0;

    struct ReadRequest
    {
        std::promise<PinnedBlock> result;
        Byte                      * data;
        io::RequestPtr            req;
    };

    //! set of ByteBlocks currently begin read from EM.
    std::unordered_map<ByteBlock*, ReadRequest> reading_;

    //! number of bytes currently being read from to EM.
    size_t reading_bytes_ = 0;

    //! total number of bytes used in RAM by pinned and unpinned blocks.
    size_t total_ram_use_ = 0;

    //! Soft limit for the block pool, blocks will be written to disk if this
    //! limit is reached. 0 for no limit.
    size_t soft_ram_limit_;

    //! Hard limit for the block pool, memory requests will block if this limit
    //! is reached. 0 for no limit.
    size_t hard_ram_limit_;

    //! Updates the memory manager for internal memory. If the hard limit is
    //! reached, the call is blocked intil memory is free'd
    void RequestInternalMemory(std::unique_lock<std::mutex>& lock, size_t size);

    //! Updates the memory manager for the internal memory, wakes up waiting
    //! BlockPool::RequestInternalMemory calls
    void ReleaseInternalMemory(size_t size);

    //! Increment a ByteBlock's pin count - without locking the mutex
    void IncBlockPinCountNoLock(ByteBlock* block_ptr, size_t local_worker_id);

    //! Unpins a block. If all pins are removed, the block might be swapped.
    //! Returns immediately. Actual unpinning is async.
    void UnpinBlock(ByteBlock* block_ptr, size_t local_worker_id);

    //! callback for async write of blocks during eviction
    void OnWriteComplete(ByteBlock* block_ptr, io::Request* req, bool success);

    //! callback for async read of blocks for pin requests
    void OnReadComplete(
        const Block& block, size_t local_worker_id, ReadRequest* read,
        io::Request* req, bool success);

    //! Evict a block from the lru list into external memory
    void EvictBlockLRU();

    //! Evict a block into external memory. The block must be unpinned and not
    //! swapped.
    void EvictBlockNoLock(ByteBlock* block_ptr);

    //! make ostream-able
    friend std::ostream& operator << (std::ostream& os, const PinCount& p);

    //! \name Block Statistics
    //! \{

    //! Total number of allocated blocks of this block pool
    size_t total_blocks_nolock()  noexcept;

    //! Total number of bytes allocated in blocks of this block pool
    size_t total_bytes_nolock()  noexcept;

    //! \}
};

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
