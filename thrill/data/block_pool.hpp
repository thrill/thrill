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
#include <thrill/data/byte_block.hpp>
#include <thrill/mem/manager.hpp>
#include <thrill/mem/page_mapper.hpp>

#include <deque>
#include <future>
#include <mutex>
#include <string>
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
    explicit BlockPool(size_t workers_per_host = 1)
        : BlockPool(0, 0, nullptr, nullptr, workers_per_host)
    { }

    /*!
     * Creates a BlockPool with given memory constrains
     *
     * \param soft_memory_limit limit (bytes) that causes the BlockPool to swap
     * out victim pages. Enter 0 for no soft limit
     *
     * \param hard_memory_limit limit (bytes) that causes the BlockPool to block
     * new allocations until some blocks are free'd. Enter 0 for no hard limit.
     *
     * \param mem_manager Memory Manager that tracks amount of RAM
     * allocated. the BlockPool will create a child manager.
     *
     * \param mem_manager_external Memory Manager that tracks amount of memory
     * allocated on disk. The BlockPool will create a child manager.
     *
     * \param workers_per_host number of workers on this host.
     */
    BlockPool(size_t soft_memory_limit, size_t hard_memory_limit,
              mem::Manager* mem_manager,
              mem::Manager* mem_manager_external,
              size_t workers_per_host)
        : mem_manager_(mem_manager, "BlockPool"),
          ext_mem_manager_(mem_manager_external, "BlockPoolEM"),
          workers_per_host_(workers_per_host),
          pin_count_(workers_per_host),
          pinned_bytes_(workers_per_host),
          soft_memory_limit_(soft_memory_limit),
          hard_memory_limit_(hard_memory_limit)
    { }

    //! Checks that all blocks were freed
    ~BlockPool();

    //! return number of workers per host
    size_t workers_per_host() const { return workers_per_host_; }

    //! Allocates a byte block with the request size. May block this thread if
    //! the hard memory limit is reached, until memory is freed by another
    //! thread.  The returned Block is allocated in RAM, but with a zero pin
    //! count.
    PinnedByteBlockPtr AllocateByteBlock(size_t size, size_t local_worker_id);

    //! Total number of allocated blocks of this block pool
    size_t block_count() const noexcept;

    //! Pins a block by swapping it in if required.
    //! \param block_ptr the block to pin
    std::future<PinnedBlock> PinBlock(const Block& block, size_t local_worker_id);

    //! Increment a ByteBlock's pin count, requires the pin count to be > 0.
    void IncBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id);

    //! Decrement a ByteBlock's pin count and possibly unpin it.
    void DecBlockPinCount(ByteBlock* block_ptr, size_t local_worker_id);

    //! Destroys the block. Called by ByteBlockPtr's deleter.
    void DestroyBlock(ByteBlock* block);

private:
    //! local Manager counting only ByteBlock allocations in internal memory.
    mem::Manager mem_manager_;

    //! local Manager counting only ByteBlocks in external memory.
    mem::Manager ext_mem_manager_;

    //! number of workers per host
    size_t workers_per_host_;

    //! list of all blocks that are _in_memory_ but are _not_ pinned. TODO(tb):
    //! probably not the right data structure.
    common::LruCacheSet<ByteBlock*> unpinned_blocks_;

    size_t num_swapped_blocks_ = 0;

    //! current total number of pins, where each thread pin counts individually.
    size_t total_pins_ = 0;

    //! number of pinned blocks per local worker id - this is used to count the
    //! amount of memory locked per thread.
    std::vector<size_t> pin_count_;

    //! number of bytes pinned per local worker id.
    std::vector<size_t> pinned_bytes_;

    //! locked before internal state is changed
    std::mutex mutex_;

    //! total number of bytes used in RAM by pinned and unpinned blocks.
    size_t total_ram_use_ = 0;
    size_t total_ram_limit_ = 10240000lu;

    //! For implementing hard limit
    std::mutex memory_mutex_;

    //! For waiting on hard memory limit
    std::condition_variable memory_change_;

    //! Limits for the block pool. 0 for no limits.
    size_t soft_memory_limit_, hard_memory_limit_;

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
    //! \param block_ptr the block to unpin
    void UnpinBlock(ByteBlock* block_ptr, size_t local_worker_id);

    //! Evict a block into external memory
    void EvictBlock(ByteBlock* block_ptr);
};

} // namespace data
} // namespace thrill

#endif // !THRILL_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
