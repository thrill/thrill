/*******************************************************************************
 * thrill/data/block_pool.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_DATA_BLOCK_POOL_HEADER
#define THRILL_DATA_BLOCK_POOL_HEADER

#include <thrill/common/signal.hpp>
#include <thrill/common/thread_pool.hpp>
#include <thrill/data/byte_block.hpp>
#include <thrill/mem/manager.hpp>
#include <thrill/mem/page_mapper.hpp>

#include <deque>
#include <mutex>

namespace thrill {
namespace data {

/*!
 * Pool to allocate, keep, swap out/in, and free all ByteBlocks on the host.
 * Starts a backgroud thread which is responsible for disk I/O
 */
class BlockPool
{
    static const bool debug = false;

public:
    /*!
     * Creates a BlockPool without memory limitations
     *
     * \param mem_manager Memory Manager that tracks amount of RAM
     * allocated. the BlockPool will create a child manager.
     *
     * \param mem_manager_external Memory Manager that tracks amount of memory
     * allocated on disk. The BlockPool will create a child manager.
     *
     * \param swap_file_suffix suffix to append on file nam of the swap file
     */
    explicit BlockPool(mem::Manager* mem_manager,
                       mem::Manager* mem_manager_external,
                       std::string swap_file_suffix = "")
        : BlockPool(0, 0, mem_manager, mem_manager_external, swap_file_suffix)
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
     * \param swap_file_suffix suffix to append on file nam of the swap file
     */
    explicit BlockPool(size_t soft_memory_limit, size_t hard_memory_limit,
                       mem::Manager* mem_manager,
                       mem::Manager* mem_manager_external,
                       std::string swap_file_suffix = "")
        : mem_manager_(mem_manager, "BlockPool"),
          ext_mem_manager_(mem_manager_external, "BlockPool"),
          page_mapper_("/tmp/thrill.swapfile" + swap_file_suffix),
          soft_memory_limit_(soft_memory_limit),
          hard_memory_limit_(hard_memory_limit) {
        tasks_.Enqueue([]() {
                           common::NameThisThread("I/O");
                       });
    }

    ~BlockPool() {
        sLOG << "waiting for I/O background thread to end";
        tasks_.LoopUntilEmpty();
    }

    //! Allocates a block the size that is requested
    //! Maybe blocks if the hard memory limit is reached
    //! \param swapable indicates whether the block can be swapped to disk
    //! pinned indicates whether the block :q
    //
    ByteBlockPtr AllocateBlock(size_t block_size, bool swapable = true, bool pinned = false);

    //! Total number of allocated blocks of this block pool
    size_t block_count() const noexcept;

protected:
    //! local Manager counting only ByteBlock allocations in internal memory.
    mem::Manager mem_manager_;

    //! local Manager counting only ByteBlocks in external memory.
    mem::Manager ext_mem_manager_;

    //! PageMapper used for swapping-in/-out blocks
    mem::PageMapper<default_block_size> page_mapper_;

    // list of all blocks that are not swapped but are not pinned
    std::deque<ByteBlock*> victim_blocks_;

    size_t num_swapped_blocks_ = { 0 };
    size_t num_pinned_blocks_ = { 0 };

    //! locked before internal state is changed
    std::mutex list_mutex_;
    std::mutex pin_mutex_;

    //! For implementing hard limit
    std::mutex memory_mutex_;

    //! For waiting on hard memory limit
    std::condition_variable memory_change_;

    //! ThreadPool used for I/O tasks
    common::ThreadPool tasks_ { 1 };

    //! Limits for the block pool. 0 for no limits.
    size_t soft_memory_limit_, hard_memory_limit_;

    // for calling [Un]PinBlock
    friend class ByteBlock;

    //! Unpins a block. If all pins are removed, the block might be swapped.
    //! Returns immediately. Actual unpinning is async.
    //! out to disk and is not accessible
    //! \param block_ptr the block to unpin
    void UnpinBlock(ByteBlock* block_ptr);

    //! Pins a block by swapping it in if required.
    //! \param block_ptr the block to pin
    //! \param callback is called when the pinning is completed
    void PinBlock(ByteBlock* block_ptr, common::delegate<void()>&& callback);

    //! Destroys the block. Only for internal purposes.
    //! Async call.
    //! Async call.
    void DestroyBlock(ByteBlock* block);

    //! Updates the memory manager for internal memory
    //! If the hard limit is reached, the call is blocked intil
    //! memory is free'd
    inline void RequestInternalMemory(size_t amount);

    //! Updates the memory manager for the internal memory
    //! wakes up waiting BlockPool::RequestInternalMemory calls
    inline void ReleaseInternalMemory(size_t amount);
};

} // namespace data
} // namespace thrill
#endif // !THRILL_DATA_BLOCK_POOL_HEADER

/******************************************************************************/
