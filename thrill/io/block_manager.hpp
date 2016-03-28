/*******************************************************************************
 * thrill/io/block_manager.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2007 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2007, 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2008-2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_BLOCK_MANAGER_HEADER
#define THRILL_IO_BLOCK_MANAGER_HEADER

#include <thrill/common/config.hpp>
#include <thrill/common/defines.hpp>
#include <thrill/io/bid.hpp>
#include <thrill/io/block_alloc_strategy.hpp>
#include <thrill/io/config_file.hpp>
#include <thrill/io/create_file.hpp>
#include <thrill/io/disk_allocator.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/io/request.hpp>

#include <algorithm>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#if THRILL_MSVC
#include <memory.h>
#endif

namespace thrill {
namespace io {

//! \addtogroup io_layer
//! \{

//! Block manager class.
//!
//! Manages allocation and deallocation of blocks in multiple/single disk setting
//! \remarks is a singleton
class BlockManager : public common::Singleton<BlockManager>
{
    static constexpr bool debug = false;

public:
    //! return total number of bytes available in all disks
    uint64_t get_total_bytes() const;

    //! Return total number of free disk allocations
    uint64_t get_free_bytes() const;

    //! Allocates new blocks.
    //!
    //! Allocates new blocks according to the strategy
    //! given by \b functor and stores block identifiers
    //! to the range [ \b bidbegin, \b bidend)
    //! Allocation will be lined up with previous partial allocations
    //! of \b offset blocks.
    //! \param functor object of model of \b allocation_strategy concept
    //! \param bidbegin bidirectional BID iterator object
    //! \param bidend bidirectional BID iterator object
    //! \param offset advance for \b functor to line up partial allocations
    template <typename DiskAssignFunctor, typename BIDIteratorClass>
    void new_blocks(
        const DiskAssignFunctor& functor,
        BIDIteratorClass bidbegin,
        BIDIteratorClass bidend,
        size_t offset = 0) {
        using bid_type = typename std::iterator_traits<BIDIteratorClass>::value_type;
        new_blocks_int<bid_type>(std::distance(bidbegin, bidend), functor, offset, bidbegin);
    }

    //! Allocates new blocks according to the strategy
    //! given by \b functor and stores block identifiers
    //! to the output iterator \b out
    //! Allocation will be lined up with previous partial allocations
    //! of \b offset blocks.
    //! \param nblocks the number of blocks to allocate
    //! \param functor object of model of \b allocation_strategy concept
    //! \param out iterator object of OutputIterator concept
    //! \param offset advance for \b functor to line up partial allocations
    //!
    //! The \c BlockType template parameter defines the type of block to allocate
    template <typename BlockType, typename DiskAssignFunctor, typename BIDIteratorClass>
    void new_blocks(const size_t nblocks,
                    const DiskAssignFunctor& functor,
                    BIDIteratorClass out,
                    size_t offset = 0) {
        using bid_type = typename BlockType::bid_type;
        new_blocks_int<bid_type>(nblocks, functor, offset, out);
    }

    //! Allocates a new block according to the strategy
    //! given by \b functor and stores the block identifier
    //! to bid.
    //! Allocation will be lined up with previous partial allocations
    //! of \b offset blocks.
    //! \param functor object of model of \b allocation_strategy concept
    //! \param bid BID to store the block identifier
    //! \param offset advance for \b functor to line up partial allocations
    template <typename DiskAssignFunctor, size_t BlockSize>
    void new_block(
        const DiskAssignFunctor& functor, BID<BlockSize>& bid, size_t offset = 0) {
        new_blocks_int<BID<BlockSize> >(1, functor, offset, &bid);
    }

    //! Deallocates blocks.
    //!
    //! Deallocates blocks in the range [ \b bidbegin, \b bidend)
    //! \param bid_begin iterator object of \b bid_iterator concept
    //! \param bid_end iterator object of \b bid_iterator concept
    template <typename BIDIteratorClass>
    void delete_blocks(
        const BIDIteratorClass& bid_begin, const BIDIteratorClass& bid_end) {
        for (BIDIteratorClass it = bid_begin; it != bid_end; it++)
            delete_block(*it);
    }

    //! Deallocates a block.
    //! \param bid block identifier
    template <size_t BlockSize>
    void delete_block(const BID<BlockSize>& bid);

    ~BlockManager();

    //! return total requested allocation in bytes
    uint64_t total_allocation() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return total_allocation_;
    }

    //! return currently allocated bytes
    uint64_t current_allocation() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return current_allocation_;
    }

    //! return maximum number of bytes allocated during program run.
    uint64_t maximum_allocation() const {
        std::unique_lock<std::mutex> lock(mutex_);
        return maximum_allocation_;
    }

protected:
    template <typename BIDType, typename DiskAssignFunctor, typename BIDIteratorClass>
    void new_blocks_int(
        const size_t nblocks, const DiskAssignFunctor& functor,
        size_t offset, BIDIteratorClass out);

private:
    friend class common::Singleton<BlockManager>;

    std::vector<DiskAllocator*> disk_allocators_;
    std::vector<FileBasePtr> disk_files_;

    size_t ndisks_;
    BlockManager();

    mutable std::mutex mutex_;

    //! total requested allocation in bytes
    uint64_t total_allocation_ = 0;

    //! currently allocated bytes
    uint64_t current_allocation_ = 0;

    //! maximum number of bytes allocated during program run.
    uint64_t maximum_allocation_ = 0;
};

template <typename BIDType, typename DiskAssignFunctor, typename OutputIterator>
void BlockManager::new_blocks_int(
    const size_t nblocks, const DiskAssignFunctor& functor,
    size_t offset, OutputIterator out) {

    std::unique_lock<std::mutex> lock(mutex_);

    OutputIterator it = out;
    for (size_t i = 0; i != nblocks; ++i, ++it)
    {
        size_t disk_id;
        FileBase* disk_file;
        DiskAllocator* disk_alloc;

        for (size_t retry = 0; retry < 100; ++retry) {
            // choose disk by calling DiskAssignFunctor
            disk_id = functor(offset + i);

            disk_file = disk_files_[disk_id];
            disk_alloc = disk_allocators_[disk_file->get_allocator_id()];

            // check if disk has enough free space
            if (disk_alloc->free_bytes() >= static_cast<int64_t>(it->size))
                break;
        }

        // if no disk has free space, pick an arbitrary one after 100 rounds.

        it->storage = disk_file;
        disk_alloc->NewBlocks(it, it + 1);
        LOG0 << "BLC:new    " << *it;

        total_allocation_ += it->size;
        current_allocation_ += it->size;
    }

    maximum_allocation_ = std::max(maximum_allocation_, current_allocation_);
}

template <size_t BlockSize>
void BlockManager::delete_block(const BID<BlockSize>& bid) {
    if (!bid.valid()) {
        // THRILL_MSG("Warning: invalid block to be deleted.");
        return;
    }
    if (!bid.is_managed())
        return;  // self managed disk

    std::unique_lock<std::mutex> lock(mutex_);

    LOG0 << "BLC:delete " << bid;
    assert(bid.storage->get_allocator_id() >= 0);
    disk_allocators_[bid.storage->get_allocator_id()]->DeleteBlock(bid);
    disk_files_[bid.storage->get_allocator_id()]->discard(bid.offset, bid.size);

    current_allocation_ -= BlockSize;
}

// in bytes
#ifndef THRILL_DEFAULT_BLOCK_SIZE
    #define THRILL_DEFAULT_BLOCK_SIZE(type) (2 * 1024 * 1024) // use traits
#endif

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_BLOCK_MANAGER_HEADER

/******************************************************************************/
