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
#include <thrill/io/block_alloc.hpp>
#include <thrill/io/config_file.hpp>
#include <thrill/io/create_file.hpp>
#include <thrill/io/disk_allocator.hpp>
#include <thrill/io/file.hpp>
#include <thrill/io/request.hpp>

#include <algorithm>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#if STXXL_MSVC
#include <memory.h>
#endif

namespace thrill {
namespace io {

#ifndef STXXL_MNG_COUNT_ALLOCATION
#define STXXL_MNG_COUNT_ALLOCATION 1
#endif  // STXXL_MNG_COUNT_ALLOCATION

//! \addtogroup mnglayer
//! \{

//! Block manager class.
//!
//! Manages allocation and deallocation of blocks in multiple/single disk setting
//! \remarks is a singleton
class block_manager : public singleton<block_manager>
{
    static const bool debug = false;

    friend class singleton<block_manager>;

    disk_allocator** disk_allocators;
    file** disk_files;

    size_t ndisks;
    block_manager();

#if STXXL_MNG_COUNT_ALLOCATION
    //! total requested allocation in bytes
    uint64_t m_total_allocation;

    //! currently allocated bytes
    uint64_t m_current_allocation;

    //! maximum number of bytes allocated during program run.
    uint64_t m_maximum_allocation;
#endif      // STXXL_MNG_COUNT_ALLOCATION

protected:
    template <class BIDType, class DiskAssignFunctor, class BIDIteratorClass>
    void new_blocks_int(
        const size_t nblocks,
        const DiskAssignFunctor& functor,
        size_t offset,
        BIDIteratorClass out);

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
    template <class DiskAssignFunctor, class BIDIteratorClass>
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
    template <class BlockType, class DiskAssignFunctor, class BIDIteratorClass>
    void new_blocks(
        const size_t nblocks,
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
    template <typename DiskAssignFunctor, unsigned BLK_SIZE>
    void new_block(const DiskAssignFunctor& functor, BID<BLK_SIZE>& bid, size_t offset = 0) {
        new_blocks_int<BID<BLK_SIZE> >(1, functor, offset, &bid);
    }

    //! Deallocates blocks.
    //!
    //! Deallocates blocks in the range [ \b bidbegin, \b bidend)
    //! \param bidbegin iterator object of \b bid_iterator concept
    //! \param bidend iterator object of \b bid_iterator concept
    template <class BIDIteratorClass>
    void delete_blocks(const BIDIteratorClass& bidbegin, const BIDIteratorClass& bidend);

    //! Deallocates a block.
    //! \param bid block identifier
    template <unsigned BLK_SIZE>
    void delete_block(const BID<BLK_SIZE>& bid);

    ~block_manager();

#if STXXL_MNG_COUNT_ALLOCATION
    //! return total requested allocation in bytes
    uint64_t get_total_allocation() const
    { return m_total_allocation; }

    //! return currently allocated bytes
    uint64_t get_current_allocation() const
    { return m_current_allocation; }

    //! return maximum number of bytes allocated during program run.
    uint64_t get_maximum_allocation() const
    { return m_maximum_allocation; }
#endif      // STXXL_MNG_COUNT_ALLOCATION
};

template <class BIDType, class DiskAssignFunctor, class OutputIterator>
void block_manager::new_blocks_int(
    const size_t nblocks,
    const DiskAssignFunctor& functor,
    size_t offset,
    OutputIterator out) {
    using bid_type = BIDType;
    using bid_array_type = BIDArray<bid_type::t_size>;

    std::vector<int> bl(ndisks, 0);
    std::vector<bid_array_type> disk_bids(ndisks);
    std::vector<file*> disk_ptrs(nblocks);

    // choose disks by calling DiskAssignFunctor

    for (size_t i = 0; i < nblocks; ++i)
    {
        size_t disk = functor(offset + i);
        disk_ptrs[i] = disk_files[disk];
        bl[disk]++;
    }

    // allocate blocks on disks

    for (size_t i = 0; i < ndisks; ++i)
    {
        if (bl[i])
        {
            disk_bids[i].resize(bl[i]);
            disk_allocators[i]->new_blocks(disk_bids[i]);
        }
    }

    std::fill(bl.begin(), bl.end(), 0);

    OutputIterator it = out;
    for (size_t i = 0; i != nblocks; ++it, ++i)
    {
        const int disk = disk_ptrs[i]->get_allocator_id();
        bid_type bid(disk_ptrs[i], disk_bids[disk][bl[disk]++].offset);
        *it = bid;
        LOG0 << "BLC:new    " << bid;
    }

#if STXXL_MNG_COUNT_ALLOCATION
    m_total_allocation += nblocks * BIDType::size;
    m_current_allocation += nblocks * BIDType::size;
    m_maximum_allocation = std::max(m_maximum_allocation, m_current_allocation);
#endif      // STXXL_MNG_COUNT_ALLOCATION
}

template <unsigned BlockSize>
void block_manager::delete_block(const BID<BlockSize>& bid) {
    if (!bid.valid()) {
        // STXXL_MSG("Warning: invalid block to be deleted.");
        return;
    }
    if (!bid.is_managed())
        return;  // self managed disk
    LOG0 << "BLC:delete " << bid;
    assert(bid.storage->get_allocator_id() >= 0);
    disk_allocators[bid.storage->get_allocator_id()]->delete_block(bid);
    disk_files[bid.storage->get_allocator_id()]->discard(bid.offset, bid.size);

#if STXXL_MNG_COUNT_ALLOCATION
    m_current_allocation -= BlockSize;
#endif      // STXXL_MNG_COUNT_ALLOCATION
}

template <class BIDIteratorClass>
void block_manager::delete_blocks(
    const BIDIteratorClass& bidbegin,
    const BIDIteratorClass& bidend) {
    for (BIDIteratorClass it = bidbegin; it != bidend; it++)
    {
        delete_block(*it);
    }
}

// in bytes
#ifndef STXXL_DEFAULT_BLOCK_SIZE
    #define STXXL_DEFAULT_BLOCK_SIZE(type) (2 * 1024 * 1024) // use traits
#endif

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_BLOCK_MANAGER_HEADER

/******************************************************************************/
