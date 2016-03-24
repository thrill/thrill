/*******************************************************************************
 * thrill/io/disk_allocator.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2004 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2007 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2009, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013-2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_DISK_ALLOCATOR_HEADER
#define THRILL_IO_DISK_ALLOCATOR_HEADER

#include <thrill/io/bid.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/mem/pool.hpp>

#include <algorithm>
#include <cassert>
#include <functional>
#include <mutex>
#include <ostream>
#include <utility>

namespace thrill {
namespace io {

class DiskConfig;

//! \ingroup io_layer
//! \{

class DiskAllocator
{
    static constexpr bool debug = false;

    //! pimpl data struct containing std::map
    struct Data;

    std::mutex mutex_;
    std::unique_ptr<Data> data_;
    int64_t free_bytes_;
    int64_t disk_bytes_;
    int64_t cfg_bytes_;
    FileBase* storage_;
    bool autogrow_;

    void Dump() const;

    template <typename SortSeqIterator>
    void DeallocationError(
        int64_t block_pos, int64_t block_size,
        const SortSeqIterator& pred, const SortSeqIterator& succ) const;

    // expects the mutex to be locked to prevent concurrent access
    void AddFreeRegion(int64_t block_pos, int64_t block_size);

    // expects the mutex to be locked to prevent concurrent access
    void GrowFile(int64_t extend_bytes) {
        if (!extend_bytes)
            return;

        storage_->set_size(disk_bytes_ + extend_bytes);
        AddFreeRegion(disk_bytes_, extend_bytes);
        disk_bytes_ += extend_bytes;
    }

public:
    DiskAllocator(FileBase* storage, const DiskConfig& cfg);

    //! non-copyable: delete copy-constructor
    DiskAllocator(const DiskAllocator&) = delete;
    //! non-copyable: delete assignment operator
    DiskAllocator& operator = (const DiskAllocator&) = delete;

    ~DiskAllocator();

    int64_t free_bytes() const {
        return free_bytes_;
    }

    int64_t used_bytes() const {
        return disk_bytes_ - free_bytes_;
    }

    int64_t total_bytes() const {
        return disk_bytes_;
    }

    template <size_t BlockSize>
    void NewBlocks(BIDArray<BlockSize>& bids) {
        NewBlocks<BlockSize>(bids.begin(), bids.end());
    }

    template <typename BidIterator>
    void NewBlocks(BidIterator begin, BidIterator end);

    template <size_t BlockSize>
    void DeleteBlock(const BID<BlockSize>& bid);
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_DISK_ALLOCATOR_HEADER

/******************************************************************************/
