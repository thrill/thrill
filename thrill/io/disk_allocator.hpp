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
#include <thrill/io/config_file.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/file_base.hpp>
#include <thrill/mem/pool.hpp>

#include <algorithm>
#include <cassert>
#include <functional>
#include <map>
#include <mutex>
#include <ostream>
#include <utility>

namespace thrill {
namespace io {

//! \ingroup io_layer
//! \{

class DiskAllocator
{
    static constexpr bool debug = false;

    using Place = std::pair<int64_t, int64_t>;

    struct FirstFit : public std::binary_function<Place, int64_t, bool>
    {
        bool operator () (
            const Place& entry,
            const int64_t size) const {
            return (entry.second >= size);
        }
    };

    using SortSeq = std::map<
              int64_t, int64_t, std::less<int64_t>,
              mem::GPoolAllocator<std::pair<const int64_t, int64_t> > >;

    std::mutex mutex_;
    SortSeq free_space_;
    int64_t free_bytes_;
    int64_t disk_bytes_;
    int64_t cfg_bytes_;
    FileBase* storage_;
    bool autogrow_;

    void dump() const;

    void deallocation_error(
        int64_t block_pos, int64_t block_size,
        const SortSeq::iterator& pred, const SortSeq::iterator& succ) const;

    // expects the mutex to be locked to prevent concurrent access
    void add_free_region(int64_t block_pos, int64_t block_size);

    // expects the mutex to be locked to prevent concurrent access
    void grow_file(int64_t extend_bytes) {
        if (!extend_bytes)
            return;

        storage_->set_size(disk_bytes_ + extend_bytes);
        add_free_region(disk_bytes_, extend_bytes);
        disk_bytes_ += extend_bytes;
    }

public:
    DiskAllocator(FileBase* storage, const DiskConfig& cfg)
        : free_bytes_(0),
          disk_bytes_(0),
          cfg_bytes_(cfg.size),
          storage_(storage),
          autogrow_(cfg.autogrow) {
        // initial growth to configured file size
        grow_file(cfg.size);
    }

    //! non-copyable: delete copy-constructor
    DiskAllocator(const DiskAllocator&) = delete;
    //! non-copyable: delete assignment operator
    DiskAllocator& operator = (const DiskAllocator&) = delete;

    ~DiskAllocator() {
        if (disk_bytes_ > cfg_bytes_) { // reduce to original size
            storage_->set_size(cfg_bytes_);
        }
    }

    int64_t get_free_bytes() const {
        return free_bytes_;
    }

    int64_t get_used_bytes() const {
        return disk_bytes_ - free_bytes_;
    }

    int64_t get_total_bytes() const {
        return disk_bytes_;
    }

    template <size_t BlockSize>
    void new_blocks(BIDArray<BlockSize>& bids) {
        new_blocks<BlockSize>(bids.begin(), bids.end());
    }

    template <typename BidIterator>
    void new_blocks(BidIterator begin, BidIterator end);

    template <size_t BlockSize>
    void delete_block(const BID<BlockSize>& bid);
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_DISK_ALLOCATOR_HEADER

/******************************************************************************/
