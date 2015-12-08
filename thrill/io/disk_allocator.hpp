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
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_DISK_ALLOCATOR_HEADER
#define THRILL_IO_DISK_ALLOCATOR_HEADER

#include <thrill/io/bid.hpp>
#include <thrill/io/config_file.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/file.hpp>

#include <algorithm>
#include <cassert>
#include <functional>
#include <map>
#include <mutex>
#include <ostream>
#include <utility>

namespace thrill {
namespace io {

//! \ingroup mnglayer
//! \{

class disk_allocator
{
    static const bool debug = false;

    using place = std::pair<int64_t, int64_t>;

    struct first_fit : public std::binary_function<place, int64_t, bool>
    {
        bool operator () (
            const place& entry,
            const int64_t size) const {
            return (entry.second >= size);
        }
    };

    using SortSeq = std::map<int64_t, int64_t>;

    std::mutex mutex_;
    SortSeq free_space_;
    int64_t free_bytes_;
    int64_t disk_bytes_;
    int64_t cfg_bytes_;
    file* storage_;
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
    disk_allocator(file* storage, const disk_config& cfg)
        : free_bytes_(0),
          disk_bytes_(0),
          cfg_bytes_(cfg.size),
          storage_(storage),
          autogrow_(cfg.autogrow) {
        // initial growth to configured file size
        grow_file(cfg.size);
    }

    //! non-copyable: delete copy-constructor
    disk_allocator(const disk_allocator&) = delete;
    //! non-copyable: delete assignment operator
    disk_allocator& operator = (const disk_allocator&) = delete;

    ~disk_allocator() {
        if (disk_bytes_ > cfg_bytes_) { // reduce to original size
            storage_->set_size(cfg_bytes_);
        }
    }

    inline int64_t get_free_bytes() const {
        return free_bytes_;
    }

    inline int64_t get_used_bytes() const {
        return disk_bytes_ - free_bytes_;
    }

    inline int64_t get_total_bytes() const {
        return disk_bytes_;
    }

    template <size_t BlockSize>
    void new_blocks(BIDArray<BlockSize>& bids) {
        new_blocks<BlockSize>(bids.begin(), bids.end());
    }

    template <size_t BlockSize>
    void new_blocks(typename BIDArray<BlockSize>::iterator begin,
                    typename BIDArray<BlockSize>::iterator end);

#if 0
    template <size_t BlockSize>
    void delete_blocks(const BIDArray<BlockSize>& bids) {
        for (size_t i = 0; i < bids.size(); ++i)
            delete_block(bids[i]);
    }
#endif

    template <size_t BlockSize>
    void delete_block(const BID<BlockSize>& bid) {
        std::unique_lock<std::mutex> lock(mutex_);

        LOG << "disk_allocator::delete_block<" << BlockSize
            << ">(pos=" << bid.offset << ", size=" << bid.size
            << "), free:" << free_bytes_ << " total:" << disk_bytes_;

        add_free_region(bid.offset, bid.size);
    }
};

template <size_t BlockSize>
void disk_allocator::new_blocks(typename BIDArray<BlockSize>::iterator begin,
                                typename BIDArray<BlockSize>::iterator end) {
    uint64_t requested_size = 0;

    for (typename BIDArray<BlockSize>::iterator cur = begin; cur != end; ++cur)
    {
        LOG << "Asking for a block with size: " << cur->size;
        requested_size += cur->size;
    }

    std::unique_lock<std::mutex> lock(mutex_);

    LOG << "disk_allocator::new_blocks<BlockSize>,  BlockSize = " << BlockSize
        << ", free:" << free_bytes_ << " total:" << disk_bytes_
        << ", blocks: " << (end - begin)
        << " begin: " << static_cast<void*>(&(*begin))
        << " end: " << static_cast<void*>(&(*end))
        << ", requested_size=" << requested_size;

    if (free_bytes_ < (int64_t)requested_size)
    {
        if (!autogrow_) {
            STXXL_THROW(bad_ext_alloc,
                        "Out of external memory error: " << requested_size <<
                        " requested, " << free_bytes_ << " bytes free. "
                        "Maybe enable autogrow flags?");
        }

        LOG << "External memory block allocation error: " << requested_size <<
            " bytes requested, " << free_bytes_ <<
            " bytes free. Trying to extend the external memory space...";

        grow_file(requested_size);
    }

    // dump();

    SortSeq::iterator space;
    space = std::find_if(free_space_.begin(), free_space_.end(),
                         bind2nd(first_fit(), requested_size));

    if (space == free_space_.end() && requested_size == BlockSize)
    {
        assert(end - begin == 1);

        if (!autogrow_) {
            LOG1 << "Warning: Severe external memory space fragmentation!";
            dump();

            LOG1 << "External memory block allocation error: " << requested_size
                 << " bytes requested, " << free_bytes_
                 << " bytes free. Trying to extend the external memory space...";
        }

        grow_file(BlockSize);

        space = std::find_if(free_space_.begin(), free_space_.end(),
                             bind2nd(first_fit(), requested_size));
    }

    if (space != free_space_.end())
    {
        int64_t region_pos = (*space).first;
        int64_t region_size = (*space).second;
        free_space_.erase(space);
        if (region_size > (int64_t)requested_size)
            free_space_[region_pos + requested_size] = region_size - requested_size;

        for (int64_t pos = region_pos; begin != end; ++begin)
        {
            begin->offset = pos;
            pos += begin->size;
        }
        free_bytes_ -= requested_size;
        // dump();

        return;
    }

    // no contiguous region found
    LOG1 << "Warning, when allocating an external memory space, no contiguous region found";
    LOG1 << "It might harm the performance";

    assert(requested_size > BlockSize);
    assert(end - begin > 1);

    lock.unlock();

    typename BIDArray<BlockSize>::iterator middle = begin + ((end - begin) / 2);
    new_blocks<BlockSize>(begin, middle);
    new_blocks<BlockSize>(middle, end);
}

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_DISK_ALLOCATOR_HEADER

/******************************************************************************/
