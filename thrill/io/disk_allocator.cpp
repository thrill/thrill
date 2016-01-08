/*******************************************************************************
 * thrill/io/disk_allocator.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/disk_allocator.hpp>
#include <thrill/io/error_handling.hpp>

#include <cassert>
#include <map>
#include <ostream>
#include <utility>

namespace thrill {
namespace io {

void DiskAllocator::dump() const {
    int64_t total = 0;
    SortSeq::const_iterator cur = free_space_.begin();
    LOG1 << "Free regions dump:";
    for ( ; cur != free_space_.end(); ++cur)
    {
        LOG1 << "Free chunk: begin: " << cur->first << " size: " << cur->second;
        total += cur->second;
    }
    LOG1 << "Total bytes: " << total;
}

void DiskAllocator::deallocation_error(
    int64_t block_pos, int64_t block_size,
    const SortSeq::iterator& pred, const SortSeq::iterator& succ) const {
    LOG1 << "Error deallocating block at " << block_pos << " size " << block_size;
    LOG1 << ((pred == succ) ? "pred==succ" : "pred!=succ");
    if (pred == free_space_.end()) {
        LOG1 << "pred==free_space.end()";
    }
    else {
        if (pred == free_space_.begin())
            LOG1 << "pred==free_space.begin()";
        LOG1 << "pred: begin=" << pred->first << " size=" << pred->second;
    }
    if (succ == free_space_.end()) {
        LOG1 << "succ==free_space.end()";
    }
    else {
        if (succ == free_space_.begin())
            LOG1 << "succ==free_space.begin()";
        LOG1 << "succ: begin=" << succ->first << " size=" << succ->second;
    }
    dump();
}

void DiskAllocator::add_free_region(int64_t block_pos, int64_t block_size) {
    // assert(block_size);
    // dump();
    LOG << "Deallocating a block with size: " << block_size << " position: " << block_pos;
    int64_t region_pos = block_pos;
    int64_t region_size = block_size;
    if (!free_space_.empty())
    {
        SortSeq::iterator succ = free_space_.upper_bound(region_pos);
        SortSeq::iterator pred = succ;
        if (pred != free_space_.begin())
            pred--;
        if (pred != free_space_.end())
        {
            if (pred->first <= region_pos && pred->first + pred->second > region_pos)
            {
                THRILL_THROW2(BadExternalAlloc,
                              "disk_allocator::check_corruption", "Error: double deallocation of external memory, trying to deallocate region " << region_pos << " + " << region_size << "  in empty space [" << pred->first << " + " << pred->second << "]");
            }
        }
        if (succ != free_space_.end())
        {
            if (region_pos <= succ->first && region_pos + region_size > succ->first)
            {
                THRILL_THROW2(BadExternalAlloc, "disk_allocator::check_corruption", "Error: double deallocation of external memory, trying to deallocate region " << region_pos << " + " << region_size << "  which overlaps empty space [" << succ->first << " + " << succ->second << "]");
            }
        }
        if (succ == free_space_.end())
        {
            if (pred == free_space_.end())
            {
                deallocation_error(block_pos, block_size, pred, succ);
                assert(pred != free_space_.end());
            }
            if ((*pred).first + (*pred).second == region_pos)
            {
                // coalesce with predecessor
                region_size += (*pred).second;
                region_pos = (*pred).first;
                free_space_.erase(pred);
            }
        }
        else
        {
            if (free_space_.size() > 1)
            {
#if 0
                if (pred == succ)
                {
                    deallocation_error(block_pos, block_size, pred, succ);
                    assert(pred != succ);
                }
#endif
                bool succ_is_not_the_first = (succ != free_space_.begin());
                if ((*succ).first == region_pos + region_size)
                {
                    // coalesce with successor
                    region_size += (*succ).second;
                    free_space_.erase(succ);
                    //-tb: set succ to pred afterwards due to iterator invalidation
                    succ = pred;
                }
                if (succ_is_not_the_first)
                {
                    if (pred == free_space_.end())
                    {
                        deallocation_error(block_pos, block_size, pred, succ);
                        assert(pred != free_space_.end());
                    }
                    if ((*pred).first + (*pred).second == region_pos)
                    {
                        // coalesce with predecessor
                        region_size += (*pred).second;
                        region_pos = (*pred).first;
                        free_space_.erase(pred);
                    }
                }
            }
            else
            {
                if ((*succ).first == region_pos + region_size)
                {
                    // coalesce with successor
                    region_size += (*succ).second;
                    free_space_.erase(succ);
                }
            }
        }
    }

    free_space_[region_pos] = region_size;
    free_bytes_ += block_size;

    // dump();
}

} // namespace io
} // namespace thrill

/******************************************************************************/
