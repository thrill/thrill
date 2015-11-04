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

#include "error_handling.hpp"
#include <thrill/io/disk_allocator.hpp>

#include <cassert>
#include <map>
#include <ostream>
#include <utility>

namespace thrill {
namespace io {

void disk_allocator::dump() const {
    int64_t total = 0;
    sortseq::const_iterator cur = free_space.begin();
    LOG1 << "Free regions dump:";
    for ( ; cur != free_space.end(); ++cur)
    {
        LOG1 << "Free chunk: begin: " << cur->first << " size: " << cur->second;
        total += cur->second;
    }
    LOG1 << "Total bytes: " << total;
}

void disk_allocator::deallocation_error(
    int64_t block_pos, int64_t block_size,
    const sortseq::iterator& pred, const sortseq::iterator& succ) const {
    LOG1 << "Error deallocating block at " << block_pos << " size " << block_size;
    LOG1 << ((pred == succ) ? "pred==succ" : "pred!=succ");
    if (pred == free_space.end()) {
        LOG1 << "pred==free_space.end()";
    }
    else {
        if (pred == free_space.begin())
            LOG1 << "pred==free_space.begin()";
        LOG1 << "pred: begin=" << pred->first << " size=" << pred->second;
    }
    if (succ == free_space.end()) {
        LOG1 << "succ==free_space.end()";
    }
    else {
        if (succ == free_space.begin())
            LOG1 << "succ==free_space.begin()";
        LOG1 << "succ: begin=" << succ->first << " size=" << succ->second;
    }
    dump();
}

void disk_allocator::add_free_region(int64_t block_pos, int64_t block_size) {
    // assert(block_size);
    // dump();
    LOG << "Deallocating a block with size: " << block_size << " position: " << block_pos;
    int64_t region_pos = block_pos;
    int64_t region_size = block_size;
    if (!free_space.empty())
    {
        sortseq::iterator succ = free_space.upper_bound(region_pos);
        sortseq::iterator pred = succ;
        if (pred != free_space.begin())
            pred--;
        if (pred != free_space.end())
        {
            if (pred->first <= region_pos && pred->first + pred->second > region_pos)
            {
                STXXL_THROW2(bad_ext_alloc,
                             "disk_allocator::check_corruption", "Error: double deallocation of external memory, trying to deallocate region " << region_pos << " + " << region_size << "  in empty space [" << pred->first << " + " << pred->second << "]");
            }
        }
        if (succ != free_space.end())
        {
            if (region_pos <= succ->first && region_pos + region_size > succ->first)
            {
                STXXL_THROW2(bad_ext_alloc, "disk_allocator::check_corruption", "Error: double deallocation of external memory, trying to deallocate region " << region_pos << " + " << region_size << "  which overlaps empty space [" << succ->first << " + " << succ->second << "]");
            }
        }
        if (succ == free_space.end())
        {
            if (pred == free_space.end())
            {
                deallocation_error(block_pos, block_size, pred, succ);
                assert(pred != free_space.end());
            }
            if ((*pred).first + (*pred).second == region_pos)
            {
                // coalesce with predecessor
                region_size += (*pred).second;
                region_pos = (*pred).first;
                free_space.erase(pred);
            }
        }
        else
        {
            if (free_space.size() > 1)
            {
#if 0
                if (pred == succ)
                {
                    deallocation_error(block_pos, block_size, pred, succ);
                    assert(pred != succ);
                }
#endif
                bool succ_is_not_the_first = (succ != free_space.begin());
                if ((*succ).first == region_pos + region_size)
                {
                    // coalesce with successor
                    region_size += (*succ).second;
                    free_space.erase(succ);
                    //-tb: set succ to pred afterwards due to iterator invalidation
                    succ = pred;
                }
                if (succ_is_not_the_first)
                {
                    if (pred == free_space.end())
                    {
                        deallocation_error(block_pos, block_size, pred, succ);
                        assert(pred != free_space.end());
                    }
                    if ((*pred).first + (*pred).second == region_pos)
                    {
                        // coalesce with predecessor
                        region_size += (*pred).second;
                        region_pos = (*pred).first;
                        free_space.erase(pred);
                    }
                }
            }
            else
            {
                if ((*succ).first == region_pos + region_size)
                {
                    // coalesce with successor
                    region_size += (*succ).second;
                    free_space.erase(succ);
                }
            }
        }
    }

    free_space[region_pos] = region_size;
    free_bytes += block_size;

    // dump();
}

} // namespace io
} // namespace thrill

/******************************************************************************/
