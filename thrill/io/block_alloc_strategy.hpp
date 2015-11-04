/*******************************************************************************
 * thrill/io/block_alloc_strategy.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2007 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2007-2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_BLOCK_ALLOC_STRATEGY_HEADER
#define THRILL_IO_BLOCK_ALLOC_STRATEGY_HEADER

#include <thrill/io/config_file.hpp>

#include <algorithm>
#include <random>

namespace thrill {
namespace io {

//! \defgroup alloc Allocation Functors
//! \ingroup mnglayer
//! Standard allocation strategies encapsulated in functors.
//! \{

//! Example disk allocation scheme functor.
//! \remarks model of \b allocation_strategy concept
struct basic_allocation_strategy
{
    basic_allocation_strategy(int disks_begin, int disks_end);
    basic_allocation_strategy();
    int operator () (int i) const;
    static const char * name();
};

//! Striping disk allocation scheme functor.
//! \remarks model of \b allocation_strategy concept
struct striping
{
    size_t            begin, diff;

public:
    striping(size_t b, size_t e) : begin(b), diff(e - b)
    { }

    striping() : begin(0) {
        diff = config::get_instance()->disks_number();
    }

    size_t operator () (size_t i) const {
        return begin + i % diff;
    }

    static const char * name() {
        return "striping";
    }
};

//! Fully randomized disk allocation scheme functor.
//! \remarks model of \b allocation_strategy concept
struct FR : public striping
{
private:
    mutable std::default_random_engine rng_ { std::random_device { } () };

public:
    FR(size_t b, size_t e) : striping(b, e)
    { }

    FR() : striping()
    { }

    size_t operator () (size_t /*i*/) const {
        return begin + rng_() % diff;
    }

    static const char * name() {
        return "fully randomized striping";
    }
};

//! Simple randomized disk allocation scheme functor.
//! \remarks model of \b allocation_strategy concept
struct SR : public striping
{
private:
    size_t offset;

    void init() {
        std::default_random_engine rng { std::random_device { } () };
        offset = rng() % diff;
    }

public:
    SR(size_t b, size_t e) : striping(b, e) {
        init();
    }

    SR() : striping() {
        init();
    }

    size_t operator () (size_t i) const {
        return begin + (i + offset) % diff;
    }

    static const char * name() {
        return "simple randomized striping";
    }
};

//! Randomized cycling disk allocation scheme functor.
//! \remarks model of \b allocation_strategy concept
struct RC : public striping
{
private:
    std::vector<size_t> perm;

    void init() {
        for (size_t i = 0; i < diff; i++)
            perm[i] = i;

        std::random_shuffle(perm.begin(), perm.end());
    }

public:
    RC(size_t b, size_t e) : striping(b, e), perm(diff) {
        init();
    }

    RC() : striping(), perm(diff) {
        init();
    }

    size_t operator () (size_t i) const {
        return begin + perm[i % diff];
    }

    static const char * name() {
        return "randomized cycling striping";
    }
};

struct RC_disk : public RC
{
    RC_disk(size_t b, size_t e) : RC(b, e)
    { }

    RC_disk() : RC(config::get_instance()->regular_disk_range().first, config::get_instance()->regular_disk_range().second)
    { }

    static const char * name() {
        return "Randomized cycling striping on regular disks";
    }
};

struct RC_flash : public RC
{
    RC_flash(size_t b, size_t e) : RC(b, e)
    { }

    RC_flash() : RC(config::get_instance()->flash_range().first, config::get_instance()->flash_range().second)
    { }

    static const char * name() {
        return "Randomized cycling striping on flash devices";
    }
};

//! 'Single disk' disk allocation scheme functor.
//! \remarks model of \b allocation_strategy concept
struct single_disk
{
    size_t            disk;
    single_disk(size_t d, size_t = 0) : disk(d)
    { }

    single_disk() : disk(0)
    { }

    size_t operator () (size_t /*i*/) const {
        return disk;
    }

    static const char * name() {
        return "single disk";
    }
};

//! Allocator functor adaptor.
//!
//! Gives offset to disk number sequence defined in constructor
template <class BaseAllocator>
struct offset_allocator
{
    BaseAllocator base;
    int           offset;

    //! Creates functor based on instance of \c BaseAllocator functor
    //! with offset \c offset_.
    //! \param offset_ offset
    //! \param base_ used to create a copy
    offset_allocator(int offset_, const BaseAllocator& base_)
        : base(base_), offset(offset_)
    { }

    //! Creates functor based on instance of \c BaseAllocator functor.
    //! \param base_ used to create a copy
    offset_allocator(const BaseAllocator& base_) : base(base_), offset(0)
    { }

    //! Creates functor based on default \c BaseAllocator functor.
    offset_allocator() : offset(0)
    { }

    size_t operator () (size_t i) const {
        return base(offset + i);
    }

    int  get_offset() const {
        return offset;
    }

    void set_offset(int i) {
        offset = i;
    }
};

#ifndef STXXL_DEFAULT_ALLOC_STRATEGY
  #define STXXL_DEFAULT_ALLOC_STRATEGY ::thrill::io::FR
#endif

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_BLOCK_ALLOC_STRATEGY_HEADER

/******************************************************************************/
