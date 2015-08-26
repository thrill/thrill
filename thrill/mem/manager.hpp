/*******************************************************************************
 * thrill/mem/manager.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_MANAGER_HEADER
#define THRILL_MEM_MANAGER_HEADER

#include <thrill/common/logger.hpp>

#include <atomic>
#include <cassert>

namespace thrill {
namespace mem {

/*!
 * Object shared by allocators and other classes to track memory
 * allocations. These is one global mem::Manager per compute host. To track
 * memory consumption of subcomponents of Thrill, one can create local child
 * mem::Managers which report allocation automatically to their superiors.
 */
class Manager
{
public:
    explicit Manager(Manager* super)
        : super_(super)
    { }

    //! return the superior Manager
    Manager * super() { return super_; }

    //! return total allocation (local value)
    size_t total() const { return total_; }

    //! add memory consumption.
    Manager & add(size_t amount) {
        total_ += amount;
        if (super_) super_->add(amount);
        return *this;
    }

    //! subtract memory consumption.
    Manager & subtract(size_t amount) {
        assert(total_ >= amount);
        total_ -= amount;
        if (super_) super_->subtract(amount);
        return *this;
    }

protected:
    //! reference to superior memory counter
    Manager* super_;

    //! total allocation
    std::atomic<size_t> total_ { 0 };
};

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_MANAGER_HEADER

/******************************************************************************/
