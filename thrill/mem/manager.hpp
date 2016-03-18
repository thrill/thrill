/*******************************************************************************
 * thrill/mem/manager.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_MANAGER_HEADER
#define THRILL_MEM_MANAGER_HEADER

#include <algorithm>
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
    static constexpr bool debug = false;

public:
    explicit Manager(Manager* super, const char* name)
        : super_(super), name_(name)
    { }

    ~Manager();

    //! return the superior Manager
    Manager * super() { return super_; }

    //! return total allocation (local value)
    size_t total() const { return total_; }

    //! add memory consumption.
    Manager& add(size_t amount) {
        size_t current = (total_ += amount);
        peak_ = std::max(peak_.load(), current);
        ++alloc_count_;
        if (super_) super_->add(amount);
        return *this;
    }

    //! subtract memory consumption.
    Manager& subtract(size_t amount) {
        assert(total_ >= amount);
        total_ -= amount;
        if (super_) super_->subtract(amount);
        return *this;
    }

private:
    //! reference to superior memory counter
    Manager* super_;

    //! description for output
    const char* name_;

    //! total allocation
    std::atomic<size_t> total_ { 0 };

    //! peak allocation
    std::atomic<size_t> peak_ { 0 };

    //! number of allocation
    std::atomic<size_t> alloc_count_ { 0 };
};

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_MANAGER_HEADER

/******************************************************************************/
