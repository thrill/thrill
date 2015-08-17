/*******************************************************************************
 * c7a/mem/memory_manager.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_MEM_MEMORY_MANAGER_HEADER
#define C7A_MEM_MEMORY_MANAGER_HEADER

#include <c7a/common/logger.hpp>

#include <atomic>
#include <cassert>

namespace c7a {
namespace mem {

/*!
 * Object shared by allocators and other classes to track memory
 * allocations. These is one global MemoryManager per compute host. To track
 * memory consumption of subcomponents of c7a, one can create local child
 * MemoryManagers which report allocation automatically to their superiors.
 */
class MemoryManager
{
public:
    explicit MemoryManager(MemoryManager* super)
        : super_(super)
    { }

    //! return the superior MemoryManager
    MemoryManager * super() { return super_; }

    //! return total allocation (local value)
    size_t total() const { return total_; }

    //! add memory consumption.
    MemoryManager & add(size_t amount) {
        total_ += amount;
        if (super_) super_->add(amount);
        return *this;
    }

    //! subtract memory consumption.
    MemoryManager & subtract(size_t amount) {
        assert(total_ >= amount);
        total_ -= amount;
        if (super_) super_->subtract(amount);
        return *this;
    }

protected:
    //! reference to superior memory counter
    MemoryManager* super_;

    //! total allocation
    std::atomic<size_t> total_ { 0 };
};

} // namespace mem
} // namespace c7a

#endif // !C7A_MEM_MEMORY_MANAGER_HEADER

/******************************************************************************/
