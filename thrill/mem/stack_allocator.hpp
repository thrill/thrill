/*******************************************************************************
 * thrill/mem/stack_allocator.hpp
 *
 * An allocator derived from short_alloc by Howard Hinnant, which first takes
 * memory from a stack allocated reserved area and then from our malloc().
 *
 * from http://howardhinnant.github.io/stack_alloc.html
 * Copyright (C) 2014 Howard Hinnant
 * and from
 * http://codereview.stackexchange.com/questions/31528/a-working-stack-allocator
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_STACK_ALLOCATOR_HEADER
#define THRILL_MEM_STACK_ALLOCATOR_HEADER

#include <thrill/mem/allocator_base.hpp>

#include <cassert>
#include <cstddef>

namespace thrill {
namespace mem {

/*!
 * Storage area allocated on the stack and usable by a StackAllocator.
 */
template <size_t Size>
class Arena
{
    static const size_t alignment = 16;

    //! stack memory area used for allocations.
    alignas(alignment) char buf_[Size];

    //! pointer into free bytes in buf_
    char* ptr_;

    //! debug method to check whether ptr_ is still in buf_.
    bool pointer_in_buffer(char* p) noexcept
    { return buf_ <= p && p <= buf_ + Size; }

public:
    //! default constructor: free pointer at the beginning.
    Arena() noexcept : ptr_(buf_) { }

    //! destructor clears ptr_ for debugging.
    ~Arena() { ptr_ = nullptr; }

    Arena(const Arena&) = delete;
    Arena& operator = (const Arena&) = delete;

    char * allocate(size_t n) {
        assert(pointer_in_buffer(ptr_) && "StackAllocator has outlived Arena");

        // try to allocate from stack memory area
        if (buf_ + Size >= ptr_ + n) {
            char* r = ptr_;
            ptr_ += n;
            return r;
        }
        // otherwise fallback to malloc()
        return static_cast<char*>(malloc(n));
    }

    void deallocate(char* p, size_t n) noexcept {
        assert(pointer_in_buffer(ptr_) && "StackAllocator has outlived Arena");
        if (pointer_in_buffer(p)) {
            // free memory area (only works for a stack-ordered
            // allocations/deallocations).
            if (p + n == ptr_)
                ptr_ = p;
        }
        else {
            free(p);
        }
    }

    //! size of memory area
    static constexpr size_t size() { return Size; }

    //! return number of bytes used in Arena
    size_t used() const { return static_cast<size_t>(ptr_ - buf_); }

    //! reset memory area
    void reset() { ptr_ = buf_; }
};

template <class Type, size_t Size>
class StackAllocator : public AllocatorBase<Type>
{
public:
    using value_type = Type;
    using pointer = Type *;
    using const_pointer = const Type *;
    using reference = Type &;
    using const_reference = const Type &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    //! C++11 type flag
    using is_always_equal = std::false_type;

    //! required rebind.
    template <class Other>
    struct rebind { using other = StackAllocator<Other, Size>; };

    //! default constructor to invalid arena
    StackAllocator() : arena_(nullptr) { }

    //! constructor with explicit arena reference
    explicit StackAllocator(Arena<Size>& arena) noexcept
        : arena_(&arena) { }

    //! constructor from another allocator with same arena size
    template <class Other>
    StackAllocator(const StackAllocator<Other, Size>& other) noexcept
        : arena_(other.arena_) { }

    //! copy-constructor: default
    StackAllocator(const StackAllocator&) noexcept = default;

#if !defined(_MSC_VER)
    //! copy-assignment: default
    StackAllocator& operator = (StackAllocator&) noexcept = default;

    //! move-constructor: default
    StackAllocator(StackAllocator&&) noexcept = default;

    //! move-assignment: default
    StackAllocator& operator = (StackAllocator&&) noexcept = default;
#endif

    //! allocate method: get memory from arena
    pointer allocate(size_t n) {
        return reinterpret_cast<Type*>(arena_->allocate(n * sizeof(Type)));
    }

    //! deallocate method: release from arena
    void deallocate(pointer p, size_t n) noexcept {
        arena_->deallocate(reinterpret_cast<char*>(p), n * sizeof(Type));
    }

    template <typename Other, size_t OtherSize>
    bool operator == (const StackAllocator<Other, OtherSize>& other) const noexcept {
        return Size == OtherSize && arena_ == other.arena_;
    }

    template <typename Other, size_t OtherSize>
    bool operator != (const StackAllocator<Other, OtherSize>& other) const noexcept {
        return Size != OtherSize || arena_ != other.arena_;
    }

    template <class Other, size_t OtherSize>
    friend class StackAllocator;

private:
    Arena<Size>* arena_;
};

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_STACK_ALLOCATOR_HEADER

/******************************************************************************/
