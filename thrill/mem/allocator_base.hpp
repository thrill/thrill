/*******************************************************************************
 * thrill/mem/allocator_base.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_ALLOCATOR_BASE_HEADER
#define THRILL_MEM_ALLOCATOR_BASE_HEADER

#include <thrill/common/string.hpp>
#include <thrill/mem/malloc_tracker.hpp>
#include <thrill/mem/manager.hpp>
#include <tlx/allocator_base.hpp>

#include <atomic>
#include <cassert>
#include <deque>
#include <iosfwd>
#include <new>
#include <string>
#include <type_traits>
#include <vector>

namespace thrill {
namespace mem {

/******************************************************************************/
// FixedAllocator

template <typename Type, Manager& manager_>
class FixedAllocator : public tlx::AllocatorBase<Type>
{
    static constexpr bool debug = false;

public:
    using value_type = Type;
    using pointer = Type *;
    using const_pointer = const Type *;
    using reference = Type&;
    using const_reference = const Type&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    //! C++11 type flag
    using is_always_equal = std::true_type;

    //! Return allocator for different type.
    template <typename U>
    struct rebind { using other = FixedAllocator<U, manager_>; };

    //! default constructor
    FixedAllocator() noexcept = default;

    //! copy-constructor
    FixedAllocator(const FixedAllocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    FixedAllocator(const FixedAllocator<OtherType, manager_>&) noexcept
    { }

#if !defined(_MSC_VER)
    //! copy-assignment operator: default
    FixedAllocator& operator = (FixedAllocator&) noexcept = default;

    //! move-assignment operator: default
    FixedAllocator& operator = (FixedAllocator&&) noexcept = default;
#endif

    //! Attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        if (n > this->max_size())
            throw std::bad_alloc();

        const size_t size = n * sizeof(Type);
        manager_.add(size);

        if (debug) {
            printf("allocate() n=%zu sizeof(T)=%zu total=%zu\n",
                   n, sizeof(Type), manager_.total());
        }

        Type* r = static_cast<Type*>(bypass_malloc(size));
        while (r == nullptr)
        {
            // If malloc fails and there is a std::new_handler, call it to try
            // free up memory.
            std::new_handler nh = std::get_new_handler();
            if (!nh)
                throw std::bad_alloc();
            nh();
            r = static_cast<Type*>(bypass_malloc(size));
        }
        return r;
    }

    //! Releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type n) const noexcept {

        manager_.subtract(n * sizeof(Type));

        if (debug) {
            printf("deallocate() n=%zu sizeof(T)=%zu total=%zu\n",
                   n, sizeof(Type), manager_.total());
        }

        bypass_free(p, n * sizeof(Type));
    }

    //! Compare to another allocator of same type
    template <typename Other>
    bool operator == (const FixedAllocator<Other, manager_>&) const noexcept {
        return true;
    }

    //! Compare to another allocator of same type
    template <typename Other>
    bool operator != (const FixedAllocator<Other, manager_>&) const noexcept {
        return true;
    }
};

template <Manager& manager_>
class FixedAllocator<void, manager_>
{
public:
    using pointer = void*;
    using const_pointer = const void*;
    using value_type = void;

    template <typename U>
    struct rebind { using other = FixedAllocator<U, manager_>; };
};

/******************************************************************************/
// BypassAllocator

//! global bypass memory manager
extern Manager g_bypass_manager;

//! instantiate FixedAllocator as BypassAllocator
template <typename Type>
using BypassAllocator = FixedAllocator<Type, g_bypass_manager>;

//! operator new with our Allocator
template <typename T, typename... Args>
T * by_new(Args&& ... args) {
    BypassAllocator<T> allocator;
    T* value = allocator.allocate(1);
    allocator.construct(value, std::forward<Args>(args) ...);
    return value;
}

//! operator delete with our Allocator
template <typename T>
void by_delete(T* value) {
    BypassAllocator<T> allocator;
    allocator.destroy(value);
    allocator.deallocate(value, 1);
}

/******************************************************************************/
// template aliases with BypassAllocator

//! string without malloc tracking
using by_string = std::basic_string<
    char, std::char_traits<char>, BypassAllocator<char> >;

//! stringbuf without malloc tracking
using by_stringbuf = std::basic_stringbuf<
    char, std::char_traits<char>, BypassAllocator<char> >;

//! vector without malloc tracking
template <typename T>
using by_vector = std::vector<T, BypassAllocator<T> >;

//! deque without malloc tracking
template <typename T>
using by_deque = std::deque<T, BypassAllocator<T> >;

//! convert to string
static inline by_string to_string(int val) {
    return common::str_snprintf<by_string>(4 * sizeof(int), "%d", val);
}

//! convert to string
static inline by_string to_string(unsigned val) {
    return common::str_snprintf<by_string>(4 * sizeof(int), "%u", val);
}

//! convert to string
static inline by_string to_string(long val) {
    return common::str_snprintf<by_string>(4 * sizeof(long), "%ld", val);
}

//! convert to string
static inline by_string to_string(unsigned long val) {
    return common::str_snprintf<by_string>(4 * sizeof(long), "%lu", val);
}

//! convert to string
static inline by_string to_string(long long val) {
    return common::str_snprintf<by_string>(4 * sizeof(long long), "%lld", val);
}

//! convert to string
static inline by_string to_string(unsigned long long val) {
    return common::str_snprintf<by_string>(4 * sizeof(long long), "%llu", val);
}

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_ALLOCATOR_BASE_HEADER

/******************************************************************************/
