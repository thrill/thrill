/*******************************************************************************
 * c7a/core/allocator_base.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_ALLOCATOR_BASE_HEADER
#define C7A_CORE_ALLOCATOR_BASE_HEADER

#include <c7a/core/malloc_tracker.hpp>

#include <atomic>
#include <cassert>
#include <memory>
#include <new>
#include <type_traits>
#include <string>
#include <iosfwd>
#include <vector>
#include <deque>

namespace c7a {
namespace core {

template <typename Type>
class AllocatorBase
{
    static const bool debug = true;

public:
    using value_type = Type;
    using pointer = Type *;
    using const_pointer = const Type *;
    using reference = Type &;
    using const_reference = const Type &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    //! C++11 type flag
    using is_always_equal = std::true_type;
    //! C++11 type flag
    using propagate_on_container_move_assignment = std::true_type;

    //! Returns the address of x.
    pointer address(reference x) const noexcept {
        return std::addressof(x);
    }

    //! Returns the address of x.
    const_pointer address(const_reference x) const noexcept {
        return std::addressof(x);
    }

    //! Maximum size possible to allocate
    size_type max_size() const noexcept {
        return size_t(-1) / sizeof(Type);
    }

    //! Constructs an element object on the location pointed by p.
    void construct(pointer p, const_reference value) {
        ::new (static_cast<void*>(p))Type(value);
    }

    //! Destroys in-place the object pointed by p.
    void destroy(pointer p) {
        p->~Type();
    }

    //! Constructs an element object on the location pointed by p.
    template <class SubType, class ... Args>
    void construct(SubType* p, Args&& ... args) {
        ::new (static_cast<void*>(p))SubType(std::forward<Args>(args) ...);
    }

    //! Destroys in-place the object pointed by p.
    template <class SubType>
    void destroy(SubType* p) {
        p->~SubType();
    }
};

template <typename Type>
class BypassAllocator : public AllocatorBase<Type>
{
    static const bool debug = true;

public:
    using value_type = Type;
    using pointer = Type *;
    using const_pointer = const Type *;
    using reference = Type &;
    using const_reference = const Type &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    //! Return allocator for different type.
    template <class U>
    struct rebind { using other = BypassAllocator<U>; };

    //! Construct Allocator with MemoryManager object
    BypassAllocator() noexcept { }

    //! copy-constructor
    BypassAllocator(const BypassAllocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    BypassAllocator(const BypassAllocator<OtherType>& /* other */) noexcept { }

    //! Attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        if (n > this->max_size())
            throw std::bad_alloc();

        return static_cast<Type*>(bypass_malloc(n * sizeof(Type)));
    }

    //! Releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type /* n */) noexcept {
        bypass_free(p);
    }

    //! Compare to another allocator of same type
    template <class Other>
    bool operator == (const BypassAllocator<Other>&) const noexcept {
        return true;
    }

    //! Compare to another allocator of same type
    template <class Other>
    bool operator != (const BypassAllocator<Other>&) const noexcept {
        return true;
    }
};

//! string without malloc tracking
using string = std::basic_string<
          char, std::char_traits<char>, BypassAllocator<char> >;

//! stringbuf without malloc tracking
using stringbuf = std::basic_stringbuf<
          char, std::char_traits<char>, BypassAllocator<char> >;

//! vector without malloc tracking
template <typename T>
using vector = std::vector<T, BypassAllocator<T> >;

//! deque without malloc tracking
template <typename T>
using deque = std::deque<T, BypassAllocator<T> >;

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_ALLOCATOR_BASE_HEADER

/******************************************************************************/
