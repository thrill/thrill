/*******************************************************************************
 * c7a/common/allocator.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_ALLOCATOR_HEADER
#define C7A_COMMON_ALLOCATOR_HEADER

#include <c7a/common/logger.hpp>

#include <atomic>
#include <cassert>
#include <deque>
#include <memory>
#include <new>
#include <type_traits>
#include <vector>

namespace c7a {
namespace common {

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

template <typename Type>
class NewAllocator
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

    //! Return allocator for different type.
    template <class U>
    struct rebind { using other = NewAllocator<U>; };

    //! Construct Allocator with MemoryManager object
    NewAllocator(MemoryManager* memory_manager) noexcept
        : memory_manager_(memory_manager) { }

    //! copy-constructor
    NewAllocator(const NewAllocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    NewAllocator(const NewAllocator<OtherType>& other) noexcept
        : memory_manager_(other.memory_manager_) { }

    //! Returns the address of x.
    pointer address(reference x) const noexcept {
        return std::addressof(x);
    }

    //! Returns the address of x.
    const_pointer address(const_reference x) const noexcept {
        return std::addressof(x);
    }

    //! Attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        if (n > max_size())
            throw std::bad_alloc();

        memory_manager_->add(n * sizeof(Type));

        LOG << "allocate() n=" << n << " sizeof(T)=" << sizeof(Type)
            << " total=" << memory_manager_->total();

        return static_cast<Type*>(::operator new (n * sizeof(Type)));
    }

    //! Releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type n) noexcept {

        memory_manager_->subtract(n * sizeof(Type));

        LOG << "deallocate() n=" << n << " sizeof(T)=" << sizeof(Type)
            << " total=" << memory_manager_->total();

        ::operator delete (p);
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

    //! Compare to another allocator of same type
    template <class Other>
    bool operator == (const NewAllocator<Other>&) noexcept {
        return true;
    }

    //! Compare to another allocator of same type
    template <class Other>
    bool operator != (const NewAllocator<Other>&) noexcept {
        return true;
    }

    //! pointer to common MemoryManager object. If we use a reference here, then
    //! the allocator cannot be default move/assigned anymore.
    MemoryManager* memory_manager_;
};

// common containers with our allocator
template <typename T>
using vector = std::vector<T, NewAllocator<T> >;

template <typename T>
using deque = std::deque<T, NewAllocator<T> >;

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_ALLOCATOR_HEADER

/******************************************************************************/
