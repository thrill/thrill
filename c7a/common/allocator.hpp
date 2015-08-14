/*******************************************************************************
 * c7a/common/allocator.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_ALLOCATOR_HEADER
#define C7A_COMMON_ALLOCATOR_HEADER

#include <c7a/common/logger.hpp>

#include <memory>
#include <new>
#include <type_traits>

namespace c7a {
namespace common {

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

    using is_always_equal = std::true_type;
    using propagate_on_container_move_assignment = std::true_type;

    template <class U>
    struct rebind { using other = NewAllocator<U>; };

    NewAllocator() noexcept { }

    NewAllocator(const NewAllocator&) noexcept { }

    template <typename OtherType>
    NewAllocator(const NewAllocator<OtherType>&) noexcept { }

    ~NewAllocator() noexcept { }

    pointer address(reference x) const noexcept {
        return std::addressof(x);
    }

    const_pointer address(const_reference x) const noexcept {
        return std::addressof(x);
    }

    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        if (n > max_size())
            throw std::bad_alloc();

        LOG << "allocate() n=" << n;

        return static_cast<Type*>(::operator new (n * sizeof(Type)));
    }

    void deallocate(pointer p, size_type n) {

        LOG << "deallocate() n=" << n;

        ::operator delete (p);
    }

    size_type max_size() const noexcept {
        return size_t(-1) / sizeof(Type);
    }

    void construct(pointer p, const_reference value) {
        ::new (static_cast<void*>(p))Type(value);
    }

    void destroy(pointer p) {
        p->~Type();
    }

    template <class SubType, class ... Args>
    void construct(SubType* p, Args&& ... args) {
        ::new (static_cast<void*>(p))SubType(std::forward<Args>(args) ...);
    }

    template <class SubType>
    void destroy(SubType* p) {
        p->~SubType();
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_ALLOCATOR_HEADER

/******************************************************************************/
