/*******************************************************************************
 * c7a/core/allocator.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_CORE_ALLOCATOR_HEADER
#define C7A_CORE_ALLOCATOR_HEADER

#include <c7a/core/allocator_base.hpp>
#include <c7a/core/memory_manager.hpp>

#include <cassert>
#include <memory>
#include <new>
#include <type_traits>
#include <string>
#include <iosfwd>

namespace c7a {
namespace core {

template <typename Type>
class Allocator : public AllocatorBase<Type>
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
    using is_always_equal = std::false_type;

    //! Return allocator for different type.
    template <class U>
    struct rebind { using other = Allocator<U>; };

    //! Construct Allocator with MemoryManager object
    Allocator(MemoryManager* memory_manager) noexcept
        : memory_manager_(memory_manager) { }

    //! copy-constructor
    Allocator(const Allocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    Allocator(const Allocator<OtherType>& other) noexcept
        : memory_manager_(other.memory_manager_) { }

    //! Attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        if (n > this->max_size())
            throw std::bad_alloc();

        memory_manager_->add(n * sizeof(Type));

        if (debug) {
            printf("allocate() n=%lu sizeof(T)=%lu total=%lu\n",
                   n, sizeof(Type), memory_manager_->total());
        }

        return static_cast<Type*>(bypass_malloc(n * sizeof(Type)));
    }

    //! Releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type n) noexcept {

        memory_manager_->subtract(n * sizeof(Type));

        if (debug) {
            printf("deallocate() n=%lu sizeof(T)=%lu total=%lu\n",
                   n, sizeof(Type), memory_manager_->total());
        }

        bypass_free(p);
    }

    //! pointer to common MemoryManager object. If we use a reference here, then
    //! the allocator cannot be default move/assigned anymore.
    MemoryManager* memory_manager_;

    //! Compare to another allocator of same type
    template <class Other>
    bool operator == (const Allocator<Other>& other) const noexcept {
        return (memory_manager_ == other.memory_manager_);
    }

    //! Compare to another allocator of same type
    template <class Other>
    bool operator != (const Allocator<Other>& other) const noexcept {
        return (memory_manager_ != other.memory_manager_);
    }
};

} // namespace core
} // namespace c7a

// template class prototypes, so we can declare containers without including the
// whole implementation code.
namespace std {

template <class T, class Alloc>
class vector;

template <class T, class Alloc>
class deque;

template <class Key, class T, class Compare, class Alloc>
class map;

} // namespace std

namespace c7a {
namespace core {

//! string with MemoryManager tracking
using mm_string = std::basic_string<
    char, std::char_traits<char>, Allocator<char> >;

//! stringbuf with MemoryManager tracking
using mm_stringbuf = std::basic_stringbuf<
    char, std::char_traits<char>, Allocator<char> >;

//! vector with MemoryManager tracking
template <typename T>
using mm_vector = std::vector<T, Allocator<T> >;

//! deque with MemoryManager tracking
template <typename T>
using mm_deque = std::deque<T, Allocator<T> >;

//! deque with MemoryManager tracking
template <class Key, class T, class Compare = std::less<Key> >
using mm_map = std::map<Key, T, Compare,
                        Allocator<std::pair<const Key, T> > >;

} // namespace core
} // namespace c7a

#endif // !C7A_CORE_ALLOCATOR_HEADER

/******************************************************************************/
