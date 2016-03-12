/*******************************************************************************
 * thrill/mem/allocator.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_ALLOCATOR_HEADER
#define THRILL_MEM_ALLOCATOR_HEADER

#include <thrill/mem/allocator_base.hpp>
#include <thrill/mem/manager.hpp>

#include <cassert>
#include <deque>
#include <iosfwd>
#include <memory>
#include <new>
#include <string>
#include <type_traits>
#include <vector>

namespace thrill {
namespace mem {

template <typename Type>
class Allocator : public AllocatorBase<Type>
{
    static constexpr bool debug = false;

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
    template <typename U>
    struct rebind { using other = Allocator<U>; };

    //! Construct Allocator with Manager object
    explicit Allocator(Manager& manager) noexcept
        : manager_(&manager) { }

    //! copy-constructor
    Allocator(const Allocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    Allocator(const Allocator<OtherType>& other) noexcept
        : manager_(other.manager_) { }

    //! copy-assignment operator
    Allocator& operator = (const Allocator&) noexcept = default;

    //! Attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        if (n > this->max_size())
            throw std::bad_alloc();

        const size_t size = n * sizeof(Type);
        manager_->add(size);

        if (debug) {
            printf("allocate() n=%zu sizeof(T)=%zu total=%zu\n",
                   n, sizeof(Type), manager_->total());
        }

        Type* r;
        while ((r = static_cast<Type*>(bypass_malloc(size))) == nullptr)
        {
            // If malloc fails and there is a std::new_handler, call it to try
            // free up memory.
            std::new_handler nh = std::get_new_handler();
            if (nh)
                nh();
            else
                throw std::bad_alloc();
        }
        return r;
    }

    //! Releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type n) const noexcept {

        manager_->subtract(n * sizeof(Type));

        if (debug) {
            printf("deallocate() n=%zu sizeof(T)=%zu total=%zu\n",
                   n, sizeof(Type), manager_->total());
        }

        bypass_free(p, n * sizeof(Type));
    }

    //! pointer to common Manager object. If we use a reference here, then
    //! the allocator cannot be default move/assigned anymore.
    Manager* manager_;

    //! Compare to another allocator of same type
    template <typename Other>
    bool operator == (const Allocator<Other>& other) const noexcept {
        return (manager_ == other.manager_);
    }

    //! Compare to another allocator of same type
    template <typename Other>
    bool operator != (const Allocator<Other>& other) const noexcept {
        return (manager_ != other.manager_);
    }
};

template <>
class Allocator<void>
{
public:
    using pointer = void*;
    using const_pointer = const void*;
    using value_type = void;

    //! C++11 type flag
    using is_always_equal = std::false_type;

    template <typename U>
    struct rebind { using other = Allocator<U>; };

    //! Construct Allocator with Manager object
    explicit Allocator(Manager& manager) noexcept
        : manager_(&manager) { }

    //! copy-constructor
    Allocator(const Allocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    Allocator(const Allocator<OtherType>& other) noexcept
        : manager_(other.manager_) { }

    //! pointer to common Manager object. If we use a reference here, then
    //! the allocator cannot be default move/assigned anymore.
    Manager* manager_;

    //! Compare to another allocator of same type
    template <typename Other>
    bool operator == (const Allocator<Other>& other) const noexcept {
        return (manager_ == other.manager_);
    }

    //! Compare to another allocator of same type
    template <typename Other>
    bool operator != (const Allocator<Other>& other) const noexcept {
        return (manager_ != other.manager_);
    }
};

//! operator new with our Allocator
template <typename T, typename ... Args>
T * mm_new(Manager& manager, Args&& ... args) {
    Allocator<T> allocator(manager);
    T* value = allocator.allocate(1);
    allocator.construct(value, std::forward<Args>(args) ...);
    return value;
}

//! operator delete with our Allocator
template <typename T>
void mm_delete(Manager& manager, T* value) {
    Allocator<T> allocator(manager);
    allocator.destroy(value);
    allocator.deallocate(value, 1);
}

//! std::default_deleter with Manager tracking
template <typename T>
class Deleter
{
public:
    //! constructor: need reference to Manager
    explicit Deleter(Manager& manager) noexcept
        : allocator_(manager) { }

    //! free the pointer
    void operator () (T* ptr) const noexcept {
        allocator_.destroy(ptr);
        allocator_.deallocate(ptr, 1);
    }

private:
    //! reference to Manager for freeing.
    Allocator<T> allocator_;
};

//! unique_ptr with Manager tracking
template <typename T>
using unique_ptr = std::unique_ptr<T, Deleter<T> >;

//! make_unique with Manager tracking
template <typename T, typename ... Args>
unique_ptr<T> make_unique(Manager& manager, Args&& ... args) {
    return unique_ptr<T>(
        mm_new<T>(manager, std::forward<Args>(args) ...),
        Deleter<T>(manager));
}

//! string with Manager tracking
using string = std::basic_string<
          char, std::char_traits<char>, Allocator<char> >;

//! stringbuf with Manager tracking
using stringbuf = std::basic_stringbuf<
          char, std::char_traits<char>, Allocator<char> >;

//! vector with Manager tracking
template <typename T>
using vector = std::vector<T, Allocator<T> >;

//! deque with Manager tracking
template <typename T>
using deque = std::deque<T, Allocator<T> >;

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_ALLOCATOR_HEADER

/******************************************************************************/
