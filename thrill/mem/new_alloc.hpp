/*******************************************************************************
 * thrill/mem/new_alloc.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2006 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2007, 2008, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_NEW_ALLOC_HEADER
#define THRILL_MEM_NEW_ALLOC_HEADER

#include <limits>
#include <memory>

namespace thrill {
namespace mem {

template <class Type>
class new_alloc;

template <typename Type, typename Rebind>
struct new_alloc_rebind;

template <typename Type>
struct new_alloc_rebind<Type, Type>{
    using other = new_alloc<Type>;
};

template <typename Type, typename Rebind>
struct new_alloc_rebind {
    using other = std::allocator<Rebind>;
};

// designed for typed_block (to use with std::vector)
template <class Type>
class new_alloc
{
public:
    // type definitions
    using value_type = Type;
    using pointer = Type *;
    using const_pointer = const Type *;
    using reference = Type &;
    using const_reference = const Type &;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    // rebind allocator to type Rebind, use new_alloc only if Rebind == Type
    template <class Rebind>
    struct rebind {
        using other = typename new_alloc_rebind<Type, Rebind>::other;
    };

    // return address of values
    pointer address(reference value) const {
        return &value;
    }
    const_pointer address(const_reference value) const {
        return &value;
    }

    new_alloc() throw () { }
    new_alloc(const new_alloc&) throw () { }
    template <class Rebind>
    new_alloc(const new_alloc<Rebind>&) throw () { }
    ~new_alloc() throw () { }

    template <class Rebind>
    operator std::allocator<Rebind>()
    {
        static std::allocator<Rebind> helper_allocator;
        return helper_allocator;
    }

    // return maximum number of elements that can be allocated
    size_type max_size() const throw () {
        return std::numeric_limits<size_type>::max() / sizeof(Type);
    }

    // allocate but don't initialize num elements of type Type
    pointer allocate(size_type num, const void* = 0) {
        return static_cast<Type*>(Type::operator new (num * sizeof(Type)));
    }

    // _GLIBCXX_RESOLVE_LIB_DEFECTS
    // 402. wrong new expression in [some_] allocator::construct
    // initialize elements of allocated storage p with value value
    void construct(pointer p, const Type& value) {
        // initialize memory with placement new
        ::new ((void*)p)Type(value);
    }

#ifdef __GXX_EXPERIMENTAL_CXX0X__
    template <typename ... Args>
    void construct(pointer p, Args&& ... args) {
        ::new ((void*)p)Type(std::forward<Args>(args) ...);
    }
#endif

    // destroy elements of initialized storage p
    void destroy(pointer p) {
        // destroy objects by calling their destructor
        p->~Type();
    }

    // deallocate storage p of deleted elements
    void deallocate(pointer p, size_type /*num*/) {
        Type::operator delete (p);
    }
};

// return that all specializations of this allocator are interchangeable
template <class Type1, class Type2>
inline bool operator == (const new_alloc<Type1>&,
                         const new_alloc<Type2>&) throw () {
    return true;
}

template <class Type1, class Type2>
inline bool operator != (const new_alloc<Type1>&,
                         const new_alloc<Type2>&) throw () {
    return false;
}

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_NEW_ALLOC_HEADER

/******************************************************************************/
