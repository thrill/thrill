/*******************************************************************************
 * thrill/mem/aligned_allocator.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_ALIGNED_ALLOCATOR_HEADER
#define THRILL_MEM_ALIGNED_ALLOCATOR_HEADER

#include <thrill/common/logger.hpp>
#include <thrill/mem/allocator_base.hpp>

#include <cassert>
#include <cstdlib>
#include <memory>

#define THRILL_DEFAULT_ALIGN 4096

namespace thrill {
namespace mem {

template <typename MustBeInt>
struct AlignedAllocatorSettings {
    static bool may_use_realloc_;
};

template <typename MustBeInt>
bool AlignedAllocatorSettings<MustBeInt>::may_use_realloc_ = false;

template <typename Type = char,
          typename BaseAllocator = std::allocator<char>,
          size_t Alignment = THRILL_DEFAULT_ALIGN>
class AlignedAllocator : public tlx::AllocatorBase<Type>
{
    static constexpr bool debug = false;

    static_assert(sizeof(typename BaseAllocator::value_type) == 1,
                  "BaseAllocator must be a char/byte allocator");

public:
    using value_type = Type;
    using pointer = Type *;
    using const_pointer = const Type *;
    using reference = Type&;
    using const_reference = const Type&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;

    //! C++11 type flag
    using is_always_equal = std::false_type;

    //! Return allocator for different type.
    template <typename U>
    struct rebind { using other = AlignedAllocator<U, BaseAllocator>; };

    //! Construct with base allocator
    explicit AlignedAllocator(const BaseAllocator& base = BaseAllocator())
        : base_(base) { }

    //! copy-constructor
    AlignedAllocator(const AlignedAllocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    AlignedAllocator(const AlignedAllocator<OtherType>& other) noexcept
        : base_(other.base()) { }

    //! copy-assignment operator
    AlignedAllocator& operator = (const AlignedAllocator&) noexcept = default;

    //! Attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        if (n > this->max_size())
            throw std::bad_alloc();

        return static_cast<pointer>(allocate_bytes(n * sizeof(Type)));
    }

    //! Releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type n) noexcept {
        deallocate_bytes(p, n * sizeof(Type));
    }

    //! Compare to another allocator of same type
    template <typename Other>
    bool operator == (const AlignedAllocator<Other>& other) const noexcept {
        return (base_ == other.base_);
    }

    //! Compare to another allocator of same type
    template <typename Other>
    bool operator != (const AlignedAllocator<Other>& other) const noexcept {
        return (base_ != other.base_);
    }

    /**************************************************************************/

    void * allocate_bytes(size_t size, size_t meta_info_size = 0);
    void deallocate_bytes(void* ptr, size_t size, size_t meta_info_size = 0) noexcept;

    const BaseAllocator& base() const { return base_; }

private:
    //! base allocator
    BaseAllocator base_;
};

// meta_info_size > 0 is needed for array allocations that have overhead
//
//                      meta_info
//                          aligned begin of data   unallocated behind data
//                      v   v                       v
//  ----===============#MMMM========================------
//      ^              ^^                           ^
//      buffer          result                      result+m_i_size+size
//                     pointer to buffer
// (---) unallocated, (===) allocated memory

template <typename Type, typename BaseAllocator, size_t Alignment>
inline void* AlignedAllocator<Type, BaseAllocator, Alignment>::allocate_bytes(
    size_t size, size_t meta_info_size) {

    LOG << "aligned_alloc<" << Alignment << ">(), size = " << size
        << ", meta info size = " << meta_info_size;

    // malloc()/realloc() variant that frees the unused amount of memory
    // after the data area of size 'size'. realloc() from valgrind does not
    // preserve the old memory area when shrinking, so out-of-bounds
    // accesses can't be detected easily.
    // Overhead: about Alignment bytes.
    size_t alloc_size = Alignment + sizeof(char*) + meta_info_size + size;
    char* buffer = reinterpret_cast<char*>(base_.allocate(alloc_size));

    if (buffer == nullptr)
        return nullptr;

    char* reserve_buffer = buffer + sizeof(char*) + meta_info_size;
    char* result = reserve_buffer + Alignment -
                   (((size_t)reserve_buffer) % (Alignment)) - meta_info_size;

    LOG << "aligned_alloc<" << Alignment << ">() address " << static_cast<void*>(result)
        << " lost " << (result - buffer) << " bytes";

    // -tb: check that there is space for one char* before the "result" pointer
    // delivered to the user. this char* is set below to the beginning of the
    // allocated area.
    assert(long(result - buffer) >= long(sizeof(char*)));

#if 0
    // free unused memory behind the data area
    // so access behind the requested size can be recognized
    size_t realloc_size = (result - buffer) + meta_info_size + size;
    if (realloc_size < alloc_size && AlignedAllocatorSettings<int>::may_use_realloc_) {
        char* realloced = static_cast<char*>(std::realloc(buffer, realloc_size));
        if (buffer != realloced) {
            // hmm, realloc does move the memory block around while shrinking,
            // might run under valgrind, so disable realloc and retry
            LOG1 << "mem::aligned_alloc: disabling realloc()";
            std::free(realloced);
            AlignedAllocatorSettings<int>::may_use_realloc_ = false;
            return allocate(size, meta_info_size);
        }
        assert(result + size <= buffer + realloc_size);
    }
#endif

    *((reinterpret_cast<char**>(result)) - 1) = buffer;
    LOG << "aligned_alloc<" << Alignment << ">(), allocated at "
        << static_cast<void*>(buffer) << " returning " << static_cast<void*>(result);
    LOG << "aligned_alloc<" << Alignment << ">(size = " << size
        << ", meta info size = " << meta_info_size
        << ") => buffer = " << static_cast<void*>(buffer)
        << ", ptr = " << static_cast<void*>(result);

    return result;
}

template <typename Type, typename BaseAllocator, size_t Alignment>
inline void AlignedAllocator<Type, BaseAllocator, Alignment>::deallocate_bytes(
    void* ptr, size_t size, size_t meta_info_size) noexcept {
    if (!ptr)
        return;
    char* buffer = *((reinterpret_cast<char**>(ptr)) - 1);
    size_t alloc_size = Alignment + sizeof(char*) + meta_info_size + size;
    LOG0 << "aligned_dealloc<" << Alignment << ">(), ptr = " << ptr
         << ", buffer = " << static_cast<void*>(buffer);
    base_.deallocate(buffer, alloc_size);
}

/******************************************************************************/
// default aligned allocation methods

static inline
void * aligned_alloc(size_t size, size_t meta_info_size = 0) {
    return AlignedAllocator<>().allocate_bytes(size, meta_info_size);
}

static inline
void aligned_dealloc(void* ptr, size_t size, size_t meta_info_size = 0) {
    return AlignedAllocator<>().deallocate_bytes(ptr, size, meta_info_size);
}

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_ALIGNED_ALLOCATOR_HEADER

/******************************************************************************/
