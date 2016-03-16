/*******************************************************************************
 * thrill/mem/pool.hpp
 *
 * A simple memory allocation manager and object allocator. The main reason for
 * this allocation Pool is to keep memory for allocation of I/O data structures
 * once the main malloc() memory pool runs out. The allocator itself may not be
 * as faster as possible.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_POOL_HEADER
#define THRILL_MEM_POOL_HEADER

#include <thrill/common/config.hpp>
#include <thrill/mem/allocator_base.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace thrill {
namespace mem {

/******************************************************************************/
// Pool - a simple memory allocation manager

/*!
 * A simple memory allocation manager. The Pool gets chunks of memory of size
 * ArenaSize from new/delete and delivers smaller byte areas to PoolAllocator.
 *
 * The main reason for this allocation Pool is to keep memory for allocation of
 * I/O data structures once the main malloc() memory pool runs out. The
 * allocator itself may not be as faster as possible.
 *
 * An Arena is organized as a singly-linked list of continuous free areas, where
 * the free information is stored inside the free memory as a Slot. A Slot is
 * exactly 8 bytes (containing only two uint32_t). All allocations are rounded
 * up to a multiple of 8.
 *
 * +--+-----------+------+---------------+-----------+-------------------------+
 * |XX| head_slot | used | free slot ... | used .... | free slot .......       |
 * +--+-----------+------+---------------+-----------+-------------------------+
 *      |                  ^  |                        ^
 *      +------------------+  +------------------------+ (next indexes)
 *
 * - XX = additional Header information
 *
 * - head_slot = sentinel Slot containing .size as the free slots, and .next to
 *   the first free slot.
 *
 * - each free Slot contains .size and .next. Size is the amount of free slots
 *   in a free area. Next is the integer offset of the following free slot
 *   (information) or the end of the Arena.
 *
 * During allocation the next fitting free slot is searched for. During
 * deallocation multiple free areas may be consolidated.
 */
class Pool
{
    static constexpr bool debug = false;
    static constexpr bool debug_verify = false;
    //! debug flag to check pairing of allocate()/deallocate() client calls
    static constexpr bool debug_check_pairing = false;
    static constexpr size_t check_limit = 4 * 1024 * 1024;

    static constexpr size_t min_free = 1024 * 1024 / 8;

public:
    //! construct with base allocator
    explicit Pool(size_t default_arena_size = 16384) noexcept;

    //! non-copyable: delete copy-constructor
    Pool(const Pool&) = delete;
    //! non-copyable: delete assignment operator
    Pool& operator = (const Pool&) = delete;
    //! move-constructor
    Pool(Pool&& pool) noexcept;
    //! move-assignment
    Pool& operator = (Pool&& pool) noexcept;

    //! dtor
    ~Pool() noexcept;

    //! allocate a continuous segment of n bytes in the arenas
    void * allocate(size_t n);

    //! Deallocate a continuous segment of n bytes in the arenas, the size n
    //! *MUST* match the allocation.
    void deallocate(void* ptr, size_t n);

    //! Allocate and construct a single item of given Type using memory from the
    //! Pool.
    template <typename Type, typename ... Args>
    Type * make(Args&& ... args) {
        Type* t = reinterpret_cast<Type*>(allocate(sizeof(Type)));
        ::new (t)Type(std::forward<Args>(args) ...);
        return t;
    }

    //! Destroy and deallocate a single item of given Type.
    template <typename Type>
    void destroy(Type* t) {
        t->~Type();
        deallocate(t, sizeof(Type));
    }

    //! Print out structure of the arenas.
    void print();

    //! maximum size possible to allocate
    size_t max_size() const noexcept;

private:
    //! struct in a Slot, which contains free information
    struct Slot;

    //! header of an Arena, used to calculate number of slots
    struct Arena;

    //! comparison function for splay tree
    struct ArenaCompare;

    //! mutex to protect data structures (remove this if you use it in another
    //! context than Thrill).
    std::mutex mutex_;

    //! pointer to first arena, arenas are in allocation order
    Arena* free_arena_ = nullptr;
    //! pointer to root arena in splay tree
    Arena* root_arena_ = nullptr;

    //! number of free slots in the arenas
    size_t free_ = 0;
    //! overall number of used slots
    size_t size_ = 0;

    //! size of default Arena allocation
    size_t default_arena_size_;

    //! array of allocations for checking
    std::vector<std::pair<void*, size_t> > allocs_;

    //! calculate maximum bytes fitting into an Arena with given size.
    size_t bytes_per_arena(size_t arena_size);

    //! allocate a new Arena blob
    Arena * AllocateFreeArena(size_t arena_size, bool die_on_failure = true);

    //! deallocate all Arenas
    void DeallocateAll();
};

//! singleton instance of global pool for I/O data structures
Pool & GPool();

/******************************************************************************/
// PoolAllocator - an allocator to draw objects from a Pool.

template <typename Type>
class PoolAllocator : public AllocatorBase<Type>
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

    //! Return allocator for different type.
    template <typename U>
    struct rebind { using other = PoolAllocator<U>; };

    //! construct PoolAllocator with Pool object
    explicit PoolAllocator(Pool& pool) noexcept
        : pool_(&pool) { }

    //! copy-constructor
    PoolAllocator(const PoolAllocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    PoolAllocator(const PoolAllocator<OtherType>& other) noexcept
        : pool_(other.pool_) { }

    //! copy-assignment operator
    PoolAllocator& operator = (const PoolAllocator&) noexcept = default;

    //! maximum size possible to allocate
    size_type max_size() const noexcept {
        return pool_->max_size();
    }

    //! attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        return static_cast<Type*>(pool_->allocate(n * sizeof(Type)));
    }

    //! releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type n) const noexcept {
        pool_->deallocate(p, n * sizeof(Type));
    }

    //! pointer to common Pool object. if we use a reference here, then the
    //! allocator cannot be default move/assigned anymore.
    Pool* pool_;

    //! compare to another allocator of same type
    template <typename Other>
    bool operator == (const PoolAllocator<Other>& other) const noexcept {
        return (pool_ == other.pool_);
    }

    //! compare to another allocator of same type
    template <typename Other>
    bool operator != (const PoolAllocator<Other>& other) const noexcept {
        return (pool_ != other.pool_);
    }
};

/******************************************************************************/
// FixedPoolAllocator - an allocator to draw objects from a fixed Pool.

template <typename Type, Pool& (* pool_)()>
class FixedPoolAllocator : public AllocatorBase<Type>
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
    using is_always_equal = std::true_type;

    //! Return allocator for different type.
    template <typename U>
    struct rebind { using other = FixedPoolAllocator<U, pool_>; };

    //! construct FixedPoolAllocator with Pool object
    FixedPoolAllocator() noexcept = default;

    //! copy-constructor
    FixedPoolAllocator(const FixedPoolAllocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    FixedPoolAllocator(const FixedPoolAllocator<OtherType, pool_>&) noexcept { }

    //! copy-assignment operator
    FixedPoolAllocator& operator = (const FixedPoolAllocator&) noexcept = default;

    //! maximum size possible to allocate
    size_type max_size() const noexcept {
        return pool_().max_size();
    }

    //! attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        return static_cast<Type*>(pool_().allocate(n * sizeof(Type)));
    }

    //! releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type n) const noexcept {
        pool_().deallocate(p, n * sizeof(Type));
    }

    //! compare to another allocator of same type
    template <typename Other>
    bool operator == (const FixedPoolAllocator<Other, pool_>&) const noexcept {
        return true;
    }

    //! compare to another allocator of same type
    template <typename Other>
    bool operator != (const FixedPoolAllocator<Other, pool_>&) const noexcept {
        return true;
    }
};

//! template alias for allocating from mem::g_pool.
template <typename Type>
using GPoolAllocator = FixedPoolAllocator<Type, GPool>;

//! deleter for unique_ptr with memory from mem::g_pool.
template <typename T>
class GPoolDeleter
{
public:
    //! free the pointer
    void operator () (T* ptr) const noexcept {
        GPool().destroy(ptr);
    }
};

//! unique_ptr with memory from mem::g_pool.
template <typename T>
using safe_unique_ptr = std::unique_ptr<T, GPoolDeleter<T> >;

//! make_unique with Manager tracking
template <typename T, typename ... Args>
safe_unique_ptr<T> safe_make_unique(Args&& ... args) {
    return safe_unique_ptr<T>(
        GPool().make<T>(std::forward<Args>(args) ...));
}

//! alias for std::string except that its memory is allocated in the safer
//! g_pool.
using safe_string = std::basic_string<
          char, std::char_traits<char>, mem::GPoolAllocator<char> >;

//! alias for std::ostringstream except that it uses the safer g_pool as
//! allocator for the internal string buffer
using safe_ostringstream = std::basic_ostringstream<
          char, std::char_traits<char>, mem::GPoolAllocator<char> >;

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_POOL_HEADER

/******************************************************************************/
