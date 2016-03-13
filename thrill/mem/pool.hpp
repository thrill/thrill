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

#include <thrill/common/die.hpp>
#include <thrill/common/splay_tree.hpp>
#include <thrill/mem/allocator_base.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
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
template <size_t ArenaSize,
          typename BaseAllocator = std::allocator<char> >
class Pool
{
    static constexpr bool debug = false;
    static constexpr bool debug_check = true;
    static constexpr size_t check_limit = 4 * 1024 * 1024;

    static_assert(sizeof(typename BaseAllocator::value_type) == 1,
                  "BaseAllocator must be a char/byte allocator");

public:
    //! construct with base allocator
    explicit Pool(const BaseAllocator& base = BaseAllocator()) noexcept
        : base_(base) {
        if (debug_check)
            allocs_.resize(check_limit, std::make_pair(nullptr, 0));
    }

    //! non-copyable: delete copy-constructor
    Pool(const Pool&) = delete;
    //! non-copyable: delete assignment operator
    Pool& operator = (const Pool&) = delete;
    //! move-constructor
    Pool(Pool&& pool) noexcept
        : base_(pool.base_), free_arena_(pool.free_arena_),
          free_(pool.free_), size_(pool.size_) {
        pool.free_arena_ = nullptr;
    }
    //! move-assignment
    Pool& operator = (Pool&& pool) noexcept {
        if (this == &pool) return *this;
        DeallocateAll();
        base_ = pool.base_;
        free_arena_ = pool.free_arena_;
        free_ = pool.free_, size_ = pool.size_;
        pool.free_arena_ = nullptr;
        pool.free_ = pool.size_ = 0;
        return *this;
    }

    ~Pool() noexcept {
        std::unique_lock<std::mutex> lock(mutex_);
        if (size_ != 0) {
            std::cout << "~Pool() still contains "
                      << sizeof(Slot) * size_ << " bytes" << std::endl;

            for (size_t i = 0; i < allocs_.size(); ++i) {
                if (allocs_[i].first == nullptr) continue;
                std::cout << "~Pool() has ptr=" << allocs_[i].first
                          << " size=" << allocs_[i].second << std::endl;
            }
        }
        assert(size_ == 0);
        DeallocateAll();
    }

    //! allocate a continuous segment of n bytes in the arenas
    void * allocate(size_t n) {
        std::unique_lock<std::mutex> lock(mutex_);

        if (debug) {
            std::cout << "allocate() n=" << n
                      << " kSlotsPerArena=" << size_t(kSlotsPerArena)
                      << std::endl;
        }

        assert(n <= max_size());
        if (n > max_size())
            throw std::bad_alloc();

        // round up to whole slot size, and divide by slot size
        size_t orig_size = n;
        n = (n + sizeof(Slot) - 1) / sizeof(Slot);

        Arena* curr_arena = free_arena_;

        if (curr_arena == nullptr || free_ < n)
            curr_arena = AllocateFreeArena();

        while (curr_arena != nullptr)
        {
            // find an arena with at least n free slots
            if (curr_arena->head_slot.size >= n)
            {
                // iterative over free areas to find a possible fit
                Slot* prev_slot = &curr_arena->head_slot;
                Slot* curr_slot = curr_arena->slots + prev_slot->next;

                while (curr_slot != curr_arena->end() && curr_slot->size < n) {
                    prev_slot = curr_slot;
                    curr_slot = curr_arena->slots + curr_slot->next;
                }

                // if curr_slot == end then no continuous area was found.
                if (curr_slot != curr_arena->end())
                {
                    curr_arena->head_slot.size -= n;

                    prev_slot->next += n;
                    size_ += n;
                    free_ -= n;

                    if (curr_slot->size > n) {
                        // splits free area, since it is larger than needed
                        Slot* next_slot = curr_arena->slots + prev_slot->next;
                        assert(next_slot != curr_arena->end());

                        next_slot->size = curr_slot->size - n;
                        next_slot->next = curr_slot->next;
                    }
                    else {
                        // join used areas
                        prev_slot->next = curr_slot->next;
                    }

                    print();

                    if (debug_check) {
                        size_t i;
                        for (i = 0; i < allocs_.size(); ++i) {
                            if (allocs_[i].first == nullptr) {
                                allocs_[i].first = curr_slot;
                                allocs_[i].second = orig_size;
                                break;
                            }
                        }
                        if (i == allocs_.size()) {
                            assert(!"Increase allocs array in Pool().");
                            abort();
                        }
                    }

                    return reinterpret_cast<void*>(curr_slot);
                }
            }

            // advance to next arena in free list order
            if (debug) {
                std::cout << "advance next arena"
                          << " curr_arena=" << curr_arena
                          << " next_arena=" << curr_arena->next_arena
                          << std::endl;
            }

            curr_arena = curr_arena->next_arena;

            if (curr_arena == nullptr)
                curr_arena = AllocateFreeArena();
        }
        abort();
    }

    //! Deallocate a continuous segment of n bytes in the arenas, the size n
    //! *MUST* match the allocation.
    void deallocate(void* ptr, size_t n) {
        if (ptr == nullptr) return;

        std::unique_lock<std::mutex> lock(mutex_);
        if (debug) {
            std::cout << "deallocate() ptr" << ptr << "n" << n << std::endl;
        }
        if (debug_check) {
            size_t i;
            for (i = 0; i < allocs_.size(); ++i) {
                if (allocs_[i].first != ptr) continue;
                if (n != allocs_[i].second) {
                    assert(!"Mismatching deallocate() size in Pool().");
                    abort();
                }
                allocs_[i].first = nullptr;
                allocs_[i].second = 0;
                break;
            }
            if (i == allocs_.size()) {
                assert(!"Unknown deallocate() in Pool().");
                abort();
            }
        }

        // round up to whole slot size, and divide by slot size
        n = (n + sizeof(Slot) - 1) / sizeof(Slot);
        assert(n <= size_);

        // splay arenas to find arena containing ptr
        root_arena_ = common::splay(ptr, root_arena_, ArenaCompare());

        if (!(ptr >= root_arena_->begin() && ptr < root_arena_->end())) {
            assert(!"deallocate() of memory not in any arena.");
            abort();
        }

        Slot* prev_slot = &root_arena_->head_slot;
        Slot* ptr_slot = reinterpret_cast<Slot*>(ptr);

        // advance prev_slot until the next jumps over ptr.
        while (root_arena_->slots + prev_slot->next < ptr_slot) {
            prev_slot = root_arena_->slots + prev_slot->next;
        }

        // fill deallocated slot with free information
        ptr_slot->next = prev_slot->next;
        ptr_slot->size = n;

        prev_slot->next = ptr_slot - root_arena_->slots;

        // defragment free slots, but exempt the head_slot
        if (prev_slot == &root_arena_->head_slot)
            prev_slot = root_arena_->slots + root_arena_->head_slot.next;

        while (prev_slot->next != kSlotsPerArena &&
               prev_slot->next == prev_slot - root_arena_->slots + prev_slot->size)
        {
            // join free slots
            Slot* next_slot = root_arena_->slots + prev_slot->next;
            prev_slot->size += next_slot->size;
            prev_slot->next = next_slot->next;
        }

        root_arena_->head_slot.size += n;
        size_ -= n;
        free_ += n;

        if (root_arena_->head_slot.size == kSlotsPerArena)
        {
            // remove current arena from tree
            Arena* root = common::splay_erase_top(root_arena_, ArenaCompare());

            // splice current arena from free list
            if (root->prev_arena)
                root->prev_arena->next_arena = root->next_arena;
            if (root->next_arena)
                root->next_arena->prev_arena = root->prev_arena;
            if (free_arena_ == root)
                free_arena_ = root->next_arena;

            base_.deallocate(reinterpret_cast<char*>(root), ArenaSize);
            free_ -= kSlotsPerArena;
        }

        print();
    }

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
    void print() {

        if (debug) {
            std::cout << "Pool::print()"
                      << " size_=" << size_ << " free_=" << free_ << std::endl;
        }

        size_t total_free = 0, total_size = 0;

        for (Arena* curr_arena = free_arena_; curr_arena != nullptr;
             curr_arena = curr_arena->next_arena)
        {
            std::ostringstream oss;

            size_t slot = curr_arena->head_slot.next;
            size_t size = 0, free = 0;

            // used area at beginning
            size += slot;

            while (slot != kSlotsPerArena) {
                if (debug)
                    oss << " slot[" << slot
                        << ",size=" << curr_arena->slots[slot].size
                        << ",next=" << curr_arena->slots[slot].next << ']';

                if (curr_arena->slots[slot].next <= slot) {
                    std::cout << "invalid chain:" << oss.str() << std::endl;
                    abort();
                }

                free += curr_arena->slots[slot].size;
                size += curr_arena->slots[slot].next - slot - curr_arena->slots[slot].size;
                slot = curr_arena->slots[slot].next;
            }

            if (debug) {
                std::cout << "arena[" << curr_arena << "]"
                          << " head_slot.(free)size=" << curr_arena->head_slot.size
                          << " head_slot.next=" << curr_arena->head_slot.next
                          << oss.str()
                          << std::endl;
            }

            die_unequal(curr_arena->head_slot.size, free);

            total_free += free;
            total_size += size;

            if (curr_arena->next_arena)
                die_unequal(curr_arena->next_arena->prev_arena, curr_arena);
            if (curr_arena->prev_arena)
                die_unequal(curr_arena->prev_arena->next_arena, curr_arena);
        }
        die_unequal(total_size, size_);
        die_unequal(total_free, free_);
    }

    //! maximum size possible to allocate
    size_t max_size() const noexcept {
        return kSlotsPerArena * sizeof(Slot);
    }

private:
    // forward declaration
    struct Arena;

    //! struct in a Slot, which contains free information
    struct Slot {
        uint32_t size;
        uint32_t next;
    };

    //! header of an Arena, used to calculate number of slots
    struct Header {
        // next and prev pointers for free list.
        Arena* next_arena, * prev_arena;
        // left and right pointers for splay tree
        Arena* left = nullptr, * right = nullptr;
        Slot head_slot;
    };

    static constexpr size_t kSlotsPerArena =
        (ArenaSize - sizeof(Header)) / sizeof(Slot);

    //! structure of an Arena
    struct Arena : public Header
    {
        Slot slots[kSlotsPerArena];

        Slot * begin() { return slots; }
        Slot * end() { return slots + kSlotsPerArena; }
    };

    static_assert(ArenaSize >= sizeof(Arena), "ArenaSize too small.");

    //! comparison function for splay tree
    struct ArenaCompare {
        bool operator () (const Arena* a, const void* ptr) const {
            return a + 1 /* (= ArenaSize) */ < ptr;
        }
        bool operator () (const void* ptr, const Arena* a) const {
            return ptr < a;
        }
        bool operator () (const Arena* a, const Arena* b) const {
            return a < b;
        }
    };

    //! base allocator
    BaseAllocator base_;

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

    //! array of allocations for checking
    std::vector<std::pair<void*, size_t> > allocs_;

    //! allocate a new Arena blob
    Arena * AllocateFreeArena() {
        if (debug) {
            std::cout << "AllocateFreeArena()" << std::endl;
        }

        // Allocate space for the new block
        Arena* new_arena = reinterpret_cast<Arena*>(base_.allocate(ArenaSize));
        new_arena->next_arena = free_arena_;
        new_arena->prev_arena = nullptr;
        if (free_arena_)
            free_arena_->prev_arena = new_arena;
        free_arena_ = new_arena;

        new_arena->head_slot.size = kSlotsPerArena;
        new_arena->head_slot.next = 0;

        new_arena->slots[0].size = kSlotsPerArena;
        new_arena->slots[0].next = kSlotsPerArena;

        free_ += kSlotsPerArena;

        root_arena_ = common::splay(new_arena, root_arena_, ArenaCompare());
        root_arena_ = common::splay_insert(new_arena, root_arena_, ArenaCompare());

        return new_arena;
    }

    void DeallocateAll() {
        Arena* curr_arena = free_arena_;
        while (curr_arena != nullptr) {
            Arena* next_arena = curr_arena->next_arena;
            base_.deallocate(reinterpret_cast<char*>(curr_arena), ArenaSize);
            curr_arena = next_arena;
        }
    }
};

//! singleton instance of global pool for I/O data structures
extern Pool<16384> g_pool;

/******************************************************************************/
// PoolAllocator - an allocator to draw objects from a Pool.

template <typename Type, size_t ArenaSize = 16384>
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

    using Pool = mem::Pool<ArenaSize>;

    //! Return allocator for different type.
    template <typename U>
    struct rebind { using other = PoolAllocator<U, ArenaSize>; };

    //! construct PoolAllocator with Pool object
    explicit PoolAllocator(Pool& pool) noexcept
        : pool_(&pool) { }

    //! copy-constructor
    PoolAllocator(const PoolAllocator&) noexcept = default;

    //! copy-constructor from a rebound allocator
    template <typename OtherType>
    PoolAllocator(const PoolAllocator<OtherType, ArenaSize>& other) noexcept
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
        Type* r;
        while ((r = static_cast<Type*>(
                    pool_->allocate(n * sizeof(Type)))) == nullptr)
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
    bool operator == (const PoolAllocator<Other, ArenaSize>& other) const noexcept {
        return (pool_ == other.pool_);
    }

    //! compare to another allocator of same type
    template <typename Other>
    bool operator != (const PoolAllocator<Other, ArenaSize>& other) const noexcept {
        return (pool_ != other.pool_);
    }
};

/******************************************************************************/
// FixedPoolAllocator - an allocator to draw objects from a fixed Pool.

template <typename Type, Pool<16384>& pool_>
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
        return pool_.max_size();
    }

    //! attempts to allocate a block of storage with a size large enough to
    //! contain n elements of member type value_type, and returns a pointer to
    //! the first element.
    pointer allocate(size_type n, const void* /* hint */ = nullptr) {
        Type* r;
        while ((r = static_cast<Type*>(
                    pool_.allocate(n * sizeof(Type)))) == nullptr)
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

    //! releases a block of storage previously allocated with member allocate
    //! and not yet released.
    void deallocate(pointer p, size_type n) const noexcept {
        pool_.deallocate(p, n * sizeof(Type));
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
using GPoolAllocator = FixedPoolAllocator<Type, g_pool>;

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_POOL_HEADER

/******************************************************************************/
