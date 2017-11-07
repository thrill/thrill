/*******************************************************************************
 * thrill/mem/pool.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016-2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/mem/pool.hpp>

#include <tlx/die.hpp>
#include <tlx/math/integer_log2.hpp>

#include <iostream>
#include <limits>
#include <new>

namespace thrill {
namespace mem {

/******************************************************************************/

Pool& GPool() {
    static Pool* pool = new Pool();
    return *pool;
}

#if defined(__clang__) || defined(__GNUC__)
static __attribute__ ((destructor)) void s_gpool_destroy() { // NOLINT
    // deallocate memory arenas but do not destroy the pool
    GPool().DeallocateAll();
}
#endif

/******************************************************************************/
// Pool::Arena

struct Pool::Slot {
    uint32_t size;
    uint32_t next;
};

struct Pool::Arena {
    //! magic word
    size_t magic;
    //! total size of this Arena
    size_t total_size;
    //! next and prev pointers for free list.
    Arena  * next_arena, * prev_arena;
    //! oversize arena
    bool   oversize;
    //! first sentinel Slot which is never used for payload data, instead size =
    //! remaining free size, and next = pointer to first free byte.
    union {
        uint32_t free_size;
        Slot     head_slot;
    };
    // following here are actual data slots
    // Slot slots[num_slots()];

    //! the number of available payload slots (excluding head_slot)
    uint32_t num_slots() const {
        return static_cast<uint32_t>(
            (total_size - sizeof(Arena)) / sizeof(Slot));
    }

    Slot * begin() { return &head_slot + 1; }
    Slot * end() { return &head_slot + 1 + num_slots(); }
    Slot * slot(size_t i) { return &head_slot + 1 + i; }

    void * find_free(size_t size);
};

/******************************************************************************/
// internal methods

//! determine bin for size.
static inline size_t calc_bin_for_size(size_t size) {
    if (size == 0)
        return 0;
    else
        return 1 + tlx::integer_log2_floor_template(size);
}

//! lowest size still in bin
static inline size_t bin_lower_bound(size_t bin) {
    if (bin == 0)
        return 0;
    else
        return (size_t(1) << (bin - 1));
}

/******************************************************************************/
// Pool

Pool::Pool(size_t default_arena_size) noexcept
    : default_arena_size_(default_arena_size) {
    std::unique_lock<std::mutex> lock(mutex_);

    for (size_t i = 0; i < num_bins + 1; ++i)
        arena_bin_[i] = nullptr;

    if (debug_check_pairing)
        allocs_.resize(check_limit);

    while (free_ < min_free_)
        AllocateFreeArena(default_arena_size_);
}

Pool::~Pool() noexcept {
    std::unique_lock<std::mutex> lock(mutex_);
    if (size_ != 0) {
        std::cout << "~Pool() pool still contains "
                  << sizeof(Slot) * size_ << " bytes" << std::endl;

        for (size_t i = 0; i < allocs_.size(); ++i) {
            if (allocs_[i].first == nullptr) continue;
            std::cout << "~Pool() has ptr=" << allocs_[i].first
                      << " size=" << allocs_[i].second << std::endl;
        }
    }
    assert(size_ == 0);
    IntDeallocateAll();
}

void* Pool::ArenaFindFree(Arena* arena, size_t bin, size_t n, size_t bytes) {
    // iterative over free areas to find a possible fit
    Slot* prev_slot = &arena->head_slot;
    Slot* curr_slot = arena->begin() + prev_slot->next;

    while (curr_slot != arena->end() && curr_slot->size < n) {
        prev_slot = curr_slot;
        curr_slot = arena->begin() + curr_slot->next;
    }

    // if curr_slot == end, then no suitable continuous area was found.
    if (TLX_UNLIKELY(curr_slot == arena->end()))
        return nullptr;

    arena->free_size -= n;

    prev_slot->next += n;
    size_ += n;
    free_ -= n;

    if (curr_slot->size > n) {
        // splits free area, since it is larger than needed
        Slot* next_slot = arena->begin() + prev_slot->next;
        assert(next_slot != arena->end());

        next_slot->size = curr_slot->size - n;
        next_slot->next = curr_slot->next;
    }
    else {
        // join used areas
        prev_slot->next = curr_slot->next;
    }

    if (arena->free_size < bin_lower_bound(bin) && !arena->oversize) {
        // recategorize bin into smaller chain.

        size_t new_bin = calc_bin_for_size(arena->free_size);

        if (debug) {
            std::cout << "Recategorize arena, previous free "
                      << arena->free_size + n
                      << " now free " << arena->free_size
                      << " from bin " << bin
                      << " to bin " << new_bin
                      << std::endl;
        }
        assert(bin != new_bin);

        // splice out arena from current bin
        if (arena->prev_arena)
            arena->prev_arena->next_arena = arena->next_arena;
        else
            arena_bin_[bin] = arena->next_arena;

        if (arena->next_arena)
            arena->next_arena->prev_arena = arena->prev_arena;

        // insert at top of new bin
        arena->prev_arena = nullptr;
        arena->next_arena = arena_bin_[new_bin];
        if (arena_bin_[new_bin])
            arena_bin_[new_bin]->prev_arena = arena;
        arena_bin_[new_bin] = arena;
    }

    // allocate more sparse memory
    while (free_ < min_free_) {
        if (!AllocateFreeArena(default_arena_size_, false)) break;
    }

    if (debug_check_pairing) {
        size_t i;
        for (i = 0; i < allocs_.size(); ++i) {
            if (allocs_[i].first == nullptr) {
                allocs_[i].first = curr_slot;
                allocs_[i].second = bytes;
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

Pool::Arena* Pool::AllocateFreeArena(size_t arena_size, bool die_on_failure) {

    if (debug) {
        std::cout << "AllocateFreeArena()"
                  << " arena_size=" << arena_size
                  << " die_on_failure=" << die_on_failure << std::endl;
    }

    // Allocate space for the new block
    Arena* new_arena =
        reinterpret_cast<Arena*>(
            bypass_aligned_alloc(default_arena_size_, arena_size));
    if (!new_arena) {
        if (!die_on_failure) return nullptr;
        fprintf(stderr, "out-of-memory - mem::Pool cannot allocate a new Arena."
                " size_=%zu\n", size_);
        abort();
    }

    die_unequal(
        new_arena,
        reinterpret_cast<Arena*>(
            reinterpret_cast<uintptr_t>(new_arena) & ~(default_arena_size_ - 1)));

    new_arena->magic = 0xAEEAAEEAAEEAAEEALLU;
    new_arena->total_size = arena_size;

    // put new area into right chain at the front
    Arena** root;
    if (arena_size <= default_arena_size_) {
        size_t bin = calc_bin_for_size(new_arena->num_slots());
        die_unless(bin < num_bins);
        root = &arena_bin_[bin];
        new_arena->oversize = false;
    }
    else {
        root = &arena_bin_[num_bins];
        new_arena->oversize = true;
    }

    new_arena->prev_arena = nullptr;
    new_arena->next_arena = *root;
    if (*root)
        (*root)->prev_arena = new_arena;
    *root = new_arena;

    new_arena->free_size = new_arena->num_slots();
    new_arena->head_slot.next = 0;

    new_arena->slot(0)->size = new_arena->num_slots();
    new_arena->slot(0)->next = new_arena->num_slots();

    free_ += new_arena->num_slots();

    Arena* check_arena =
        reinterpret_cast<Arena*>(
            reinterpret_cast<uintptr_t>(new_arena) & ~(default_arena_size_ - 1));
    die_unless(check_arena->magic == 0xAEEAAEEAAEEAAEEALLU);

    return new_arena;
}

void Pool::DeallocateAll() {
    std::unique_lock<std::mutex> lock(mutex_);
    IntDeallocateAll();
}

void Pool::IntDeallocateAll() {
    for (size_t i = 0; i <= num_bins; ++i) {
        Arena* curr_arena = arena_bin_[i];
        while (curr_arena != nullptr) {
            Arena* next_arena = curr_arena->next_arena;
            bypass_aligned_free(curr_arena, curr_arena->total_size);
            curr_arena = next_arena;
        }
    }
    min_free_ = 0;
}

size_t Pool::max_size() const noexcept {
    return sizeof(Slot) * std::numeric_limits<uint32_t>::max();
}

size_t Pool::bytes_per_arena(size_t arena_size) {
    return arena_size - sizeof(Arena);
}

void* Pool::allocate(size_t bytes) {
    // return malloc(bytes);

    std::unique_lock<std::mutex> lock(mutex_);

    if (debug) {
        std::cout << "Pool::allocate() bytes " << bytes
                  << std::endl;
    }

    // round up to whole slot size, and divide by slot size
    uint32_t n =
        static_cast<uint32_t>((bytes + sizeof(Slot) - 1) / sizeof(Slot));

    // check whether n is too large for allocation in our default Arenas, then
    // allocate a special larger one.
    if (n * sizeof(Slot) > bytes_per_arena(default_arena_size_)) {
        if (debug) {
            std::cout << "Allocate overflow arena of size "
                      << n * sizeof(Slot) << std::endl;
        }
        Arena* sp_arena = AllocateFreeArena(sizeof(Arena) + n * sizeof(Slot));

        void* ptr = ArenaFindFree(sp_arena, num_bins, n, bytes);
        if (ptr != nullptr)
            return ptr;
    }

    // find bin for n slots
    size_t bin = calc_bin_for_size(n);
    while (bin < num_bins)
    {
        if (debug)
            std::cout << "Searching in bin " << bin << std::endl;

        Arena* curr_arena = arena_bin_[bin];

        while (curr_arena != nullptr)
        {
            // find an arena with at least n free slots
            if (curr_arena->free_size >= n)
            {
                void* ptr = ArenaFindFree(curr_arena, bin, n, bytes);
                if (ptr != nullptr)
                    return ptr;
            }

            // advance to next arena in free list order
            curr_arena = curr_arena->next_arena;
        }

        // look into larger bin
        ++bin;
    }

    // allocate new arena with default size
    Arena* curr_arena = AllocateFreeArena(default_arena_size_);
    bin = calc_bin_for_size(curr_arena->num_slots());

    // look into new arena
    void* ptr = ArenaFindFree(curr_arena, bin, n, bytes);
    if (ptr != nullptr)
        return ptr;

    die("Pool::allocate() failed, no memory available.");
}

void Pool::deallocate(void* ptr, size_t bytes) {
    // return free(ptr);

    if (ptr == nullptr) return;

    std::unique_lock<std::mutex> lock(mutex_);
    if (debug) {
        std::cout << "Pool::deallocate() ptr " << ptr
                  << " bytes " << bytes << std::endl;
    }

    if (debug_check_pairing) {
        size_t i;
        for (i = 0; i < allocs_.size(); ++i) {
            if (allocs_[i].first != ptr) continue;
            if (bytes != allocs_[i].second) {
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
    uint32_t n
        = static_cast<uint32_t>((bytes + sizeof(Slot) - 1) / sizeof(Slot));
    assert(n <= size_);

    // find arena containing ptr
    Arena* arena =
        reinterpret_cast<Arena*>(
            reinterpret_cast<uintptr_t>(ptr) & ~(default_arena_size_ - 1));
    die_unless(arena->magic == 0xAEEAAEEAAEEAAEEALLU);

    if (!(ptr >= arena->begin() && ptr < arena->end())) {
        assert(!"deallocate() of memory not in any arena.");
        abort();
    }

    Slot* prev_slot = &arena->head_slot;
    Slot* ptr_slot = reinterpret_cast<Slot*>(ptr);

    // advance prev_slot until the next jumps over ptr.
    while (arena->begin() + prev_slot->next < ptr_slot) {
        prev_slot = arena->begin() + prev_slot->next;
    }

    // fill deallocated slot with free information
    ptr_slot->next = prev_slot->next;
    ptr_slot->size = n;

    prev_slot->next = static_cast<uint32_t>(ptr_slot - arena->begin());

    // defragment free slots, but exempt the head_slot
    if (prev_slot == &arena->head_slot)
        prev_slot = arena->begin() + arena->head_slot.next;

    while (prev_slot->next != arena->num_slots() &&
           prev_slot->next == prev_slot - arena->begin() + prev_slot->size)
    {
        // join free slots
        Slot* next_slot = arena->begin() + prev_slot->next;
        prev_slot->size += next_slot->size;
        prev_slot->next = next_slot->next;
    }

    arena->free_size += n;
    size_ -= n;
    free_ += n;

    // always deallocate oversize arenas
    if (arena->oversize)
    {
        if (debug)
            std::cout << "destroy special arena" << std::endl;

        // splice out arena from current bin
        if (arena->prev_arena)
            arena->prev_arena->next_arena = arena->next_arena;
        else
            arena_bin_[num_bins] = arena->next_arena;

        if (arena->next_arena)
            arena->next_arena->prev_arena = arena->prev_arena;

        free_ -= arena->num_slots();
        bypass_aligned_free(arena, arena->total_size);
        return;
    }

    // check if this arena is empty and free_ space is beyond our min_free_
    // limit, then simply deallocate it.
    if (arena->free_size == arena->num_slots() &&
        free_ >= min_free_ + arena->num_slots())
    {
        if (debug)
            std::cout << "destroy empty arena" << std::endl;

        size_t bin = calc_bin_for_size(arena->free_size - n);

        // splice out arena from current bin
        if (arena->prev_arena)
            arena->prev_arena->next_arena = arena->next_arena;
        else
            arena_bin_[bin] = arena->next_arena;

        if (arena->next_arena)
            arena->next_arena->prev_arena = arena->prev_arena;

        // free arena
        free_ -= arena->num_slots();
        bypass_aligned_free(arena, arena->total_size);
        return;
    }

    if (calc_bin_for_size(arena->free_size - n) !=
        calc_bin_for_size(arena->free_size))
    {
        // recategorize arena into larger chain.
        if (debug)
            std::cout << "recategorize arena into larger chain." << std::endl;

        size_t bin = calc_bin_for_size(arena->free_size - n);
        size_t new_bin = calc_bin_for_size(arena->free_size);

        if (debug) {
            std::cout << "Recategorize arena, previous free "
                      << arena->free_size
                      << " now free " << arena->free_size + n
                      << " from bin " << bin
                      << " to bin " << new_bin
                      << std::endl;
        }

        // splice out arena from current bin
        if (arena->prev_arena)
            arena->prev_arena->next_arena = arena->next_arena;
        else
            arena_bin_[bin] = arena->next_arena;

        if (arena->next_arena)
            arena->next_arena->prev_arena = arena->prev_arena;

        // insert at top of new bin
        arena->prev_arena = nullptr;
        arena->next_arena = arena_bin_[new_bin];
        if (arena_bin_[new_bin])
            arena_bin_[new_bin]->prev_arena = arena;
        arena_bin_[new_bin] = arena;
    }
}

void Pool::print(bool debug) {
    if (debug) {
        std::cout << "Pool::print()"
                  << " size_=" << size_ << " free_=" << free_ << std::endl;
    }

    size_t total_free = 0, total_size = 0;

    for (size_t bin = 0; bin < num_bins; ++bin)
    {
        for (Arena* curr_arena = arena_bin_[bin]; curr_arena != nullptr;
             curr_arena = curr_arena->next_arena)
        {
            std::ostringstream oss;

            size_t arena_bin = calc_bin_for_size(curr_arena->free_size);
            die_unequal(arena_bin, bin);

            size_t slot = curr_arena->head_slot.next;
            size_t size = 0, free = 0;

            // used area at beginning
            size += slot;

            while (slot != curr_arena->num_slots()) {
                if (debug)
                    oss << " slot[" << slot
                        << ",size=" << curr_arena->slot(slot)->size
                        << ",next=" << curr_arena->slot(slot)->next << ']';

                if (curr_arena->slot(slot)->next <= slot) {
                    std::cout << "invalid chain:" << oss.str() << std::endl;
                    abort();
                }

                free += curr_arena->slot(slot)->size;
                size += curr_arena->slot(slot)->next - slot - curr_arena->slot(slot)->size;
                slot = curr_arena->slot(slot)->next;
            }

            if (debug) {
                std::cout << "arena[" << bin << ':' << curr_arena << "]"
                          << " free_size=" << curr_arena->free_size
                          << " head_slot.next=" << curr_arena->head_slot.next
                          << oss.str()
                          << std::endl;
            }

            die_unequal(curr_arena->head_slot.size, free);

            total_free += free;
            total_size += size;

            if (curr_arena->next_arena)
                die_unless(curr_arena->next_arena->prev_arena == curr_arena);
            if (curr_arena->prev_arena)
                die_unless(curr_arena->prev_arena->next_arena == curr_arena);
        }
    }

    die_unequal(total_size, size_);
    die_unequal(total_free, free_);
}

void Pool::self_verify() {
    print(false);
}

} // namespace mem
} // namespace thrill

/******************************************************************************/
