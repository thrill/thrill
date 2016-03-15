/*******************************************************************************
 * thrill/mem/pool.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/mem/pool.hpp>

#include <thrill/common/splay_tree.hpp>

#include <new>

namespace thrill {
namespace mem {

/******************************************************************************/

Pool g_pool;

/******************************************************************************/
// Pool::Arena

struct Pool::Slot {
    uint32_t size;
    uint32_t next;
};

struct Pool::Arena {
    //! total size of this Arena
    size_t total_size;
    // next and prev pointers for free list.
    Arena  * next_arena, * prev_arena;
    // left and right pointers for splay tree
    Arena  * left = nullptr, * right = nullptr;
    //! first sentinel Slot which is never used for payload data
    Slot   head_slot;
    // following here are actual data slots

    //! the number of available payload slots (excluding head_slot)
    uint32_t num_slots() const {
        return static_cast<uint32_t>(
            (total_size - sizeof(Arena)) / sizeof(Slot));
    }

    Slot * begin() { return &head_slot + 1; }
    Slot * end() { return &head_slot + 1 + num_slots(); }
    Slot * slot(size_t i) { return &head_slot + 1 + i; }
};

/******************************************************************************/
// Pool

Pool::Pool(size_t arena_size) noexcept
    : arena_size_(arena_size) {
    if (debug_check)
        allocs_.resize(check_limit);

    while (free_ < min_free)
        AllocateFreeArena();
}

Pool::Pool(Pool&& pool) noexcept
    : free_arena_(pool.free_arena_),
      free_(pool.free_), size_(pool.size_) {
    pool.free_arena_ = nullptr;
}

Pool& Pool::operator = (Pool&& pool) noexcept {
    if (this == &pool) return *this;
    DeallocateAll();
    free_arena_ = pool.free_arena_;
    free_ = pool.free_, size_ = pool.size_;
    pool.free_arena_ = nullptr;
    pool.free_ = pool.size_ = 0;
    return *this;
}

Pool::~Pool() noexcept {
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

struct Pool::ArenaCompare {
    bool operator () (const Arena* a, const void* ptr) const {
        return reinterpret_cast<const char*>(a) + a->total_size < ptr;
    }
    bool operator () (const void* ptr, const Arena* a) const {
        return ptr < a;
    }
    bool operator () (const Arena* a, const Arena* b) const {
        return a < b;
    }
};

Pool::Arena* Pool::AllocateFreeArena(bool die_on_failure) {
    if (debug) {
        std::cout << "AllocateFreeArena()" << std::endl;
    }

    // Allocate space for the new block
    Arena* new_arena =
        reinterpret_cast<Arena*>(bypass_malloc(arena_size_));
    if (!new_arena) {
        if (!die_on_failure) return nullptr;
        fprintf(stderr, "out-of-memory - mem::Pool cannot allocate a new Arena."
                " size_=%zu\n", size_);
        abort();
    }
    new_arena->total_size = arena_size_;
    new_arena->next_arena = free_arena_;
    new_arena->prev_arena = nullptr;
    if (free_arena_)
        free_arena_->prev_arena = new_arena;
    free_arena_ = new_arena;

    new_arena->head_slot.size = new_arena->num_slots();
    new_arena->head_slot.next = 0;

    new_arena->slot(0)->size = new_arena->num_slots();
    new_arena->slot(0)->next = new_arena->num_slots();

    free_ += new_arena->num_slots();

    root_arena_ = common::splay(new_arena, root_arena_, ArenaCompare());
    root_arena_ = common::splay_insert(new_arena, root_arena_, ArenaCompare());

    return new_arena;
}

void Pool::DeallocateAll() {
    Arena* curr_arena = free_arena_;
    while (curr_arena != nullptr) {
        Arena* next_arena = curr_arena->next_arena;
        bypass_free(curr_arena, curr_arena->total_size);
        curr_arena = next_arena;
    }
}

void* Pool::allocate(size_t n) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (debug) {
        std::cout << "allocate() n=" << n
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
            Slot* curr_slot = curr_arena->begin() + prev_slot->next;

            while (curr_slot != curr_arena->end() && curr_slot->size < n) {
                prev_slot = curr_slot;
                curr_slot = curr_arena->begin() + curr_slot->next;
            }

            // if curr_slot == end then no continuous area was found.
            if (curr_slot != curr_arena->end())
            {
                curr_arena->head_slot.size -= n;

                prev_slot->next += n;
                size_ += n;
                free_ -= n;

                while (free_ < min_free) {
                    if (!AllocateFreeArena(false)) break;
                }

                if (curr_slot->size > n) {
                    // splits free area, since it is larger than needed
                    Slot* next_slot = curr_arena->begin() + prev_slot->next;
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

void Pool::deallocate(void* ptr, size_t n) {
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
    while (root_arena_->begin() + prev_slot->next < ptr_slot) {
        prev_slot = root_arena_->begin() + prev_slot->next;
    }

    // fill deallocated slot with free information
    ptr_slot->next = prev_slot->next;
    ptr_slot->size = n;

    prev_slot->next = ptr_slot - root_arena_->begin();

    // defragment free slots, but exempt the head_slot
    if (prev_slot == &root_arena_->head_slot)
        prev_slot = root_arena_->begin() + root_arena_->head_slot.next;

    while (prev_slot->next != root_arena_->num_slots() &&
           prev_slot->next == prev_slot - root_arena_->begin() + prev_slot->size)
    {
        // join free slots
        Slot* next_slot = root_arena_->begin() + prev_slot->next;
        prev_slot->size += next_slot->size;
        prev_slot->next = next_slot->next;
    }

    root_arena_->head_slot.size += n;
    size_ -= n;
    free_ += n;

    if (root_arena_->head_slot.size == root_arena_->num_slots() &&
        free_ >= min_free + root_arena_->num_slots())
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

        free_ -= root->num_slots();
        bypass_free(root, root->total_size);
    }

    print();
}

void Pool::print() {
    if (!debug) return;

    std::cout << "Pool::print()"
              << " size_=" << size_ << " free_=" << free_ << std::endl;

    size_t total_free = 0, total_size = 0;

    for (Arena* curr_arena = free_arena_; curr_arena != nullptr;
         curr_arena = curr_arena->next_arena)
    {
        std::ostringstream oss;

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
            std::cout << "arena[" << curr_arena << "]"
                      << " head_slot.(free)size=" << curr_arena->head_slot.size
                      << " head_slot.next=" << curr_arena->head_slot.next
                      << oss.str()
                      << std::endl;
        }

        assert(curr_arena->head_slot.size == free);

        total_free += free;
        total_size += size;

        if (curr_arena->next_arena)
            assert(curr_arena->next_arena->prev_arena == curr_arena);
        if (curr_arena->prev_arena)
            assert(curr_arena->prev_arena->next_arena == curr_arena);
    }
    assert(total_size == size_);
    assert(total_free == free_);
}

} // namespace mem
} // namespace thrill

/******************************************************************************/
