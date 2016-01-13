/*******************************************************************************
 * thrill/mem/malloc_tracker.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <thrill/mem/malloc_tracker.hpp>

#if __linux__ || __APPLE__ || __FreeBSD__

#include <dlfcn.h>

#endif

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#if defined(__clang__) || defined (__GNUC__)

#define ATTRIBUTE_NO_SANITIZE_ADDRESS \
    __attribute__ ((no_sanitize_address)) /* NOLINT */

#if defined (__GNUC__) && __GNUC__ >= 5
#define ATTRIBUTE_NO_SANITIZE_THREAD \
    __attribute__ ((no_sanitize_thread))  /* NOLINT */
#else
#define ATTRIBUTE_NO_SANITIZE_THREAD
#endif

#define ATTRIBUTE_NO_SANITIZE \
    ATTRIBUTE_NO_SANITIZE_ADDRESS ATTRIBUTE_NO_SANITIZE_THREAD

#else
#define ATTRIBUTE_NO_SANITIZE
#endif

namespace thrill {
namespace mem {

//! user-defined options for output malloc()/free() operations to stderr

static const int log_operations = 0; //! <-- set this to 1 for log output
static const size_t log_operations_threshold = 1;

#define LOG_MALLOC_PROFILER 0

//! to each allocation additional data is added for bookkeeping.
static const size_t padding = 16;    /* bytes (>= 2*sizeof(size_t)) */

//! function pointer to the real procedures, loaded using dlsym()
using malloc_type = void* (*)(size_t);
using free_type = void (*)(void*);
using realloc_type = void* (*)(void*, size_t);

static malloc_type real_malloc = nullptr;
static free_type real_free = nullptr;
static realloc_type real_realloc = nullptr;

//! a sentinel value prefixed to each allocation
static const size_t sentinel = 0xDEADC0DE;

//! CounterType is used for atomic counters, and get to retrieve their contents
#if defined(_MSC_VER)
using CounterType = std::atomic<size_t>;
size_t get(const CounterType& a) {
    return a.load();
}
#else
// we cannot use std::atomic on gcc/clang because only real atomic instructions
// work with the Sanitizers
using CounterType = size_t;
size_t get(const CounterType& a) {
    return a;
}
#endif

//! a simple memory heap for allocations prior to dlsym loading
#define INIT_HEAP_SIZE 1024 * 1024
static char init_heap[INIT_HEAP_SIZE];
static CounterType init_heap_use = 0;
static const int log_operations_init_heap = 0;

//! align allocations to init_heap to this number by rounding up allocations
static const size_t init_alignment = sizeof(size_t);

//! output
#define PPREFIX "malloc_tracker ### "

/*****************************************/
/* run-time memory allocation statistics */
/*****************************************/

static CounterType peak = 0;
static CounterType curr = 0;
static CounterType total = 0;
static CounterType total_allocs = 0;
static CounterType current_allocs = 0;

//! add allocation to statistics
ATTRIBUTE_NO_SANITIZE
static void inc_count(size_t inc) {
#if defined(_MSC_VER)
    size_t mycurr = (curr += inc);
#else
    size_t mycurr = __sync_add_and_fetch(&curr, inc);
#endif
    if (mycurr > peak) peak = mycurr;
    total += inc;
    ++total_allocs;
    ++current_allocs;
}

//! decrement allocation to statistics
ATTRIBUTE_NO_SANITIZE
static void dec_count(size_t dec) {
    curr -= dec;
    --current_allocs;
}

//! bypass malloc tracker and access malloc() directly
void * bypass_malloc(size_t size) noexcept {
#if defined(_MSC_VER)
    return malloc(size);
#else
    return real_malloc(size);
#endif
}

//! bypass malloc tracker and access free() directly
void bypass_free(void* ptr) noexcept {
#if defined(_MSC_VER)
    return free(ptr);
#else
    return real_free(ptr);
#endif
}

//! user function to return the currently allocated amount of memory
size_t malloc_tracker_current() {
    return curr;
}

//! user function to return the peak allocation
size_t malloc_tracker_peak() {
    return peak;
}

//! user function to reset the peak allocation to current
void malloc_tracker_reset_peak() {
    peak = get(curr);
}

//! user function to return total number of allocations
size_t malloc_tracker_total_allocs() {
    return total_allocs;
}

//! user function which prints current and peak allocation to stderr
void malloc_tracker_print_status() {
    fprintf(stderr, PPREFIX "current %zu, peak %zu\n",
            get(curr), get(peak));
}

#if __linux__ || __APPLE__ || __FreeBSD__

ATTRIBUTE_NO_SANITIZE
static __attribute__ ((constructor)) void init() { // NOLINT

    // try to use AddressSanitizer's malloc first.
    real_malloc = (malloc_type)dlsym(RTLD_DEFAULT, "__interceptor_malloc");
    if (real_malloc)
    {
        real_realloc = (realloc_type)dlsym(RTLD_DEFAULT, "__interceptor_realloc");
        if (!real_realloc) {
            fprintf(stderr, PPREFIX "dlerror %s\n", dlerror());
            exit(EXIT_FAILURE);
        }

        real_free = (free_type)dlsym(RTLD_DEFAULT, "__interceptor_free");
        if (!real_free) {
            fprintf(stderr, PPREFIX "dlerror %s\n", dlerror());
            exit(EXIT_FAILURE);
        }

        fprintf(stderr, PPREFIX "using AddressSanitizer's malloc\n");
        return;
    }

    real_malloc = (malloc_type)dlsym(RTLD_NEXT, "malloc");
    if (!real_malloc) {
        fprintf(stderr, PPREFIX "dlerror %s\n", dlerror());
        exit(EXIT_FAILURE);
    }

    real_realloc = (realloc_type)dlsym(RTLD_NEXT, "realloc");
    if (!real_realloc) {
        fprintf(stderr, PPREFIX "dlerror %s\n", dlerror());
        exit(EXIT_FAILURE);
    }

    real_free = (free_type)dlsym(RTLD_NEXT, "free");
    if (!real_free) {
        fprintf(stderr, PPREFIX "dlerror %s\n", dlerror());
        exit(EXIT_FAILURE);
    }
}

ATTRIBUTE_NO_SANITIZE
static __attribute__ ((destructor)) void finish() { // NOLINT
    fprintf(stderr, PPREFIX
            "exiting, total: %zu, peak: %zu, current: %zu, "
            "allocs: %zu, unfreed: %zu\n",
            get(total), get(peak), get(curr),
            get(total_allocs), get(current_allocs));
}

#endif

} // namespace mem
} // namespace thrill

/****************************************************/
/* exported symbols that overlay the libc functions */
/****************************************************/

using namespace thrill::mem; // NOLINT

ATTRIBUTE_NO_SANITIZE
static void * preinit_malloc(size_t size) noexcept {

    size_t aligned_size = size + (init_alignment - size % init_alignment);

#if defined(_MSC_VER)
    size_t offset = (init_heap_use += (padding + aligned_size));
#else
    size_t offset = __sync_fetch_and_add(&init_heap_use, padding + aligned_size);
#endif

    if (offset > INIT_HEAP_SIZE) {
        fprintf(stderr, PPREFIX "init heap full !!!\n");
        exit(EXIT_FAILURE);
    }

    char* ret = init_heap + offset;

    //! prepend allocation size and check sentinel
    *reinterpret_cast<size_t*>(ret) = aligned_size;
    *reinterpret_cast<size_t*>(ret + padding - sizeof(size_t)) = sentinel;

    inc_count(aligned_size);

    if (log_operations_init_heap) {
        fprintf(stderr, PPREFIX "malloc(%zu / %zu) = %p   on init heap\n",
                size, aligned_size, static_cast<void*>(ret + padding));
    }

    return ret + padding;
}

ATTRIBUTE_NO_SANITIZE
static void * preinit_realloc(void* ptr, size_t size) {

    if (log_operations_init_heap) {
        fprintf(stderr, PPREFIX "realloc(%p) = on init heap\n", ptr);
    }

    ptr = static_cast<char*>(ptr) - padding;

    if (*reinterpret_cast<size_t*>(
            static_cast<char*>(ptr) + padding - sizeof(size_t)) != sentinel) {
        fprintf(stderr, PPREFIX
                "realloc(%p) has no sentinel !!! memory corruption?\n",
                ptr);
    }

    size_t oldsize = *reinterpret_cast<size_t*>(ptr);

    if (oldsize >= size) {
        //! keep old area
        return static_cast<char*>(ptr) + padding;
    }
    else {
        //! allocate new area and copy data
        ptr = static_cast<char*>(ptr) + padding;
        void* newptr = malloc(size);
        memcpy(newptr, ptr, oldsize);
        free(ptr);
        return newptr;
    }
}

ATTRIBUTE_NO_SANITIZE
static void preinit_free(void* ptr) {
    // don't do any real deallocation.

    ptr = static_cast<char*>(ptr) - padding;

    if (*reinterpret_cast<size_t*>(
            static_cast<char*>(ptr) + padding - sizeof(size_t)) != sentinel) {
        fprintf(stderr, PPREFIX
                "free(%p) has no sentinel !!! memory corruption?\n",
                ptr);
    }

    size_t size = *reinterpret_cast<size_t*>(ptr);
    dec_count(size);

    if (log_operations_init_heap) {
        fprintf(stderr, PPREFIX "free(%p) -> %zu   on init heap\n", ptr, size);
    }
}

#if __APPLE__

#define NOEXCEPT
#define MALLOC_USABLE_SIZE malloc_size
#include <malloc/malloc.h>

#elif __FreeBSD__

#define NOEXCEPT
#define MALLOC_USABLE_SIZE malloc_usable_size
#include <malloc_np.h>

#elif __linux__

#define NOEXCEPT noexcept
#define MALLOC_USABLE_SIZE malloc_usable_size
#include <malloc.h>

#include <execinfo.h>
#include <unistd.h>

#endif

#if defined(MALLOC_USABLE_SIZE)

/*
 * This is a malloc() tracker implementation which uses an available system call
 * to determine the amount of memory used by an allocation (which may be more
 * than the allocated size). On Linux's glibc there is malloc_usable_size().
 */

//! exported malloc symbol that overrides loading from libc
ATTRIBUTE_NO_SANITIZE
void * malloc(size_t size) NOEXCEPT {

    if (!real_malloc)
        return preinit_malloc(size);

    //! call real malloc procedure in libc
    void* ret = (*real_malloc)(size);

    size_t size_used = MALLOC_USABLE_SIZE(ret);
    inc_count(size_used);

    if (log_operations && size_used >= log_operations_threshold) {
        fprintf(stderr, PPREFIX "malloc(%zu size / %zu used) = %p   (current %zu)\n",
                size, size_used, ret, curr);
    }
    {
#if __linux__ && LOG_MALLOC_PROFILER
        static thread_local bool recursive = false;

        if (size_used >= log_operations_threshold && !recursive) {
            recursive = true;

/*[[[perl
  # depth of call stack to print
  my $depth = 16;

  print <<EOF;
            // storage array for stack trace address data
            void* addrlist[$depth + 1];

            // retrieve current stack addresses
            int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

EOF

  for my $d (3...$depth-1) {
    print("fprintf(stdout, PPREFIX \"caller".(" %p"x$d)." size %zu\\n\",\n");
    print("        ".join("", map("addrlist[$_], ", 0...$d-1))." size);");
  }
  ]]]*/
            // storage array for stack trace address data
            void* addrlist[16 + 1];

            // retrieve current stack addresses
            int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

            fprintf(stdout, PPREFIX "caller %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], addrlist[7], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], addrlist[7], addrlist[8], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], addrlist[7], addrlist[8], addrlist[9], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], addrlist[7], addrlist[8], addrlist[9], addrlist[10], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], addrlist[7], addrlist[8], addrlist[9], addrlist[10], addrlist[11], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], addrlist[7], addrlist[8], addrlist[9], addrlist[10], addrlist[11], addrlist[12], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], addrlist[7], addrlist[8], addrlist[9], addrlist[10], addrlist[11], addrlist[12], addrlist[13], size);
            fprintf(stdout, PPREFIX "caller %p %p %p %p %p %p %p %p %p %p %p %p %p %p %p size %zu\n",
                    addrlist[0], addrlist[1], addrlist[2], addrlist[3], addrlist[4], addrlist[5], addrlist[6], addrlist[7], addrlist[8], addrlist[9], addrlist[10], addrlist[11], addrlist[12], addrlist[13], addrlist[14], size);
// [[[end]]]

            // fprintf(stderr, "--- begin stack -------------------------------\n");
            // backtrace_symbols_fd(addrlist, addrlen, STDERR_FILENO);
            // fprintf(stderr, "--- end stack ---------------------------------\n");

            recursive = false;
        }
#endif
    }

    return ret;
}

//! exported free symbol that overrides loading from libc
ATTRIBUTE_NO_SANITIZE
void free(void* ptr) NOEXCEPT {

    if (!ptr) return;   //! free(nullptr) is no operation

    if (static_cast<char*>(ptr) >= init_heap &&
        static_cast<char*>(ptr) <= init_heap + get(init_heap_use))
    {
        return preinit_free(ptr);
    }

    if (!real_free) {
        fprintf(stderr, PPREFIX
                "free(%p) outside init heap and without real_free !!!\n", ptr);
        return;
    }

    size_t size_used = MALLOC_USABLE_SIZE(ptr);
    dec_count(size_used);

    if (log_operations && size_used >= log_operations_threshold) {
        fprintf(stderr, PPREFIX "free(%p) -> %zu   (current %zu)\n",
                ptr, size_used, curr);
    }

    (*real_free)(ptr);
}

//! exported calloc() symbol that overrides loading from libc, implemented using
//! our malloc
ATTRIBUTE_NO_SANITIZE
void * calloc(size_t nmemb, size_t size) NOEXCEPT {
    size *= nmemb;
    void* ret = malloc(size);
    memset(ret, 0, size);
    return ret;
}

//! exported realloc() symbol that overrides loading from libc
ATTRIBUTE_NO_SANITIZE
void * realloc(void* ptr, size_t size) NOEXCEPT {

    if (static_cast<char*>(ptr) >= static_cast<char*>(init_heap) &&
        static_cast<char*>(ptr) <= static_cast<char*>(init_heap) + get(init_heap_use))
    {
        return preinit_realloc(ptr, size);
    }

    if (size == 0) { //! special case size == 0 -> free()
        free(ptr);
        return nullptr;
    }

    if (ptr == nullptr) { //! special case ptr == 0 -> malloc()
        return malloc(size);
    }

    size_t oldsize_used = MALLOC_USABLE_SIZE(ptr);
    dec_count(oldsize_used);

    void* newptr = (*real_realloc)(ptr, size);

    size_t newsize_used = MALLOC_USABLE_SIZE(newptr);
    inc_count(newsize_used);

    if (log_operations && newsize_used >= log_operations_threshold)
    {
        if (newptr == ptr)
            fprintf(stderr, PPREFIX
                    "realloc(%zu -> %zu / %zu) = %p   (current %zu)\n",
                    oldsize_used, size, newsize_used, newptr, curr);
        else
            fprintf(stderr, PPREFIX
                    "realloc(%zu -> %zu / %zu) = %p -> %p   (current %zu)\n",
                    oldsize_used, size, newsize_used, ptr, newptr, curr);
    }

    return newptr;
}

#elif !defined(_MSC_VER) // GENERIC IMPLEMENTATION for Unix

/*
 * This is a generic implementation to count memory allocation by prefixing
 * every user allocation with the size. On free, the size can be
 * retrieves. Obviously, this wastes lots of memory if there are many small
 * allocations.
 */

//! exported malloc symbol that overrides loading from libc
ATTRIBUTE_NO_SANITIZE
void * malloc(size_t size) NOEXCEPT {

    if (!real_malloc)
        return preinit_malloc(size);

    //! call real malloc procedure in libc
    void* ret = (*real_malloc)(padding + size);

    inc_count(size);
    if (log_operations && size >= log_operations_threshold) {
        fprintf(stderr, PPREFIX "malloc(%zu) = %p   (current %zu)\n",
                size, static_cast<char*>(ret) + padding, get(curr));
    }

    //! prepend allocation size and check sentinel
    *reinterpret_cast<size_t*>(ret) = size;
    *reinterpret_cast<size_t*>(
        static_cast<char*>(ret) + padding - sizeof(size_t)) = sentinel;

    return static_cast<char*>(ret) + padding;
}

//! exported free symbol that overrides loading from libc
ATTRIBUTE_NO_SANITIZE
void free(void* ptr) NOEXCEPT {

    if (!ptr) return;   //! free(nullptr) is no operation

    if (static_cast<char*>(ptr) >= init_heap &&
        static_cast<char*>(ptr) <= init_heap + get(init_heap_use))
    {
        return preinit_free(ptr);
    }

    if (!real_free) {
        fprintf(stderr, PPREFIX
                "free(%p) outside init heap and without real_free !!!\n", ptr);
        return;
    }

    ptr = static_cast<char*>(ptr) - padding;

    if (*reinterpret_cast<size_t*>(
            static_cast<char*>(ptr) + padding - sizeof(size_t)) != sentinel) {
        fprintf(stderr, PPREFIX
                "free(%p) has no sentinel !!! memory corruption?\n", ptr);
    }

    size_t size = *reinterpret_cast<size_t*>(ptr);
    dec_count(size);

    if (log_operations && size >= log_operations_threshold) {
        fprintf(stderr, PPREFIX "free(%p) -> %zu   (current %zu)\n",
                ptr, size, get(curr));
    }

    (*real_free)(ptr);
}

//! exported calloc() symbol that overrides loading from libc, implemented using
//! our malloc
ATTRIBUTE_NO_SANITIZE
void * calloc(size_t nmemb, size_t size) NOEXCEPT {
    size *= nmemb;
    if (!size) return nullptr;
    void* ret = malloc(size);
    memset(ret, 0, size);
    return ret;
}

//! exported realloc() symbol that overrides loading from libc
ATTRIBUTE_NO_SANITIZE
void * realloc(void* ptr, size_t size) NOEXCEPT {

    if (static_cast<char*>(ptr) >= static_cast<char*>(init_heap) &&
        static_cast<char*>(ptr) <= static_cast<char*>(init_heap) + get(init_heap_use))
    {
        return preinit_realloc(ptr, size);
    }

    if (size == 0) { //! special case size == 0 -> free()
        free(ptr);
        return nullptr;
    }

    if (ptr == nullptr) { //! special case ptr == 0 -> malloc()
        return malloc(size);
    }

    ptr = static_cast<char*>(ptr) - padding;

    if (*reinterpret_cast<size_t*>(
            static_cast<char*>(ptr) + padding - sizeof(size_t)) != sentinel) {
        fprintf(stderr, PPREFIX
                "free(%p) has no sentinel !!! memory corruption?\n", ptr);
    }

    size_t oldsize = *reinterpret_cast<size_t*>(ptr);

    dec_count(oldsize);
    inc_count(size);

    void* newptr = (*real_realloc)(ptr, padding + size);

    if (log_operations && size >= log_operations_threshold)
    {
        if (newptr == ptr)
            fprintf(stderr, PPREFIX
                    "realloc(%zu -> %zu) = %p   (current %zu)\n",
                    oldsize, size, newptr, get(curr));
        else
            fprintf(stderr, PPREFIX
                    "realloc(%zu -> %zu) = %p -> %p   (current %zu)\n",
                    oldsize, size, ptr, newptr, get(curr));
    }

    *reinterpret_cast<size_t*>(newptr) = size;

    return static_cast<char*>(newptr) + padding;
}

#else // if defined(_MSC_VER)

// TODO(tb): dont know how to override malloc/free.

#endif // IMPLEMENTATION SWITCH

/******************************************************************************/
