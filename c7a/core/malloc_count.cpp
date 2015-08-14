/*******************************************************************************
 * c7a/core/malloc_count.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <c7a/core/malloc_count.hpp>

#include <dlfcn.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <atomic>

namespace c7a {
namespace core {

//! user-defined options for output malloc()/free() operations to stderr

static const int log_operations = 0;    //! <-- set this to 1 for log output
static const size_t log_operations_threshold = 1024 * 1024;

//! to each allocation additional data is added for bookkeeping. due to
//! alignment requirements, we can optionally add more than just one integer.
static const size_t alignment = 16; /* bytes (>= 2*sizeof(size_t)) */

//! function pointer to the real procedures, loaded using dlsym
typedef void* (* malloc_type)(size_t);
typedef void (* free_type)(void*);
typedef void* (* realloc_type)(void*, size_t);

static malloc_type real_malloc = NULL;
static free_type real_free = NULL;
static realloc_type real_realloc = NULL;

//! a sentinel value prefixed to each allocation
static const size_t sentinel = 0xDEADC0DE;

//! a simple memory heap for allocations prior to dlsym loading
#define INIT_HEAP_SIZE 1024 * 1024
static char init_heap[INIT_HEAP_SIZE];
static size_t init_heap_use = 0;
static const int log_operations_init_heap = 0;

//! output
#define PPREFIX "malloc_count ### "

/*****************************************/
/* run-time memory allocation statistics */
/*****************************************/

static std::atomic<size_t> peak = { 0 };
static std::atomic<size_t> curr = { 0 };
static std::atomic<size_t> total = { 0 };
static std::atomic<size_t> num_allocs = { 0 };

//! add allocation to statistics
static void inc_count(size_t inc) {
    size_t mycurr = (curr += inc);
    if (mycurr > peak) peak = mycurr;
    total += inc;
    ++num_allocs;
}

//! decrement allocation to statistics
static void dec_count(size_t dec) {
    curr -= dec;
}

//! user function to return the currently allocated amount of memory
size_t malloc_count_current() {
    return curr;
}

//! user function to return the peak allocation
size_t malloc_count_peak() {
    return peak;
}

//! user function to reset the peak allocation to current
void malloc_count_reset_peak() {
    peak = curr.load();
}

//! user function to return total number of allocations
size_t malloc_count_num_allocs() {
    return num_allocs;
}

//! user function which prints current and peak allocation to stderr
void malloc_count_print_status() {
    fprintf(stderr, PPREFIX "current %lu, peak %lu\n",
            curr.load(), peak.load());
}

static __attribute__ ((constructor)) void init() {
    char* error;

    dlerror();

    real_malloc = (malloc_type)dlsym(RTLD_NEXT, "malloc");
    if ((error = dlerror()) != NULL) {
        fprintf(stderr, PPREFIX "error %s\n", error);
        exit(EXIT_FAILURE);
    }

    real_realloc = (realloc_type)dlsym(RTLD_NEXT, "realloc");
    if ((error = dlerror()) != NULL) {
        fprintf(stderr, PPREFIX "error %s\n", error);
        exit(EXIT_FAILURE);
    }

    real_free = (free_type)dlsym(RTLD_NEXT, "free");
    if ((error = dlerror()) != NULL) {
        fprintf(stderr, PPREFIX "error %s\n", error);
        exit(EXIT_FAILURE);
    }
}

static __attribute__ ((destructor)) void finish() {
    fprintf(stderr, PPREFIX
            "exiting, total: %lu, peak: %lu, current: %lu\n",
            total.load(), peak.load(), curr.load());
}

} // namespace core
} // namespace c7a

/****************************************************/
/* exported symbols that overlay the libc functions */
/****************************************************/

using namespace c7a::core; // NOLINT

//! exported malloc symbol that overrides loading from libc
void * malloc(size_t size) noexcept {
    void* ret;

    if (real_malloc)
    {
        //! call read malloc procedure in libc
        ret = (*real_malloc)(alignment + size);

        inc_count(size);
        if (log_operations && size >= log_operations_threshold) {
            fprintf(stderr, PPREFIX "malloc(%lu) = %p   (current %lu)\n",
                    size, static_cast<char*>(ret) + alignment, curr.load());
        }

        //! prepend allocation size and check sentinel
        *reinterpret_cast<size_t*>(ret) = size;
        *reinterpret_cast<size_t*>(
            static_cast<char*>(ret) + alignment - sizeof(size_t)) = sentinel;

        return static_cast<char*>(ret) + alignment;
    }
    else
    {
        if (init_heap_use + alignment + size > INIT_HEAP_SIZE) {
            fprintf(stderr, PPREFIX "init heap full !!!\n");
            exit(EXIT_FAILURE);
        }

        ret = init_heap + init_heap_use;
        init_heap_use += alignment + size;

        //! prepend allocation size and check sentinel
        *reinterpret_cast<size_t*>(ret) = size;
        *reinterpret_cast<size_t*>(
            static_cast<char*>(ret) + alignment - sizeof(size_t)) = sentinel;

        if (log_operations_init_heap) {
            fprintf(stderr, PPREFIX "malloc(%lu) = %p   on init heap\n",
                    size, static_cast<char*>(ret) + alignment);
        }

        return static_cast<char*>(ret) + alignment;
    }
}

//! exported free symbol that overrides loading from libc
void free(void* ptr) noexcept {
    size_t size;

    if (!ptr) return;   //! free(NULL) is no operation

    if (static_cast<char*>(ptr) >= init_heap &&
        static_cast<char*>(ptr) <= init_heap + init_heap_use)
    {
        if (log_operations_init_heap) {
            fprintf(stderr, PPREFIX "free(%p)   on init heap\n", ptr);
        }
        return;
    }

    if (!real_free) {
        fprintf(stderr, PPREFIX
                "free(%p) outside init heap and without real_free !!!\n", ptr);
        return;
    }

    ptr = static_cast<char*>(ptr) - alignment;

    if (*reinterpret_cast<size_t*>(
            static_cast<char*>(ptr) + alignment - sizeof(size_t)) != sentinel) {
        fprintf(stderr, PPREFIX
                "free(%p) has no sentinel !!! memory corruption?\n", ptr);
    }

    size = *reinterpret_cast<size_t*>(ptr);
    dec_count(size);

    if (log_operations && size >= log_operations_threshold) {
        fprintf(stderr, PPREFIX "free(%p) -> %lu   (current %lu)\n",
                ptr, size, curr.load());
    }

    (*real_free)(ptr);
}

//! exported calloc() symbol that overrides loading from libc, implemented using
//! our malloc
void * calloc(size_t nmemb, size_t size) noexcept {
    void* ret;
    size *= nmemb;
    if (!size) return NULL;
    ret = malloc(size);
    memset(ret, 0, size);
    return ret;
}

//! exported realloc() symbol that overrides loading from libc
void * realloc(void* ptr, size_t size) noexcept {
    void* newptr;
    size_t oldsize;

    if (static_cast<char*>(ptr) >= static_cast<char*>(init_heap) &&
        static_cast<char*>(ptr) <= static_cast<char*>(init_heap) + init_heap_use)
    {
        if (log_operations_init_heap) {
            fprintf(stderr, PPREFIX "realloc(%p) = on init heap\n", ptr);
        }

        ptr = static_cast<char*>(ptr) - alignment;

        if (*reinterpret_cast<size_t*>(
                static_cast<char*>(ptr) + alignment - sizeof(size_t)) != sentinel) {
            fprintf(stderr, PPREFIX
                    "realloc(%p) has no sentinel !!! memory corruption?\n",
                    ptr);
        }

        oldsize = *reinterpret_cast<size_t*>(ptr);

        if (oldsize >= size) {
            //! keep old area, just reduce the size
            *reinterpret_cast<size_t*>(ptr) = size;
            return static_cast<char*>(ptr) + alignment;
        }
        else {
            //! allocate new area and copy data
            ptr = static_cast<char*>(ptr) + alignment;
            newptr = malloc(size);
            memcpy(newptr, ptr, oldsize);
            free(ptr);
            return newptr;
        }
    }

    if (size == 0) { //! special case size == 0 -> free()
        free(ptr);
        return NULL;
    }

    if (ptr == NULL) { //! special case ptr == 0 -> malloc()
        return malloc(size);
    }

    ptr = static_cast<char*>(ptr) - alignment;

    if (*reinterpret_cast<size_t*>(
            static_cast<char*>(ptr) + alignment - sizeof(size_t)) != sentinel) {
        fprintf(stderr, PPREFIX
                "free(%p) has no sentinel !!! memory corruption?\n", ptr);
    }

    oldsize = *reinterpret_cast<size_t*>(ptr);

    dec_count(oldsize);
    inc_count(size);

    newptr = (*real_realloc)(ptr, alignment + size);

    if (log_operations && size >= log_operations_threshold)
    {
        if (newptr == ptr)
            fprintf(stderr, PPREFIX
                    "realloc(%lu -> %lu) = %p   (current %lu)\n",
                    oldsize, size, newptr, curr.load());
        else
            fprintf(stderr, PPREFIX
                    "realloc(%lu -> %lu) = %p -> %p   (current %lu)\n",
                    oldsize, size, ptr, newptr, curr.load());
    }

    *reinterpret_cast<size_t*>(newptr) = size;

    return static_cast<char*>(newptr) + alignment;
}

/******************************************************************************/
