/*******************************************************************************
 * thrill/mem/aligned_alloc.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_ALIGNED_ALLOC_HEADER
#define THRILL_MEM_ALIGNED_ALLOC_HEADER

#include <thrill/common/logger.hpp>

#include <cassert>
#include <cstdlib>

#ifndef STXXL_VERBOSE_ALIGNED_ALLOC
#define STXXL_VERBOSE_ALIGNED_ALLOC STXXL_VERBOSE2
#endif

namespace thrill {
namespace mem {

template <typename MustBeInt>
struct aligned_alloc_settings {
    static bool may_use_realloc;
};

template <typename MustBeInt>
bool aligned_alloc_settings<MustBeInt>::may_use_realloc = true;

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

template <size_t Alignment>
inline void * aligned_alloc(size_t size, size_t meta_info_size = 0) {
    static const bool debug = false;

    LOG << "aligned_alloc<" << Alignment << ">(), size = " << size
        << ", meta info size = " << meta_info_size;

#if !defined(STXXL_WASTE_MORE_MEMORY_FOR_IMPROVED_ACCESS_AFTER_ALLOCATED_MEMORY_CHECKS)
    // malloc()/realloc() variant that frees the unused amount of memory
    // after the data area of size 'size'. realloc() from valgrind does not
    // preserve the old memory area when shrinking, so out-of-bounds
    // accesses can't be detected easily.
    // Overhead: about Alignment bytes.
    size_t alloc_size = Alignment + sizeof(char*) + meta_info_size + size;
    char* buffer = static_cast<char*>(std::malloc(alloc_size));
#else
    // More space consuming and memory fragmenting variant using
    // posix_memalign() instead of malloc()/realloc(). Ensures that the end
    // of the data area (of size 'size') will match the end of the allocated
    // block, so no corrections are neccessary and
    // access-behind-allocated-memory problems can be easily detected by
    // valgrind. Usually produces an extra memory fragment of about
    // Alignment bytes.
    // Overhead: about 2 * Alignment bytes.
    size_t alloc_size = Alignment * div_ceil(sizeof(char*) + meta_info_size, Alignment) + size;
    char* buffer;
    if (posix_memalign(static_cast<void**>(&buffer), Alignment, alloc_size) != 0)
        throw std::bad_alloc();
#endif
    if (buffer == nullptr)
        throw std::bad_alloc();
#ifdef STXXL_ALIGNED_CALLOC
    memset(buffer, 0, alloc_size);
#endif
    char* reserve_buffer = buffer + sizeof(char*) + meta_info_size;
    char* result = reserve_buffer + Alignment -
                   (((size_t)reserve_buffer) % (Alignment)) - meta_info_size;
    LOG << "aligned_alloc<" << Alignment << ">() address " << static_cast<void*>(result)
        << " lost " << (result - buffer) << " bytes";
    //-tb: check that there is space for one char* before the "result" pointer
    // delivered to the user. this char* is set below to the beginning of the
    // allocated area.
    assert(long(result - buffer) >= long(sizeof(char*)));

    // free unused memory behind the data area
    // so access behind the requested size can be recognized
    size_t realloc_size = (result - buffer) + meta_info_size + size;
    if (realloc_size < alloc_size && aligned_alloc_settings<int>::may_use_realloc) {
        char* realloced = static_cast<char*>(std::realloc(buffer, realloc_size));
        if (buffer != realloced) {
            // hmm, realloc does move the memory block around while shrinking,
            // might run under valgrind, so disable realloc and retry
            LOG1 << "stxxl::aligned_alloc: disabling realloc()";
            std::free(realloced);
            aligned_alloc_settings<int>::may_use_realloc = false;
            return aligned_alloc<Alignment>(size, meta_info_size);
        }
        assert(result + size <= buffer + realloc_size);
    }

    *((reinterpret_cast<char**>(result)) - 1) = buffer;
    LOG << "aligned_alloc<" << Alignment << ">(), allocated at "
        << static_cast<void*>(buffer) << " returning " << static_cast<void*>(result);
    LOG << "aligned_alloc<" << Alignment << ">(size = " << size
        << ", meta info size = " << meta_info_size
        << ") => buffer = " << static_cast<void*>(buffer) << ", ptr = " << static_cast<void*>(result);

    return result;
}

template <size_t Alignment>
inline void
aligned_dealloc(void* ptr) {
    if (!ptr)
        return;
    char* buffer = *((reinterpret_cast<char**>(ptr)) - 1);
    LOG0 << "aligned_dealloc<" << Alignment << ">(), ptr = " << ptr
         << ", buffer = " << static_cast<void*>(buffer);
    std::free(buffer);
}

} // namespace mem
} // namespace thrill

#endif // !THRILL_MEM_ALIGNED_ALLOC_HEADER

/******************************************************************************/
