/*******************************************************************************
 * thrill/mem/page_mapper.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_MEM_PAGE_MAPPER_HEADER
#define THRILL_MEM_PAGE_MAPPER_HEADER

#include <sys/types.h> //open
#include <sys/stat.h>  //open
#include <fcntl.h>     //open
#include <unistd.h>    //sysconfig
#include <stdio.h>     //remove file
#include <sys/mman.h>  //mappings + advice

#ifdef USE_EXPLAIN
#include <libexplain/mmap.h> // explain mmap errors
#endif

#include <queue>

#include <thrill/common/logger.hpp>
#include <thrill/data/byte_block.hpp>

namespace thrill {
namespace mem {

/*! The PageMapper maps objects onto disk using mmap and madvise and acts as
 * a mapper for the c syscalls.
 *
 * No temporal file is needed (anonymous mapping).
 * object_size_ must be divideable by page size
 */
class PageMapper
{
public:

    //! Creates a PageMapper for objects of given size.
    //! Checks if the object size is valid
    PageMapper(size_t object_size = thrill::data::default_block_size) : object_size_(object_size) {
        //runtime check if object_size_ is correct
        long page_size = sysconf(_SC_PAGESIZE);
        die_unless(object_size % page_size == 0);
    }

    char* Allocate() {
        //Flags exaplained:
        //- readable
        //- writeable
        //- not shared with other processes -> no automatic writebacks
        //- use huge pages (2MB on my machine) - not sure about this
        //- don't zero out pages
        //- anonymous = not backed by a file :)
        static const int protection_flags = PROT_READ | PROT_WRITE;
        static const int flags = MAP_PRIVATE | MAP_HUGETLB | MAP_ANONYMOUS;
        static void* addr_hint = nullptr; //we give no hint - kernel decides alone
        static const int fd = -1; //for portability
        static const size_t offset = 0; //for portability
        void* result = mmap(addr_hint, object_size_, protection_flags, flags, fd, offset);
        die_unless(result != MAP_FAILED);
        return static_cast<char*>(result);
    }

    void Free(char* addr) {
        die_unless(munmap(addr, object_size_) == 0);
    }

    void SwapOut(char* addr) {
        // flags explained in order
        // - don't need pages in near future
        static const int flags = MADV_DONTNEED;
        int result = madvise(addr, object_size_, flags);
        die_unless(result == EAGAIN || result == 0);
    }

    void Prefetch(char* addr) {
        // flags explained in order
        // - aggressig prefetching for sequential read access
        // - will need pages in the memmory region -> plz prefetch Mr Kernel.
        static const int flags = MADV_SEQUENTIAL | MADV_WILLNEED;
        int result = madvise(addr, object_size_, flags);
        die_unless(result == EAGAIN || result == 0);
    }

    //! Returns the object_size that is used for this mapper
    size_t object_size() const noexcept {
        return object_size_;
    }

private:
    size_t object_size_;
};

} // namespace mem
} // namespace thrill

#endif // ! THRILL_MEM_PAGE_MAPPER_HEADER

/******************************************************************************/
