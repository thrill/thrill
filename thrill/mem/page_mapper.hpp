/*******************************************************************************
 * thrill/mem/page_mapper.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Tobias Sturm <mail@tobiassturm.de>
 *
 * This file has no license. Only Chuck Norris can compile it.
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
#include <libexplain/lseek.h>
#endif

#include <queue>

#include <thrill/common/logger.hpp>
#include <thrill/common/concurrent_queue.hpp>
#include <thrill/data/byte_block.hpp>

namespace thrill {
namespace mem {

/*! The PageMapper maps objects onto disk using mmap and madvise and acts as
 * a wrapper for the c syscalls.
 *
 * OBJECT_SIZE must be divideable by page size
 */
template<size_t OBJECT_SIZE>
class PageMapper
{
public:
    //! when swap file is streched, it will be streched
    //! for min(1, min_growth_delta) objects
    static const size_t min_growth_delta = 0;

    //! temporal swap file
    static constexpr const char* swap_file_path = "/tmp/thrill.swap";

    //! Creates a PageMapper for objects of given size.
    //! Removes and creates a temporal file (PageMapper::swap_file_path)
    //! Checks if the object size is valid
    PageMapper() {
        //runtime check if OBJECT_SIZE is correct
        die_unless(OBJECT_SIZE % page_size() == 0);

        //create our swapfile
        //- read + write access
        //- create the file
        //- delete content if file exists
        //- this is gonna be a large file-> use 64bit ptrs
        //- don't update access time
        static const int flags = O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE | O_NOATIME;

        // user can read+write, group may read
        static const int permission = S_IRUSR | S_IWUSR | S_IRGRP;

        std::remove(swap_file_path);
        fd_ = open64(swap_file_path, flags, permission);
        die_unless(fd_ != -1);
    }

    //! Allocates a memory region of OBJECT_SIZE with a
    //! file-backing. Returns the memory address of this region and a
    //! result_token that can be used to address the memory region.
    uint8_t* Allocate(size_t& result_token) {
        sLOG << "allocate memory w/ disk backing";
        result_token = next_free_token();
        return SwapIn(result_token, false /*prefetch*/);
    }


    //! Releases an allocated token.
    //!
    //! Does NOT write memory content back to disk. Use PageMapper::SwapOut
    //! instead. Make sure to call this only when the matching memory region
    //! has been writen back before (PageMapper::SwapOut).
    //!
    //!\param token that was returned from Allocate before
    void ReleaseToken(size_t token) {
        sLOG << "free swap token" << token;
        free_tokens_.push(token);
    }

    //! Swaps out a given memory region. The memory region will be invalidated and
    //! is not accessible afterwards. Only memory regions that have been
    //! allocated via PageMapper::Allocate can be swapped out.
    //!
    //! \param addr that was returned by PageMapper::Allocate before
    //! \param write_back set to false if memory region's content may be
    //!        dismissed
    void SwapOut(uint8_t* addr, bool write_back = true) {
        //we might sometimes not write back, if we want to unmap a block but
        //don't care about the content (block for net send operation)
        if(write_back) {
            sLOG << "writing back" << static_cast<void*>(addr);
            die_unless(msync(static_cast<void*>(addr), OBJECT_SIZE, MS_SYNC) == 0);
        }
        sLOG << "unmapping " << static_cast<void*>(addr);
        die_unless(munmap(static_cast<void*>(addr), OBJECT_SIZE) == 0);
    }

    //! Swaps in a given memory region.
    //! \param token that was returned from Allocate before
    //! \returns pointer to memory region
    uint8_t* SwapIn(size_t token, bool prefetch = true) const {
        //Flags exaplained:
        //- readable
        //- writeable
        static const int protection_flags = PROT_READ | PROT_WRITE;
        //- not shared with other processes -> no automatic writebacks
        //- no swap space reserved (we like to live dangerously)
        //TODO we want MAP_HUGE_2MB
        int flags = MAP_SHARED | MAP_NORESERVE;

        //we don't want to prefetch when we allocate a fresh mapping
        if (prefetch)
            flags |= MAP_POPULATE;

        static void* addr_hint = nullptr; //we give no hint - kernel decides alone
        off64_t offset = token * OBJECT_SIZE;

        void* result = mmap64(addr_hint, OBJECT_SIZE, protection_flags, flags, fd_, offset);
        sLOG << "swapping in token" << token << "to address" << result << "into offset" << offset << "prefetch?" << prefetch;
        die_unless(result != MAP_FAILED);
        return static_cast<uint8_t*>(result);
    }

    //! Hint that the object at the specified memory region is likely to be
    //! accessed in sequential order
    void WillNeed(uint8_t* addr) const {
        madvise(static_cast<void*>(addr), OBJECT_SIZE, MADV_SEQUENTIAL | MADV_WILLNEED);
    }

    //! Hint that the object at the specified memory region is likely not to
    //! be used.
    void WillNotNeed(uint8_t* addr) const {
        madvise(static_cast<void*>(addr), OBJECT_SIZE, MADV_DONTNEED);
    }

    //! Returns the page size. OBJECT_SIZE must be a multiple of the page size.
    static size_t page_size() {
        return sysconf(_SC_PAGESIZE);
    }

private:
    static const bool debug = false;
    int fd_;
    size_t next_token_ = { 0 };
    common::ConcurrentQueue<size_t, std::allocator<size_t>> free_tokens_;

    //! Returns the next free token and eventually streches the swapfile if
    //! required
    size_t next_free_token() {
        size_t result = 0;
        if (free_tokens_.try_pop(result)) {
            sLOG << "reuse swap token" << result;
            return result;
        }

        //remember result
        result = next_token_;

        //+1 since result is 0-based
        size_t file_size = (1 + min_growth_delta + result) * OBJECT_SIZE;
        sLOG << "streching swap file to" << file_size;

        //seek to end - 1 of file and write one zero-byte to 'strech' file
        die_unless(lseek64(fd_, file_size - 1, SEEK_SET) != -1);
        die_unless(write(fd_, "\0", 1) == 1); //expect 1byte written

        //push remaining allocated ids into free-queue
        for(size_t token = result + 1; token <= next_token_ + min_growth_delta ; token++) {
            sLOG << "create new swap token" << token;
            free_tokens_.push(token);
        }

        next_token_++;
        sLOG << "use swap token" << result;
        return result;
    }
};

} // namespace mem
} // namespace thrill

#endif // ! THRILL_MEM_PAGE_MAPPER_HEADER

/******************************************************************************/
