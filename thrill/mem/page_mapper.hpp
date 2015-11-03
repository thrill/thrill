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

// aliases - MAC does not support these flags
#ifdef __APPLE__
#define O_NOATIME 0
#define O_LARGEFILE 0
#define MAP_POPULATE 0
#endif

#include <fcntl.h>           //open
#include <stdio.h>           //remove
#include <sys/mman.h>        //mappings + advice
#include <sys/stat.h>        //open
#include <sys/types.h>       //open
#include <unistd.h>          //sysconfig

#ifdef USE_EXPLAIN
#include <libexplain/lseek.h>
#include <libexplain/mmap.h> // explain mmap errors
#endif

#include <mutex>
#include <queue>

#include <thrill/common/concurrent_queue.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/byte_block.hpp>

namespace thrill {
namespace mem {

/*! The PageMapper maps objects onto disk using mmap and madvise and acts as
 * a wrapper for the c syscalls.
 *
 * OBJECT_SIZE must be divideable by page size
 */
template <size_t OBJECT_SIZE>
class PageMapper
{
public:
    //! when swap file is streched, it will be streched
    //! for min(1, min_growth_delta) objects
    static const uint32_t min_growth_delta = 0;

    //! Creates a PageMapper for objects of given size.
    //! Removes and creates a temporal file (PageMapper::swap_file_name)
    //! Checks if the object size is valid
    PageMapper(std::string swap_file_name) : swap_file_name_(swap_file_name) {
        // runtime check if OBJECT_SIZE is correct
        die_unless(OBJECT_SIZE % page_size() == 0);

        // create our swap file
        //- read + write access
        //- create the file
        //- delete content if file exists
        //- this is gonna be a large file-> use 64bit ptrs
        //- don't update access time
        static int flags = O_RDWR | O_CREAT | O_TRUNC | O_LARGEFILE | O_NOATIME;

        // user can read+write, group may read
        static const int permission = S_IRUSR | S_IWUSR | S_IRGRP;

        sLOG << "creating swapfile" << swap_file_name;
        fd_ = open(swap_file_name.c_str(), flags, permission);
        die_unless(fd_ != -1);
    }

    //! Removes the swap file
    ~PageMapper() {
        remove(swap_file_name_.c_str());
    }

    //! Allocates a memory region of OBJECT_SIZE with a
    //! file-backing. Returns the memory address of this region and a
    //! result_token that can be used to address the memory region.
    //! \param result_token is the will be filled with the token associated with the block
    uint8_t * Allocate(uint32_t& result_token) {
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
    //! \param token that was returned from Allocate before
    void ReleaseToken(uint32_t token) {
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
        // we might sometimes not write back, if we want to unmap a block but
        // don't care about the content (block for net send operation)
        if (write_back) {
            sLOG << "writing back" << static_cast<void*>(addr);
            die_unless(msync(static_cast<void*>(addr), OBJECT_SIZE, MS_SYNC) == 0);
        }
        sLOG << "unmapping " << static_cast<void*>(addr);
        die_unless(munmap(static_cast<void*>(addr), OBJECT_SIZE) == 0);
    }

    //! Swaps in a given memory region.
    //! \param token that was returned from Allocate before
    //! \returns pointer to memory region
    uint8_t * SwapIn(uint32_t token, bool prefetch = true) const {
        // Flags exaplained:
        //- readable
        //- writeable
        static const int protection_flags = PROT_READ | PROT_WRITE;
        //- not shared with other processes -> no automatic writebacks
        //- no swap space reserved (we like to live dangerously)
        // TODO we want MAP_HUGE_2MB
        int flags = MAP_SHARED | MAP_NORESERVE;

        // we don't want to prefetch when we allocate a fresh mapping
        if (prefetch)
            flags |= MAP_POPULATE;

        static void* addr_hint = nullptr; //we give no hint - kernel decides alone
        off_t offset = token * OBJECT_SIZE;

        void* result = mmap(addr_hint, OBJECT_SIZE, protection_flags, flags, fd_, offset);
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
    std::string swap_file_name_;
    int fd_;
    size_t next_token_ = { 0 };
    std::mutex mutex_;
    common::ConcurrentQueue<uint32_t, std::allocator<uint32_t> > free_tokens_;

    //! Returns the next free token and eventually streches the swap file if
    //! required
    uint32_t next_free_token() {
        uint32_t result = 0;
        if (free_tokens_.try_pop(result)) {
            sLOG << "reuse swap token" << result;
            return result;
        }
        std::lock_guard<std::mutex> lock(mutex_);

        // remember result
        result = next_token_;

        // +1 since result is 0-based
        size_t file_size = (1 + min_growth_delta + result) * OBJECT_SIZE;
        sLOG << "streching swap file to" << file_size;

        // seek to end - 1 of file and write one zero-byte to 'strech' file
        die_unless(lseek(fd_, file_size - 1, SEEK_SET) != -1);
        die_unless(write(fd_, "\0", 1) == 1); //expect 1byte written

        // push remaining allocated ids into free-queue
        for (uint32_t token = result + 1; token <= next_token_ + min_growth_delta; token++) {
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

#endif // !THRILL_MEM_PAGE_MAPPER_HEADER

/******************************************************************************/
