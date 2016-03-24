/*******************************************************************************
 * thrill/io/memory_file.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2013-2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/io/iostats.hpp>
#include <thrill/io/memory_file.hpp>
#include <thrill/mem/aligned_allocator.hpp>

#include <cassert>
#include <cstring>
#include <limits>

namespace thrill {
namespace io {

void MemoryFile::serve(void* buffer, offset_type offset, size_type bytes,
                       Request::ReadOrWriteType type) {
    std::unique_lock<std::mutex> lock(mutex_);

    if (type == Request::READ)
    {
        Stats::ScopedReadTimer read_timer(bytes);
        memcpy(buffer, ptr_ + offset, bytes);
    }
    else
    {
        Stats::ScopedWriteTimer write_timer(bytes);
        memcpy(ptr_ + offset, buffer, bytes);
    }
}

const char* MemoryFile::io_type() const {
    return "memory";
}

MemoryFile::~MemoryFile() {
    free(ptr_);
    ptr_ = nullptr;
}

void MemoryFile::lock() {
    // nothing to do
}

FileBase::offset_type MemoryFile::size() {
    return size_;
}

void MemoryFile::set_size(offset_type newsize) {
    std::unique_lock<std::mutex> lock(mutex_);
    assert(newsize <= std::numeric_limits<offset_type>::max());

    ptr_ = static_cast<char*>(realloc(ptr_, static_cast<size_t>(newsize)));
    size_ = newsize;
}

void MemoryFile::discard(offset_type offset, offset_type size) {
    std::unique_lock<std::mutex> lock(mutex_);
#ifndef THRILL_MEMFILE_DONT_CLEAR_FREED_MEMORY
    // overwrite the freed region with uninitialized memory
    LOG << "discard at " << offset << " len " << size;
    void* uninitialized = malloc(THRILL_DEFAULT_ALIGN);
    while (size >= THRILL_DEFAULT_ALIGN) {
        memcpy(ptr_ + offset, uninitialized, THRILL_DEFAULT_ALIGN);
        offset += THRILL_DEFAULT_ALIGN;
        size -= THRILL_DEFAULT_ALIGN;
    }
    assert(size <= std::numeric_limits<offset_type>::max());
    if (size > 0)
        memcpy(ptr_ + offset, uninitialized, (size_t)size);
    free(uninitialized);
#else
    common::THRILL_UNUSED(offset);
    common::THRILL_UNUSED(size);
#endif
}

} // namespace io
} // namespace thrill

/******************************************************************************/
