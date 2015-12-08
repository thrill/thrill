/*******************************************************************************
 * thrill/io/bid.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2004 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2009, 2010 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_BID_HEADER
#define THRILL_IO_BID_HEADER

#include <thrill/io/file.hpp>

#include <cstring>
#include <iomanip>
#include <ostream>
#include <vector>

namespace thrill {
namespace io {

//! \addtogroup mnglayer
//! \{

//! Block identifier class.
//!
//! Stores block identity, given by file and offset within the file
template <size_t Size>
struct BID
{
    enum
    {
        size = Size,    //!< Block size
        t_size = Size   //!< Blocks size, given by the parameter
    };

    file    * storage;  //!< pointer to the file of the block
    int64_t offset;     //!< offset within the file of the block

    BID() : storage(nullptr), offset(0)
    { }

    bool    valid() const {
        return storage != nullptr;
    }

    BID(file* s, int64_t o) : storage(s), offset(o)
    { }

    BID(const BID& obj) : storage(obj.storage), offset(obj.offset)
    { }

    BID& operator = (BID&) = default;

    template <size_t BlockSize>
    explicit BID(const BID<BlockSize>& obj)
        : storage(obj.storage), offset(obj.offset)
    { }

    template <size_t BlockSize>
    BID& operator = (const BID<BlockSize>& obj) {
        storage = obj.storage;
        offset = obj.offset;
        return *this;
    }

    bool is_managed() const {
        return storage->get_allocator_id() != file::NO_ALLOCATOR;
    }
};

//! Specialization of block identifier class (BID) for variable size block size.
//!
//! Stores block identity, given by file, offset within the file, and size of the block
template <>
struct BID<0>
{
    file    * storage; //!< pointer to the file of the block
    int64_t offset;    //!< offset within the file of the block
    size_t  size;      //!< size of the block in bytes

    enum
    {
        t_size = 0     //!< Blocks size, given by the parameter
    };

    BID() : storage(nullptr), offset(0), size(0)
    { }

    BID(file* f, int64_t o, size_t s) : storage(f), offset(o), size(s)
    { }

    bool valid() const {
        return (storage != nullptr);
    }
};

template <size_t BlockSize>
bool operator == (const BID<BlockSize>& a, const BID<BlockSize>& b) {
    return (a.storage == b.storage) && (a.offset == b.offset) && (a.size == b.size);
}

template <size_t BlockSize>
bool operator != (const BID<BlockSize>& a, const BID<BlockSize>& b) {
    return (a.storage != b.storage) || (a.offset != b.offset) || (a.size != b.size);
}

template <size_t BlockSize>
std::ostream& operator << (std::ostream& s, const BID<BlockSize>& bid) {
    // [0x12345678|0]0x00100000/0x00010000
    // [file ptr|file id]offset/size

    std::ios state(nullptr);
    state.copyfmt(s);

    s << "[" << bid.storage << "|";
    if (bid.storage)
        s << bid.storage->get_allocator_id();
    else
        s << "?";
    s << "]0x" << std::hex << std::setfill('0') << std::setw(8) << bid.offset << "/0x" << std::setw(8) << bid.size << std::dec;

    s.copyfmt(state);
    return s;
}

template <size_t BlockSize>
using BIDArray = std::vector<BID<BlockSize> >;

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_BID_HEADER

/******************************************************************************/
