/*******************************************************************************
 * thrill/io/bid.cpp
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

#include <thrill/io/bid.hpp>

#include <thrill/io/file_base.hpp>

#include <iomanip>

namespace thrill {
namespace io {

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
    s << "]0x"
      << std::hex << std::setfill('0') << std::setw(8) << bid.offset << "/0x"
      << std::setw(8) << bid.size << std::dec;

    s.copyfmt(state);
    return s;
}

// template instantiations
template std::ostream& operator << (std::ostream& s, const BID<0>& bid);

} // namespace io
} // namespace thrill

/******************************************************************************/
