/*******************************************************************************
 * thrill/common/ring_buffer.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_RING_BUFFER_HEADER
#define THRILL_COMMON_RING_BUFFER_HEADER

#include <tlx/ring_buffer.hpp>

namespace thrill {
namespace common {

template <typename Type, typename Allocator = std::allocator<Type> >
using RingBuffer = tlx::RingBuffer<Type, Allocator>;

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_RING_BUFFER_HEADER

/******************************************************************************/
