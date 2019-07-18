/*******************************************************************************
 * thrill/common/thread_barrier.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_THREAD_BARRIER_HEADER
#define THRILL_COMMON_THREAD_BARRIER_HEADER

#include <tlx/thread_barrier_mutex.hpp>
#include <tlx/thread_barrier_spin.hpp>

namespace thrill {
namespace common {

// select thread barrier implementation.
#if THRILL_HAVE_THREAD_SANITIZER
using ThreadBarrier = tlx::ThreadBarrierMutex;
#else
using ThreadBarrier = tlx::ThreadBarrierSpin;
#endif

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_THREAD_BARRIER_HEADER

/******************************************************************************/
