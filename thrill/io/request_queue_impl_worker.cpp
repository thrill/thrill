/*******************************************************************************
 * thrill/io/request_queue_impl_worker.cpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2005 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/config.hpp>
#include <thrill/common/semaphore.hpp>
#include <thrill/common/state.hpp>
#include <thrill/io/error_handling.hpp>
#include <thrill/io/request_queue_impl_worker.hpp>

#include <cassert>
#include <cstddef>

#if THRILL_STD_THREADS && THRILL_MSVC >= 1700
 #include <windows.h>
#endif

namespace thrill {
namespace io {

void RequestQueueImplWorker::start_thread(void* (*worker)(void*), void* arg, Thread& t, common::state<thread_state>& s) {
    assert(s() == NOT_RUNNING);
    t = new std::thread(worker, arg);
    s.set_to(RUNNING);
}

void RequestQueueImplWorker::stop_thread(Thread& t, common::state<thread_state>& s, common::semaphore& sem) {
    assert(s() == RUNNING);
    s.set_to(TERMINATING);
    sem++;
#if THRILL_MSVC >= 1700
    // In the Visual C++ Runtime 2012 and 2013, there is a deadlock bug, which
    // occurs when threads are joined after main() exits. Apparently, Microsoft
    // thinks this is not a big issue. It has not been fixed in VC++RT 2013.
    // https://connect.microsoft.com/VisualStudio/feedback/details/747145
    //
    // All STXXL threads are created by singletons, which are global variables
    // that are deleted after main() exits. The fix applied here it to use
    // std::thread::native_handle() and access the WINAPI to terminate the
    // thread directly (after it finished handling its i/o requests).

    WaitForSingleObject(t->native_handle(), INFINITE);
    CloseHandle(t->native_handle());
#else
    t->join();
    delete t;
#endif
    assert(s() == TERMINATED);
    s.set_to(NOT_RUNNING);
}

} // namespace io
} // namespace thrill

/******************************************************************************/
