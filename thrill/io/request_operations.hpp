/*******************************************************************************
 * thrill/io/request_operations.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008, 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_REQUEST_OPERATIONS_HEADER
#define THRILL_IO_REQUEST_OPERATIONS_HEADER

#include <thrill/io/iostats.hpp>
#include <thrill/io/request.hpp>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Collection of functions to track statuses of a number of requests.

//! Suspends calling thread until \b all given requests are completed.
//! \param reqs_begin begin of request sequence to wait for
//! \param reqs_end end of request sequence to wait for
template <typename RequestIterator>
void wait_all(RequestIterator reqs_begin, RequestIterator reqs_end) {
    for ( ; reqs_begin != reqs_end; ++reqs_begin)
        (RequestPtr(*reqs_begin))->wait();
}

//! Suspends calling thread until \b all given requests are completed.
//! \param req_array array of request_ptr objects
//! \param count size of req_array
static inline void wait_all(RequestPtr req_array[], size_t count) {
    wait_all(req_array, req_array + count);
}

//! Cancel requests.
//! The specified requests are canceled unless already being processed.
//! However, cancelation cannot be guaranteed.
//! Cancelled requests must still be waited for in order to ensure correct
//! operation.
//! \param reqs_begin begin of request sequence
//! \param reqs_end end of request sequence
//! \return number of request canceled
template <typename RequestIterator>
typename std::iterator_traits<RequestIterator>::difference_type
cancel_all(RequestIterator reqs_begin, RequestIterator reqs_end) {
    typename std::iterator_traits<RequestIterator>::difference_type num_canceled = 0;
    while (reqs_begin != reqs_end)
    {
        if ((RequestPtr(*reqs_begin))->cancel())
            ++num_canceled;
        ++reqs_begin;
    }
    return num_canceled;
}

//! Polls requests.
//! \param reqs_begin begin of request sequence to poll
//! \param reqs_end end of request sequence to poll
//! \return \c true if any of requests is completed, then index contains valid value, otherwise \c false
template <typename RequestIterator>
RequestIterator poll_any(RequestIterator reqs_begin, RequestIterator reqs_end) {
    while (reqs_begin != reqs_end)
    {
        if ((RequestPtr(*reqs_begin))->poll())
            return reqs_begin;

        ++reqs_begin;
    }
    return reqs_end;
}

//! Polls requests.
//! \param req_array array of request_ptr objects
//! \param count size of req_array
//! \param index contains index of the \b first completed request if any
//! \return \c true if any of requests is completed, then index contains valid value, otherwise \c false
inline bool poll_any(RequestPtr req_array[], size_t count, size_t& index) {
    RequestPtr* res = poll_any(req_array, req_array + count);
    index = res - req_array;
    return res != (req_array + count);
}

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_OPERATIONS_HEADER

/******************************************************************************/
