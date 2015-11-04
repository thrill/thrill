/*******************************************************************************
 * thrill/io/serving_request.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_SERVING_REQUEST_HEADER
#define THRILL_IO_SERVING_REQUEST_HEADER

#include <thrill/io/request_with_state.hpp>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Request which serves an I/O by calling the synchronous routine of the file.
class serving_request : public request_with_state
{
    template <class base_file_type>
    friend class fileperblock_file;
    friend class request_queue_impl_qwqr;
    friend class request_queue_impl_1q;

public:
    serving_request(
        const completion_handler& on_cmpl,
        io::file* f,
        void* buf,
        offset_type off,
        size_type b,
        ReadOrWriteType t);

protected:
    virtual void serve();

public:
    const char * io_type() const;
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_SERVING_REQUEST_HEADER

/******************************************************************************/
