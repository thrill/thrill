/*******************************************************************************
 * thrill/io/request_interface.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008-2011 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 * Copyright (C) 2009 Johannes Singler <singler@ira.uka.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_REQUEST_INTERFACE_HEADER
#define THRILL_IO_REQUEST_INTERFACE_HEADER

#include <thrill/common/onoff_switch.hpp>

#include <ostream>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

//! Functional interface of a request.
//!
//! Since all library I/O operations are asynchronous,
//! one needs to keep track of their status:
//! e.g. whether an I/O operation completed or not.
class request_interface
{
public:
    using offset_type = size_t;
    using size_type = size_t;
    enum request_type { READ, WRITE };

    //! default constructor
    request_interface() = default;

    //! non-copyable: delete copy-constructor
    request_interface(const request_interface&) = delete;
    //! non-copyable: delete assignment operator
    request_interface& operator = (const request_interface&) = delete;
    //! move-constructor: default
    request_interface(request_interface&&) = default;
    //! move-assignment operator: default
    request_interface& operator = (request_interface&&) = default;

public:
    virtual bool add_waiter(common::onoff_switch* sw) = 0;
    virtual void delete_waiter(common::onoff_switch* sw) = 0;

protected:
    virtual void notify_waiters() = 0;

protected:
    virtual void completed(bool canceled) = 0;

public:
    //! Suspends calling thread until completion of the request.
    virtual void wait(bool measure_time = true) = 0;

    //! Cancel a request.
    //!
    //! The request is canceled unless already being processed.
    //! However, cancellation cannot be guaranteed.
    //! Canceled requests must still be waited for in order to ensure correct operation.
    //! If the request was canceled successfully, the completion handler will not be called.
    //! \return \c true iff the request was canceled successfully
    virtual bool cancel() = 0;

    //! Polls the status of the request.
    //! \return \c true if request is completed, otherwise \c false
    virtual bool poll() = 0;

    //! Identifies the type of I/O implementation.
    //! \return pointer to null terminated string of characters, containing the name of I/O implementation
    virtual const char * io_type() const = 0;

    //! Dumps properties of a request.
    virtual std::ostream & print(std::ostream& out) const = 0;

    virtual ~request_interface()
    { }
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_REQUEST_INTERFACE_HEADER

/******************************************************************************/
