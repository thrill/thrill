/*******************************************************************************
 * thrill/io/completion_handler.hpp
 *
 * Loki-style completion handler (functors)
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2003 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2008 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_COMPLETION_HANDLER_HEADER
#define THRILL_IO_COMPLETION_HANDLER_HEADER

#include <cstdlib>
#include <memory>

namespace thrill {
namespace io {

class request;

class completion_handler_impl
{
public:
    virtual void operator () (request*) = 0;
    virtual completion_handler_impl * clone() const = 0;
    virtual ~completion_handler_impl() { }
    completion_handler_impl() = default;
    completion_handler_impl(const completion_handler_impl&) = default;
    completion_handler_impl& operator = (completion_handler_impl&) = default;
};

template <typename HandlerType>
class completion_handler1 : public completion_handler_impl
{
private:
    HandlerType m_handler;

public:
    completion_handler1(const HandlerType& handler)
        : m_handler(handler)
    { }
    completion_handler1 * clone() const {
        return new completion_handler1(*this);
    }
    void operator () (request* req) {
        m_handler(req);
    }
};

//! Completion handler class (Loki-style).
//!
//! In some situations one needs to execute some actions after completion of an
//! I/O request. In these cases one can use an I/O completion handler - a
//! function object that can be passed as a parameter to asynchronous I/O calls
//! \c stxxl::file::aread and \c stxxl::file::awrite .
class completion_handler
{
    std::unique_ptr<completion_handler_impl> m_ptr;

public:
    //! Construct default, no operation completion handler.
    completion_handler()
        : m_ptr(static_cast<completion_handler_impl*>(nullptr))
    { }

    //! Copy constructor.
    completion_handler(const completion_handler& obj)
        : m_ptr(obj.m_ptr.get() ? obj.m_ptr.get()->clone() : nullptr)
    { }

    //! Construct a completion handler which calls some function.
    template <typename HandlerType>
    completion_handler(const HandlerType& handler)
        : m_ptr(new completion_handler1<HandlerType>(handler))
    { }

    //! Assignment operator
    completion_handler& operator = (const completion_handler& obj) {
        m_ptr.reset(obj.m_ptr.get() ? obj.m_ptr.get()->clone() : nullptr);
        return *this;
    }

    //! Call the enclosed completion handler.
    void operator () (request* req) {
        if (m_ptr.get())
            (*m_ptr)(req);
    }
};

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_COMPLETION_HANDLER_HEADER

/******************************************************************************/
