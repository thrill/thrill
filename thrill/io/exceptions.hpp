/*******************************************************************************
 * thrill/io/exceptions.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2006 Roman Dementiev <dementiev@ira.uka.de>
 * Copyright (C) 2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_EXCEPTIONS_HEADER
#define THRILL_IO_EXCEPTIONS_HEADER

#include <thrill/mem/pool.hpp>

#include <stdexcept>
#include <string>
#include <utility>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

class IoError : public std::exception
{
public:
    explicit IoError(const mem::safe_string& message) noexcept
        : std::exception(), safe_message_(message) { }

    virtual const char * what() const noexcept {
        return safe_message_.c_str();
    }

    const mem::safe_string & safe_message() const { return safe_message_; }

private:
    mem::safe_string safe_message_;
};

class BadExternalAlloc : public std::runtime_error
{
public:
    BadExternalAlloc() noexcept
        : std::runtime_error(std::string())
    { }

    explicit BadExternalAlloc(const std::string& message) noexcept
        : std::runtime_error(message)
    { }
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_EXCEPTIONS_HEADER

/******************************************************************************/
