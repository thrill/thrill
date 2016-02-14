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

#include <ios>
#include <stdexcept>
#include <string>
#include <utility>

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

class IoError : public std::ios_base::failure
{
public:
    IoError() noexcept
        : std::ios_base::failure(std::string())
    { }

    explicit IoError(const std::string& message) noexcept
        : std::ios_base::failure(message)
    { }
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
