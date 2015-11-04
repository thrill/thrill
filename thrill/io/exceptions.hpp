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

namespace thrill {
namespace io {

//! \addtogroup reqlayer
//! \{

class io_error : public std::ios_base::failure
{
public:
    io_error() noexcept
        : std::ios_base::failure(std::string())
    { }

    io_error(const std::string& message) noexcept
        : std::ios_base::failure(message)
    { }
};

class bad_ext_alloc : public std::runtime_error
{
public:
    bad_ext_alloc() noexcept
        : std::runtime_error(std::string())
    { }

    bad_ext_alloc(const std::string& message) noexcept
        : std::runtime_error(message)
    { }
};

//! \}

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_EXCEPTIONS_HEADER

/******************************************************************************/
