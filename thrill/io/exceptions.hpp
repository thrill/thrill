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

#include <foxxll/common/exceptions.hpp>
#include <thrill/mem/pool.hpp>

#include <stdexcept>
#include <string>
#include <utility>

namespace thrill {
namespace io {

using namespace foxxll;

using IoError = foxxll::io_error;
using BadExternalAlloc = foxxll::bad_ext_alloc;

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_EXCEPTIONS_HEADER

/******************************************************************************/
