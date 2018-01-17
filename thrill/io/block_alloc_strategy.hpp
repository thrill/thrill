/*******************************************************************************
 * thrill/io/block_alloc_strategy.hpp
 *
 * Copied and modified from STXXL https://github.com/stxxl/stxxl, which is
 * distributed under the Boost Software License, Version 1.0.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2002-2007 Roman Dementiev <dementiev@mpi-sb.mpg.de>
 * Copyright (C) 2007-2009 Andreas Beckmann <beckmann@cs.uni-frankfurt.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_IO_BLOCK_ALLOC_STRATEGY_HEADER
#define THRILL_IO_BLOCK_ALLOC_STRATEGY_HEADER

#include <thrill/io/config_file.hpp>

#include <foxxll/mng/block_alloc_strategy.hpp>

#include <algorithm>
#include <random>
#include <vector>

namespace thrill {
namespace io {

using namespace foxxll;

} // namespace io
} // namespace thrill

#endif // !THRILL_IO_BLOCK_ALLOC_STRATEGY_HEADER

/******************************************************************************/
