/*******************************************************************************
 * examples/suffix_sorting/dc3.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_DC3_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_DC3_HEADER

#include <thrill/api/dia.hpp>

namespace examples {
namespace suffix_sorting {

template <typename Index, typename InputDIA>
thrill::DIA<Index> DC3(const InputDIA& input_dia, size_t input_size);

} // namespace suffix_sorting
} // namespace examples

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_DC3_HEADER

/******************************************************************************/
