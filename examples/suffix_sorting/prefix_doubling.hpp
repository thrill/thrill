/*******************************************************************************
 * examples/suffix_sorting/prefix_doubling.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_PREFIX_DOUBLING_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_PREFIX_DOUBLING_HEADER

#include <thrill/api/dia.hpp>

namespace examples {
namespace suffix_sorting {

template <typename Index, typename InputDIA>
thrill::DIA<Index>
PrefixDoublingWindow(const InputDIA& input_dia, size_t input_size, bool packed);

template <typename Index, typename InputDIA>
thrill::DIA<Index>
PrefixDoublingSorting(const InputDIA& input_dia, size_t input_size, bool packed);

template <typename Index, typename InputDIA>
thrill::DIA<Index>
PrefixDoublingDiscarding(const InputDIA& input_dia, size_t input_size, bool packed);

} // namespace suffix_sorting
} // namespace examples

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_PREFIX_DOUBLING_HEADER

/******************************************************************************/
