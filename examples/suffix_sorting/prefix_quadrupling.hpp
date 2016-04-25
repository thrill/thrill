/*******************************************************************************
 * examples/suffix_sorting/prefix_quadrupling.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_PREFIX_QUADRUPLING_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_PREFIX_QUADRUPLING_HEADER

#include <thrill/api/dia.hpp>

namespace examples {
namespace suffix_sorting {

template <typename Index, typename InputDIA>
thrill::DIA<Index>
PrefixQuadrupling(const InputDIA& input_dia, size_t input_size);

template <typename Index, typename InputDIA>
thrill::DIA<Index>
PrefixQuadruplingDiscarding(const InputDIA& input_dia, size_t input_size);

} // namespace suffix_sorting
} // namespace examples

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_PREFIX_QUADRUPLING_HEADER

/******************************************************************************/
