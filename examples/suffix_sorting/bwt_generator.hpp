/*******************************************************************************
 * examples/suffix_sorting/bwt_generator.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Florian Kurpicz <florian.kurpicz@tu-dortmund.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_BWT_GENERATOR_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_BWT_GENERATOR_HEADER

namespace examples {
namespace suffix_sorting {

template <typename InputDIA, typename SuffixArrayDIA>
InputDIA GenerateBWT(const InputDIA& input, const SuffixArrayDIA& suffix_array);

} // namespace suffix_sorting
} // namespace examples

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_BWT_GENERATOR_HEADER

/******************************************************************************/
