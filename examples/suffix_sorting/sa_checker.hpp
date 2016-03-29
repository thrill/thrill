/*******************************************************************************
 * examples/suffix_sorting/sa_checker.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SUFFIX_SORTING_SA_CHECKER_HEADER
#define THRILL_EXAMPLES_SUFFIX_SORTING_SA_CHECKER_HEADER

template <typename InputDIA, typename SuffixArrayDIA>
bool CheckSA(const InputDIA& input, const SuffixArrayDIA& suffix_array);

#endif // !THRILL_EXAMPLES_SUFFIX_SORTING_SA_CHECKER_HEADER

/******************************************************************************/
