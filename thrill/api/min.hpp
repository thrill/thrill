/*******************************************************************************
 * thrill/api/min.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MIN_HEADER
#define THRILL_API_MIN_HEADER

#include <thrill/api/sum.hpp>
#include <thrill/common/functional.hpp>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename Stack>
template <typename MinFunction>
auto DIA<ValueType, Stack>::Min(
    const MinFunction &min_function, const ValueType &initial_value) const {
    return Sum(min_function, initial_value);
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_MIN_HEADER

/******************************************************************************/
