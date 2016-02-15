/*******************************************************************************
 * thrill/api/max.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MAX_HEADER
#define THRILL_API_MAX_HEADER

#include <thrill/api/sum.hpp>
#include <thrill/common/functional.hpp>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType, typename Stack>
template <typename MaxFunction>
auto DIA<ValueType, Stack>::Max(
    const MaxFunction &max_function, const ValueType &initial_value) const {
    return Sum(max_function, initial_value);
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_MAX_HEADER

/******************************************************************************/
