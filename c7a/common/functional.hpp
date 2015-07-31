/*******************************************************************************
 * c7a/common/functional.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_FUNCTIONAL_HEADER
#define C7A_COMMON_FUNCTIONAL_HEADER

#include <utility>

namespace c7a {
namespace common {

//! Identity functor, very useful for default parameters.
struct Identity {
    template <typename Type>
    constexpr auto operator () (Type&& v) const noexcept
    ->decltype(std::forward<Type>(v)) {
        return std::forward<Type>(v);
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_FUNCTIONAL_HEADER

/******************************************************************************/
