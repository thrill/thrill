/*******************************************************************************
 * c7a/common/functional.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
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

//thanks to http://stackoverflow.com/a/7127988
template <typename T>
struct is_pair : public std::false_type { };

template <typename S, typename T>
struct is_pair<std::pair<S, T> >: public std::true_type { };

//! template for constexpr max, because std::max is not good enough.
template <typename T>
constexpr
static inline const T & max(const T& a, const T& b) {
    return a > b ? a : b;
}

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_FUNCTIONAL_HEADER

/******************************************************************************/
