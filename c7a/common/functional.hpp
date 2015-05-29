/*******************************************************************************
 * c7a/common/functional.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_FUNCTIONAL_HEADER
#define C7A_COMMON_FUNCTIONAL_HEADER

namespace c7a {
namespace common {

//! Identity functor, very useful for default parameters.
struct Identity {
    template<typename Type>
    constexpr auto operator()(Type&& v) const noexcept
        -> decltype(std::forward<Type>(v)) {
        return std::forward<Type>(v);
    }
};

//! Simple sum operator
template <typename T>
struct SumOp {
    //! returns the sum of a and b
    T operator () (const T& a, const T& b) {
        return a + b;
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_FUNCTIONAL_HEADER

/******************************************************************************/
