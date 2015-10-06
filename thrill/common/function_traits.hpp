/*******************************************************************************
 * thrill/common/function_traits.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_FUNCTION_TRAITS_HEADER
#define THRILL_COMMON_FUNCTION_TRAITS_HEADER

#include <cstddef>
#include <tuple>

namespace thrill {
namespace common {

#ifndef THRILL_DOXYGEN_IGNORE

// taken from: http://stackoverflow.com/questions/7943525/is-it-possible-to-figure-out-the-parameter-type-and-return-type-of-a-lambda
template <typename T>
struct FunctionTraits : public FunctionTraits<decltype(& T::operator ())>{ };
// For generic types, directly use the result of the signature of its 'operator()'

#endif

//! specialize for pointers to const member function
template <typename ClassType, typename ReturnType, typename ... Args>
struct FunctionTraits<ReturnType (ClassType::*)(Args ...) const>{

    //! arity is the number of arguments.
    enum { arity = sizeof ... (Args) };

    using result_type = ReturnType;
    using is_const = std::true_type;

    //! the i-th argument is equivalent to the i-th tuple element of a tuple
    //! composed of those arguments.
    template <size_t i>
    using arg = typename std::tuple_element<i, std::tuple<Args ...> >::type;

    //! return i-th argument reduced to plain type: remove_cv and
    //! remove_reference.
    template <size_t i>
    using arg_plain =
              typename std::remove_cv<
                  typename std::remove_reference<
                      arg<i>
                      >::type
                  >::type;
};

//! specialize for pointers to mutable member function
template <typename ClassType, typename ReturnType, typename ... Args>
struct FunctionTraits<ReturnType (ClassType::*)(Args ...)>
    : public FunctionTraits<ReturnType (ClassType::*)(Args ...) const>
{
    using is_const = std::false_type;
};

//! specialize for function pointers
template <typename ReturnType, typename ... Args>
struct FunctionTraits<ReturnType (*)(Args ...)>{

    //! arity is the number of arguments.
    enum { arity = sizeof ... (Args) };

    using result_type = ReturnType;
    using is_const = std::true_type;

    //! the i-th argument is equivalent to the i-th tuple element of a tuple
    //! composed of those arguments.
    template <size_t i>
    using arg = typename std::tuple_element<i, std::tuple<Args ...> >::type;

    //! return i-th argument reduced to plain type: remove_cv and
    //! remove_reference.
    template <size_t i>
    using arg_plain =
              typename std::remove_cv<
                  typename std::remove_reference<
                      arg<i>
                      >::type
                  >::type;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_FUNCTION_TRAITS_HEADER

/******************************************************************************/
