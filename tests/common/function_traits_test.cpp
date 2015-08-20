/*******************************************************************************
 * tests/common/function_traits_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/function_traits.hpp>

#include <string>

TEST(FunctionTraits, LambdaParametersTest) {
    using c7a::common::FunctionTraits;

    auto lambda1 =
        [=](int x, char c) -> std::string {
            return std::to_string(x) + " " + c;
        };

    using Lambda1Type = decltype(lambda1);

    static_assert(FunctionTraits<Lambda1Type>::arity == 2,
                  "Arity of lambda1 is 2");

    static_assert(std::is_same<
                      FunctionTraits<Lambda1Type>::result_type, std::string>::value,
                  "Result type is std::string");

    static_assert(std::is_same<
                      FunctionTraits<Lambda1Type>::arg<0>, int>::value,
                  "Argument 0 is int");

    static_assert(std::is_same<
                      FunctionTraits<Lambda1Type>::arg<1>, char>::value,
                  "Argument 1 is char");
}

TEST(FunctionTraits, MutableLambdaParametersTest) {
    using c7a::common::FunctionTraits;

    auto lambda1 =
        [=](int x, char c) mutable -> std::string {
            return std::to_string(x) + " " + c;
        };

    using Lambda1Type = decltype(lambda1);

    static_assert(FunctionTraits<Lambda1Type>::arity == 2,
                  "Arity of lambda1 is 2");

    static_assert(std::is_same<
                      FunctionTraits<Lambda1Type>::result_type, std::string>::value,
                  "Result type is std::string");

    static_assert(std::is_same<
                      FunctionTraits<Lambda1Type>::arg<0>, int>::value,
                  "Argument 0 is int");

    static_assert(std::is_same<
                      FunctionTraits<Lambda1Type>::arg<1>, char>::value,
                  "Argument 1 is char");
}

/******************************************************************************/
