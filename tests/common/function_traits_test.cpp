/*******************************************************************************
 * tests/common/function_traits_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/function_traits.hpp>

#include <string>

using namespace thrill; // NOLINT

std::string func1(int, double) {
    return "hello";
}

TEST(FunctionTraits, FunctionPointerTest) {
    using common::FunctionTraits;

    using Func1 = decltype(&func1);

    static_assert(std::is_same<
                      FunctionTraits<Func1>::result_type, std::string>::value,
                  "Result type is std::string");

    static_assert(FunctionTraits<Func1>::arity == 2,
                  "Arity of func1 is 2");

    static_assert(std::is_same<
                      FunctionTraits<Func1>::arg<0>, int>::value,
                  "Argument 0 is int");

    static_assert(std::is_same<
                      FunctionTraits<Func1>::arg<1>, double>::value,
                  "Argument 1 is double");
}

TEST(FunctionTraits, LambdaParametersTest) {
    using common::FunctionTraits;

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
    using common::FunctionTraits;

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
