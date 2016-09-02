/*******************************************************************************
 * examples/logistic_regression/strtonumber.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_LOGISTIC_REGRESSION_STRTONUMBER_HEADER
#define THRILL_EXAMPLES_LOGISTIC_REGRESSION_STRTONUMBER_HEADER

#include <cstdlib>

namespace examples {
namespace logistic_regression {

/*!
 * Number parsing helpers, wraps strto{f,d,ld,l,ul,ll,ull}() via type switch.
 */
template <typename T>
T from_str(const char* nptr, char** endptr = nullptr, int base = 10);

/******************************************************************************/
// specializations for floating point types

// float
template <>
float from_str<float>(const char* nptr, char** endptr, int) {
    return std::strtof(nptr, endptr);
}

// double
template <>
double from_str<double>(const char* nptr, char** endptr, int) {
    return std::strtod(nptr, endptr);
}

// long double
template <>
long double from_str<long double>(const char* nptr, char** endptr, int) {
    return std::strtold(nptr, endptr);
}

/******************************************************************************/
// specializations for integral types

// long
template <>
long from_str<long>(const char* nptr, char** endptr, int base) {
    return std::strtol(nptr, endptr, base);
}
// unsigned long
template <>
unsigned long from_str<unsigned long>(
    const char* nptr, char** endptr, int base) {
    return std::strtoul(nptr, endptr, base);
}

// long long
template <>
long long from_str<long long>(const char* nptr, char** endptr, int base) {
    return std::strtoll(nptr, endptr, base);
}
// unsigned long long
template <>
unsigned long long from_str<unsigned long long>(
    const char* nptr, char** endptr, int base) {
    return std::strtoull(nptr, endptr, base);
}

} // namespace logistic_regression
} // namespace examples

#endif // !THRILL_EXAMPLES_LOGISTIC_REGRESSION_STRTONUMBER_HEADER

/******************************************************************************/
