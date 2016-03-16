/*******************************************************************************
 * examples/logistic_regression/strtonumber.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/


#pragma once

#ifndef THRILL_EXAMPLES_LOGISTIC_REGRESSION_STRTONUMBER_HEADER
#define THRILL_EXAMPLES_LOGISTIC_REGRESSION_STRTONUMBER_HEADER

#include <cstdlib>

namespace examples {
namespace logistic_regression {

/**
 * Number passing helpers, wrap strto{f,d,ld,l,ul,ll,ull}
 */

// FLOATING POINT TYPES
// float
template<typename T>
T strtonumber(const char *nptr, char **endptr,
              typename std::enable_if<std::is_same<T, float>::value>::type* = 0) {
    return strtof(nptr, endptr);
}

// double
template<typename T>
T strtonumber(const char *nptr, char **endptr,
              typename std::enable_if<std::is_same<T, double>::value>::type* = 0) {
    return strtod(nptr, endptr);
}

// long double
template<typename T>
T strtonumber(const char *nptr, char **endptr,
              typename std::enable_if<std::is_same<T, long double>::value>::type* = 0) {
    return strtold(nptr, endptr);
}


// INTEGRAL TYPES
// long
template<typename T>
T strtonumber(const char *nptr, char **endptr, int base = 10,
              typename std::enable_if<std::is_same<T, long int>::value>::type* = 0) {
    return strtol(nptr, endptr, base);
}
// unsigned long
template<typename T>
T strtonumber(const char *nptr, char **endptr, int base = 10,
              typename std::enable_if<std::is_same<T, unsigned long int>::value>::type* = 0) {
    return strtoul(nptr, endptr, base);
}

// long long
template<typename T>
T strtonumber(const char *nptr, char **endptr, int base = 10,
              typename std::enable_if<std::is_same<T, long long int>::value>::type* = 0) {
    return strtoll(nptr, endptr, base);
}
// unsigned long long
template<typename T>
T strtonumber(const char *nptr, char **endptr, int base = 10,
              typename std::enable_if<std::is_same<T, unsigned long long int>::value>::type* = 0) {
    return strtoull(nptr, endptr, base);
}

} // namespace logistic_regression
} // namespace examples

#endif // !THRILL_EXAMPLES_LOGISTIC_REGRESSION_STRTONUMBER_HEADER

/******************************************************************************/
