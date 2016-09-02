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
 * Number parsing helpers, wrap strto{f,d,ld,l,ul,ll,ull}
 */
template<typename T>
T strtonumber(const char* nptr, char **endptr, int base = 10);

// FLOATING POINT TYPES
// float
template<>
float strtonumber<float>(const char* nptr, char **endptr, int) {
    return strtof(nptr, endptr);
}

// double
template<>
double strtonumber<double>(const char* nptr, char **endptr, int) {
    return strtod(nptr, endptr);
}

// long double
template<>
long double strtonumber<long double>(const char* nptr, char **endptr, int) {
    return strtold(nptr, endptr);
}


// INTEGRAL TYPES
// long
template<>
long strtonumber<long>(const char *nptr, char **endptr, int base) {
    return strtol(nptr, endptr, base);
}
// unsigned long
template<>
unsigned long strtonumber<unsigned long>(const char *nptr, char **endptr,
                                         int base) {
    return strtoul(nptr, endptr, base);
}

// long long
template<>
long long strtonumber<long long>(const char *nptr, char **endptr, int base) {
    return strtoll(nptr, endptr, base);
}
// unsigned long long
template<>
unsigned long long strtonumber<unsigned long long>(const char *nptr,
                                                   char **endptr, int base) {
    return strtoull(nptr, endptr, base);
}

} // namespace logistic_regression
} // namespace examples

#endif // !THRILL_EXAMPLES_LOGISTIC_REGRESSION_STRTONUMBER_HEADER

/******************************************************************************/
