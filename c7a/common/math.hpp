/*******************************************************************************
 * c7a/common/math.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_MATH_HEADER
#define C7A_COMMON_MATH_HEADER

namespace c7a {
namespace common {

/******************************************************************************/

//! calculate the log2 floor of an integer type (by repeated bit shifts)
template <typename IntegerType>
unsigned int IntegerLog2Floor(IntegerType i) {
    unsigned int p = 0;
    while (i >= 256) i >>= 8, p += 8;
    while (i >>= 1) ++p;
    return p;
}

//! calculate the log2 ceiling of an integer type (by repeated bit shifts)
template <typename IntegerType>
unsigned int IntegerLog2Ceil(const IntegerType& i) {
    if (i <= 1) return 0;
    return IntegerLog2Floor(i - 1) + 1;
}

//! does what it says.
template <typename Integral>
static inline Integral RoundUpToPowerOfTwo(Integral n) {
    --n;
    for (int k = 1; !(k & (2 << sizeof(n))); k <<= 1)
        n |= n >> k;
    ++n;
    return n;
}

/******************************************************************************/

//! calculate n div k with rounding up
template <typename IntegerType>
IntegerType IntegerDivRoundUp(const IntegerType& n, const IntegerType& k) {
    return (n + k - 1) / k;
}

/******************************************************************************/

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_MATH_HEADER

/******************************************************************************/
