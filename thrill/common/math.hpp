/*******************************************************************************
 * thrill/common/math.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_MATH_HEADER
#define THRILL_COMMON_MATH_HEADER

#include <thrill/api/context.hpp>

#include <algorithm>
#include <tuple>

namespace thrill {
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

//! given a global range [0,global_size) and p PEs to split the range, calculate
//! the [local_begin,local_end) index range assigned to the PE i.
std::tuple<size_t, size_t> CalculateLocalRange(
    size_t global_size, size_t p, size_t i) {

    double per_pe = static_cast<double>(global_size) / static_cast<double>(p);
    return std::make_tuple(
        std::ceil(i * per_pe),
        std::min(static_cast<size_t>(std::ceil((i + 1) * per_pe)),
                 global_size));
}

//! given a global range [0,global_size) and p PEs to split the range, calculate
//! the [local_begin,local_end) index range assigned to the PE i. Takes the
//! information from the Context.
std::tuple<size_t, size_t> CalculateLocalRange(
    size_t global_size, const Context& ctx) {
    return CalculateLocalRange(global_size, ctx.num_workers(), ctx.my_rank());
}

/******************************************************************************/

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_MATH_HEADER

/******************************************************************************/
