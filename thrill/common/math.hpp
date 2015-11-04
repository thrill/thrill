/*******************************************************************************
 * thrill/common/math.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_MATH_HEADER
#define THRILL_COMMON_MATH_HEADER

#include <algorithm>
#include <cassert>
#include <cmath>
#include <ostream>

namespace thrill {
namespace common {

/******************************************************************************/

//! calculate the log2 floor of an integer type (by repeated bit shifts)
template <typename IntegerType>
static inline unsigned int IntegerLog2Floor(IntegerType i) {
    unsigned int p = 0;
    while (i >= 256) i >>= 8, p += 8;
    while (i >>= 1) ++p;
    return p;
}

//! calculate the log2 ceiling of an integer type (by repeated bit shifts)
template <typename IntegerType>
static inline unsigned int IntegerLog2Ceil(const IntegerType& i) {
    if (i <= 1) return 0;
    return IntegerLog2Floor(i - 1) + 1;
}

//! does what it says.
template <typename Integral>
static inline Integral RoundUpToPowerOfTwo(Integral n) {
    --n;
    for (size_t k = 1; k != 8 * sizeof(n); k <<= 1) {
        n |= n >> k;
    }
    ++n;
    return n;
}

//! does what it says.
template <typename Integral>
static inline Integral RoundDownToPowerOfTwo(Integral n) {
    return RoundUpToPowerOfTwo(n + 1) >> 1;
}

/******************************************************************************/

//! calculate n div k with rounding up
template <typename IntegerType>
static inline IntegerType IntegerDivRoundUp(const IntegerType& n, const IntegerType& k) {
    return (n + k - 1) / k;
}

/******************************************************************************/

//! represents a 1 dimensional range (interval) [begin,end)
class Range
{
public:
    Range() = default;
    Range(size_t _begin, size_t _end) : begin(_begin), end(_end) { }

    //! begin index
    size_t begin = 0;
    //! end index
    size_t end = 0;

    //! valid range (begin <= end)
    bool valid() const { return begin <= end; }
    //! size of range
    size_t size() const { return end - begin; }

    //! ostream-able
    friend std::ostream& operator << (std::ostream& os, const Range& r) {
        return os << '[' << r.begin << ',' << r.end << ')';
    }
};

//! given a global range [0,global_size) and p PEs to split the range, calculate
//! the [local_begin,local_end) index range assigned to the PE i.
static inline Range CalculateLocalRange(
    size_t global_size, size_t p, size_t i) {

    double per_pe = static_cast<double>(global_size) / static_cast<double>(p);
    return Range(
        static_cast<size_t>(std::ceil(static_cast<double>(i) * per_pe)),
        std::min(static_cast<size_t>(
                     std::ceil(static_cast<double>(i + 1) * per_pe)),
                 global_size));
}

/******************************************************************************/

/*!
 * Number of rounds in Perfect Matching (1-Factor).
 */
static inline size_t CalcOneFactorSize(size_t n) {
    return n % 2 == 0 ? n - 1 : n;
}

/*!
 * Calculate a Perfect Matching (1-Factor) on a Complete Graph. Used by
 * collective network algorithms.
 *
 * \param r round [0..n-1) of matching
 * \param p rank of this processor 0..n-1
 * \param n number of processors (graph size)
 * \return peer processor in this round
 */
static inline size_t CalcOneFactorPeer(size_t r, size_t p, size_t n) {
    assert(r < CalcOneFactorSize(n));
    assert(p < n);

    if (n % 2 == 0) {
        // p is even
        size_t idle = (r * n / 2) % (n - 1);
        if (p == n - 1)
            return idle;
        else if (p == idle)
            return n - 1;
        else
            return (r - p + n - 1) % (n - 1);
    }
    else {
        // p is odd
        return (r - p + n) % n;
    }
}

/******************************************************************************/

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_MATH_HEADER

/******************************************************************************/
