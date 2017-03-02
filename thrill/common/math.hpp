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
#include <limits>
#include <ostream>

namespace thrill {
namespace common {

/******************************************************************************/

//! absolute difference, which also works for unsigned types
template <typename T>
T abs_diff(const T& a, const T& b) {
    return a > b ? a - b : b - a;
}

//! Add x + y but truncate result upwards such that it fits into original
//! datatype
template <typename IntegerType, unsigned bits = (8* sizeof(IntegerType))>
static inline
IntegerType AddTruncToType(const IntegerType& a, const IntegerType& b) {
    size_t s = static_cast<size_t>(a) + static_cast<size_t>(b);
    if (s >= (size_t(1) << bits))
        s = (size_t(1) << bits) - 1;
    return static_cast<IntegerType>(s);
}

/******************************************************************************/

//! represents a 1 dimensional range (interval) [begin,end)
class Range
{
public:
    Range() = default;
    Range(size_t _begin, size_t _end) : begin(_begin), end(_end) { }

    static Range Invalid() {
        return Range(std::numeric_limits<size_t>::max(),
                     std::numeric_limits<size_t>::min());
    }

    //! begin index
    size_t begin = 0;
    //! end index
    size_t end = 0;

    //! size of range
    size_t size() const { return end - begin; }

    //! range is empty (begin == end)
    bool IsEmpty() const { return begin == end; }
    //! valid non-empty range (begin < end)
    bool IsValid() const { return begin < end; }

    //! swap boundaries, making a valid range invalid.
    void Swap() { std::swap(begin, end); }

    //! calculate a partition range [begin,end) by taking the current Range
    //! splitting it into p parts and taking the i-th one.
    Range Partition(size_t index, size_t parts) const {
        return Range(index * size() / parts, (index + 1) * size() / parts);
    }

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
