/*******************************************************************************
 * thrill/common/qsort.hpp
 *
 * Quicksort implementations with two and three pivots. Pretty much the
 * algorithm of Yaroslavskiy (from Java) and Kushagra et al. (2014) at
 * ALENEX. Some parts are also from libc++ which is available under the MIT
 * license.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2014 Sebastian Wild <wild@cs.uni-kl.de>
 * Copyright (C) 2014 Martin Aumüller <martin.aumueller@tu-ilmenau.de>
 * Copyright (C) 2014 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_QSORT_HEADER
#define THRILL_COMMON_QSORT_HEADER

#include <thrill/common/logger.hpp>

#include <algorithm>

namespace thrill {
namespace common {
namespace qsort_local {

//! Assigns a0 <- a1, a1 <- a2, and a2 <- a0.
template <typename ValueType>
void rotate3(ValueType& a0, ValueType& a1, ValueType& a2) {
    ValueType tmp = std::move(a0);
    a0 = std::move(a1);
    a1 = std::move(a2);
    a2 = std::move(tmp);
}

//! Assigns a0 <- a1, a1 <- a2, a2 <- a3, and a3 <- a0.
template <typename ValueType>
void rotate4(ValueType& a0, ValueType& a1, ValueType& a2, ValueType& a3) {
    ValueType tmp = std::move(a0);
    a0 = std::move(a1);
    a1 = std::move(a2);
    a2 = std::move(a3);
    a3 = std::move(tmp);
}

//! Sort three items, stable, 2-3 compares, 0-2 swaps
template <typename Compare, typename Iterator>
void sort3(Iterator x, Iterator y, Iterator z, Compare cmp) {
    using std::swap;

    if (!cmp(*y, *x)) {        // if x <= y
        if (!cmp(*z, *y))      // if y <= z
            return;            // x <= y && y <= z
        // x <= y && y > z
        swap(*y, *z);          // x <= z && y < z
        if (cmp(*y, *x))       // if x > y
            swap(*x, *y);      // x < y && y <= z
        return;                // x <= y && y < z
    }
    if (cmp(*z, *y)) {         // x > y, if y > z
        swap(*x, *z);          // x < y && y < z
        return;
    }
    swap(*x, *y);              // x > y && y <= z
                               // x < y && x <= z
    if (cmp(*z, *y))           // if y > z
        swap(*y, *z);          // x <= y && y < z
    // x <= y && y <= z
}

//! Sort four items, stable, 3-6 compares, 0-5 swaps
template <typename Compare, typename Iterator>
void sort4(Iterator x1, Iterator x2, Iterator x3, Iterator x4, Compare cmp) {
    using std::swap;

    sort3<Compare>(x1, x2, x3, cmp);
    if (cmp(*x4, *x3)) {
        swap(*x3, *x4);
        if (cmp(*x3, *x2)) {
            swap(*x2, *x3);
            if (cmp(*x2, *x1)) {
                swap(*x1, *x2);
            }
        }
    }
}

//! Sort five items, 4-10 compares, 0-9 swaps
template <typename Compare, typename Iterator>
void sort5(Iterator x1, Iterator x2, Iterator x3,
           Iterator x4, Iterator x5, Compare cmp) {
    using std::swap;

    sort4<Compare>(x1, x2, x3, x4, cmp);
    if (cmp(*x5, *x4)) {
        swap(*x4, *x5);
        if (cmp(*x4, *x3)) {
            swap(*x3, *x4);
            if (cmp(*x3, *x2)) {
                swap(*x2, *x3);
                if (cmp(*x2, *x1)) {
                    swap(*x1, *x2);
                }
            }
        }
    }
}

template <typename Compare, typename Iterator>
void InsertionSort(Iterator left, Iterator right, Compare cmp) {
    using value_type = typename std::iterator_traits<Iterator>::value_type;
    using std::swap;

    switch (right - left) {
    case 0:
    case 1:
        return;
    case 2:
        if (cmp(*(--right), *left))
            swap(*left, *right);
        return;
    case 3:
        return qsort_local::sort3<Compare>(left, left + 1, left + 2, cmp);
    case 4:
        return qsort_local::sort4<Compare>(
            left, left + 1, left + 2, left + 3, cmp);
    case 5:
        return qsort_local::sort5<Compare>(
            left, left + 1, left + 2, left + 3, left + 4, cmp);
    }

    for (Iterator i = left + 1; i != right; ++i) {
        Iterator j = i;
        value_type t(std::move(*j));
        for (Iterator k = i; k != left && cmp(t, *(--k)); --j)
            *j = std::move(*k);
        *j = std::move(t);
    }
}

template <typename Iterator, typename Compare>
Iterator median3(Iterator a, Iterator b, Iterator c, Compare cmp) {
    if (cmp(*a, *b)) {
        if (cmp(*b, *c))
            return b;
        else if (cmp(*a, *c))
            return c;
        else
            return a;
    }
    else {
        if (cmp(*a, *c))
            return a;
        else if (cmp(*b, *c))
            return c;
        else
            return b;
    }
}

//! Sort _iterators_ by their content, used for sorting pivot samples.
template <typename Iterator, typename Compare>
void sort_samples(Iterator* A, size_t size, Compare cmp) {
    for (size_t i = 1; i < size; i++) {
        size_t j = i;
        Iterator t = A[i];
        while (j > 0) {
            if (cmp(*t, *(A[j - 1]))) {
                A[j] = A[j - 1];
                j--;
            }
            else {
                break;
            }
        }
        A[j] = t;
    }
}

} // namespace qsort_local

template <typename Compare, typename Iterator>
void qsort_two_pivots_yaroslavskiy(Iterator lo, Iterator hi, Compare cmp) {
    using value_type = typename std::iterator_traits<Iterator>::value_type;
    using std::swap;

    if (hi - lo < 32)
        return qsort_local::InsertionSort<Compare>(lo, hi, cmp);

    size_t n = hi - lo;

    Iterator samples[7] = {
        lo + n * 1 / 8, lo + n * 2 / 8, lo + n * 3 / 8,
        lo + n * 4 / 8, lo + n * 5 / 8, lo + n * 6 / 8, lo + n * 7 / 8
    };

    qsort_local::sort_samples(samples, 7, cmp);

    swap(*lo, *(samples[2]));
    swap(*(hi - 1), *(samples[4]));

    const value_type p = *lo;
    const value_type q = *(hi - 1);

    Iterator l = lo + 1;
    Iterator g = hi - 2;
    Iterator k = l;

    while (k <= g) {
        if (cmp(*k, p)) {
            swap(*k, *l);
            ++l;
        }
        else if (!cmp(*k, q)) {
            while (cmp(q, *g))
                --g;

            if (k < g) {
                if (cmp(*g, p)) {
                    qsort_local::rotate3(*g, *k, *l);
                    ++l;
                }
                else {
                    swap(*k, *g);
                }
                --g;
            }
        }
        ++k;
    }
    --l, ++g;
    swap(*lo, *l);
    swap(*(hi - 1), *g);

    qsort_two_pivots_yaroslavskiy<Compare>(lo, l, cmp);
    qsort_two_pivots_yaroslavskiy<Compare>(l + 1, g, cmp);
    qsort_two_pivots_yaroslavskiy<Compare>(g + 1, hi, cmp);
}

template <typename Compare, typename Iterator>
void qsort_three_pivots(Iterator left, Iterator right, Compare cmp) {
    using value_type = typename std::iterator_traits<Iterator>::value_type;
    using std::swap;

    size_t n = right - left;

    if (n <= 32)
        return qsort_local::InsertionSort(left, right, cmp);

    Iterator samples[7] = {
        left + n * 1 / 8, left + n * 2 / 8, left + n * 3 / 8,
        left + n * 4 / 8, left + n * 5 / 8, left + n * 6 / 8, left + n * 7 / 8
    };

    qsort_local::sort_samples(samples, 7, cmp);

    swap(*left, *(samples[1]));
    swap(*(left + 1), *(samples[3]));
    swap(*(right - 1), *(samples[5]));

    Iterator i = left + 2;
    Iterator j = i;

    Iterator k = right - 2;
    Iterator l = k;

    const value_type p = *left;
    const value_type q = *(left + 1);
    const value_type r = *(right - 1);

    while (j <= k) {
        while (cmp(*j, q)) {
            if (cmp(*j, p)) {
                swap(*i, *j);
                i++;
            }
            j++;
        }

        while (cmp(q, *k)) {
            if (cmp(r, *k)) {
                swap(*k, *l);
                l--;
            }
            k--;
        }

        if (j <= k) {
            if (cmp(r, *j)) {
                if (cmp(*k, p)) {
                    qsort_local::rotate4(*j, *i, *k, *l);
                    i++;
                }
                else {
                    qsort_local::rotate3(*j, *k, *l);
                }
                l--;
            }
            else {
                if (cmp(*k, p)) {
                    qsort_local::rotate3(*j, *i, *k);
                    i++;
                }
                else {
                    swap(*j, *k);
                }
            }
            j++, k--;
        }
    }

    qsort_local::rotate3(*(left + 1), *(i - 1), *(j - 1));

    swap(*left, *(i - 2));
    swap(*(right - 1), *(l + 1));

    sLOG0 << "qsort_three_pivots: "
          << i - 2 - left
          << j - i
          << l + 1 - j
          << right - l - 2;

    qsort_three_pivots<Compare>(left, i - 2, cmp);
    qsort_three_pivots<Compare>(i - 1, j - 1, cmp);
    qsort_three_pivots<Compare>(j, l + 1, cmp);
    qsort_three_pivots<Compare>(l + 2, right, cmp);
}

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_QSORT_HEADER

/******************************************************************************/
