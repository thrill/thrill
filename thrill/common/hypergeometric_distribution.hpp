/*******************************************************************************
 * thrill/common/hypergeometric_distribution.hpp
 *
 * A hypergeomitric distribution random generator adapted from NumPy.
 *
 * The implementation of loggam(), rk_hypergeometric(), rk_hypergeometric_hyp(),
 * and rk_hypergeometric_hrua() were adapted from NumPy's
 * numpy/random/mtrand/distributions.c which has this license:
 *
 * Copyright 2005 Robert Kern (robert.kern@gmail.com)
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS
 * OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 *
 * The implementations of rk_hypergeometric_hyp(), rk_hypergeometric_hrua(),
 * and rk_triangular() were adapted from Ivan Frohne's rv.py which has this
 * license:
 *
 *            Copyright 1998 by Ivan Frohne; Wasilla, Alaska, U.S.A.
 *                            All Rights Reserved
 *
 * Permission to use, copy, modify and distribute this software and its
 * documentation for any purpose, free of charge, is granted subject to the
 * following conditions:
 *   The above copyright notice and this permission notice shall be included in
 *   all copies or substantial portions of the software.
 *
 *   THE SOFTWARE AND DOCUMENTATION IS PROVIDED WITHOUT WARRANTY OF ANY KIND,
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO MERCHANTABILITY, FITNESS
 *   FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.  IN NO EVENT SHALL THE AUTHOR
 *   OR COPYRIGHT HOLDER BE LIABLE FOR ANY CLAIM OR DAMAGES IN A CONTRACT
 *   ACTION, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 *   SOFTWARE OR ITS DOCUMENTATION.
 *
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_HYPERGEOMETRIC_DISTRIBUTION_HEADER
#define THRILL_COMMON_HYPERGEOMETRIC_DISTRIBUTION_HEADER

#include <thrill/common/logger.hpp>

#include <cmath>
#include <random>

namespace thrill {
namespace common {

template <typename int_t = int64_t, typename fp_t = double>
class hypergeometric_distribution
{
public:
    using result_type = int_t;
    using real_type = fp_t;

    hypergeometric_distribution(size_t seed = 0)
        : rng(seed != 0 ? seed : std::random_device { }
              ()) { }

    int_t operator () (int_t good, int_t bad, int_t sample) {
        if (sample < 1) {
            sLOG1 << "hypergeometric distribution error: sample < 1:" << sample;
            return static_cast<int_t>(-1);
        }
        if (good + bad < sample) {
            sLOG1 << "hypergeometric distribution error: good + bad < sample:"
                  << "good:" << good << "bad:" << bad << "sample:" << sample;
            return static_cast<int_t>(-1);
        }
        return rk_hypergeometric(good, bad, sample);
    }

    void seed(size_t seed_val) {
        rng.seed(seed_val);
    }

private:
    /*
     * log-gamma function to support some of these distributions. The
     * algorithm comes from SPECFUN by Shanjie Zhang and Jianming Jin and their
     * book "Computation of Special Functions", 1996, John Wiley & Sons, Inc.
     */
    fp_t loggam(fp_t x) const {
        fp_t x0, x2, xp, gl, gl0;
        int_t k, n;

        static fp_t a[10] = {
            8.333333333333333e-02, -2.777777777777778e-03,
            7.936507936507937e-04, -5.952380952380952e-04,
            8.417508417508418e-04, -1.917526917526918e-03,
            6.410256410256410e-03, -2.955065359477124e-02,
            1.796443723688307e-01, -1.39243221690590e+00
        };
        x0 = x;
        n = 0;
        if ((x == 1.0) || (x == 2.0))
        {
            return 0.0;
        }
        else if (x <= 7.0)
        {
            n = static_cast<int_t>(7 - x);
            x0 = x + n;
        }
        x2 = 1.0 / (x0 * x0);
        xp = 2 * M_PI;
        gl0 = a[9];
        for (k = 8; k >= 0; k--)
        {
            gl0 *= x2;
            gl0 += a[k];
        }
        gl = gl0 / x0 + 0.5 * std::log(xp) + (x0 - 0.5) * std::log(x0) - x0;
        if (x <= 7.0)
        {
            for (k = 1; k <= n; k++)
            {
                gl -= std::log(x0 - 1.0);
                x0 -= 1.0;
            }
        }
        return gl;
    }

    /* D1 = 2*sqrt(2/e) */
    /* D2 = 3 - 2*sqrt(3/e) */
    static constexpr fp_t D1 = 1.7155277699214135;
    static constexpr fp_t D2 = 0.8989161620588988;

    int_t rk_hypergeometric_hyp(int_t good, int_t bad, int_t sample) {
        int_t d1, K, Z;
        fp_t d2, U, Y;

        d1 = bad + good - sample;
        d2 = static_cast<fp_t>(std::min(bad, good));

        Y = d2;
        K = sample;
        while (Y > 0.0) {
            U = dist(rng); // rk_double(state);
            Y -= static_cast<int_t>(floor(U + Y / (d1 + K)));
            K--;
            if (K == 0) break;
        }
        Z = static_cast<int_t>(d2 - Y);
        if (good > bad) Z = sample - Z;
        return Z;
    }

    int_t rk_hypergeometric_hrua(int_t good, int_t bad, int_t sample) {
        int_t mingoodbad, maxgoodbad, popsize, m, d9;
        fp_t d4, d5, d6, d7, d8, d10, d11;
        int_t Z;
        fp_t T, W, X, Y;

        mingoodbad = std::min(good, bad);
        popsize = good + bad;
        maxgoodbad = std::max(good, bad);
        m = std::min(sample, popsize - sample);
        d4 = static_cast<fp_t>(mingoodbad) / popsize;
        d5 = 1.0 - d4;
        d6 = m * d4 + 0.5;
        d7 = sqrt(static_cast<fp_t>(popsize - m) * sample * d4 * d5 /
                  (popsize - 1) + 0.5);
        d8 = D1 * d7 + D2;
        d9 = static_cast<int_t>(floor(static_cast<fp_t>(m + 1) *
                                      (mingoodbad + 1) / (popsize + 2)));
        d10 =
            (loggam(d9 + 1) + loggam(mingoodbad - d9 + 1) + loggam(m - d9 + 1) +
             loggam(maxgoodbad - m + d9 + 1));
        d11 = std::min(std::min(m, mingoodbad) + 1.0, floor(d6 + 16 * d7));
        /* 16 for 16-decimal-digit precision in D1 and D2 */

        while (1) {
            X = dist(rng); // rk_double(state);
            Y = dist(rng); // rk_double(state);
            W = d6 + d8 * (Y - 0.5) / X;

            /* fast rejection: */
            if ((W < 0.0) || (W >= d11)) continue;

            Z = static_cast<int_t>(floor(W));
            T = d10 - (loggam(Z + 1) + loggam(mingoodbad - Z + 1) +
                       loggam(m - Z + 1) + loggam(maxgoodbad - m + Z + 1));

            /* fast acceptance: */
            if ((X * (4.0 - X) - 3.0) <= T) break;

            /* fast rejection: */
            if (X * (X - T) >= 1) continue;

            if (2.0 * std::log(X) <= T) break; /* acceptance */
        }

        /* this is a correction to HRUA* by Ivan Frohne in rv.py */
        if (good > bad) Z = m - Z;

        /* another fix from rv.py to allow sample to exceed popsize/2 */
        if (m < sample) Z = good - Z;

        return Z;
    }

    int_t rk_hypergeometric(int_t good, int_t bad, int_t sample) {
        if (sample > 10) {
            return rk_hypergeometric_hrua(good, bad, sample);
        }
        else {
            return rk_hypergeometric_hyp(good, bad, sample);
        }
    }

    // Data members:
    std::mt19937 rng;                          // random number generator
    std::uniform_real_distribution<fp_t> dist; // [0.0...1.0) distribution
};

using hypergeometric = hypergeometric_distribution<>;

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_HYPERGEOMETRIC_DISTRIBUTION_HEADER

/******************************************************************************/
