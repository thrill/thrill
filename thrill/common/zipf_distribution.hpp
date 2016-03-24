/*******************************************************************************
 * thrill/common/zipf_distribution.hpp
 *
 * A Zipf distribution random generator using std::discrete_distribution and a
 * computed probability table.
 *
 * Code borrowed from
 * https://github.com/gkohri/discreteRNG/blob/master/zipf-mandelbrot.h
 *
 * Copyright (c) 2015, G.A. Kohring
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * * Redistributions of source code must retain the above copyright
 *   notice, this list of conditions and the following disclaimer.
 *
 * * Redistributions in binary form must reproduce the above copyright
 *   notice, this list of conditions and the following disclaimer in the
 *   documentation and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_ZIPF_DISTRIBUTION_HEADER
#define THRILL_COMMON_ZIPF_DISTRIBUTION_HEADER

#include <thrill/common/logger.hpp>

#include <random>
#include <vector>

namespace thrill {
namespace common {

/*!
 * A class for producing random integers distributed according to the
 * Zipf-Mandelbrot probability mass function:
 *
 *           p(k;N,q,s) = 1/( H(N,q,s)*(k+q)^s )
 *
 * where
 *           H(N,q,s) = sum_(n=1)^(N) 1/(n+q)^s
 *
 * and s > 1, q >= 0, N > 1.
 *
 * When q = 0 this becomes the mass function for Zipf's Law
 * When N -> infinity this becomes the Hurwitz Zeta mass function
 * When N -> infinity and q = 0, this becomes the Riemann Zeta mass function
 */
class ZipfDistribution
{
public:
    //! create uninitialized object. be careful.
    ZipfDistribution() = default;

    /*!
     * Creates a new Zipf-Mandelbrot distribution given s, q, N
     *
     *           p(k;N,q,s) = 1/( H(N,q,s)*(k+q)^s )
     *
     * where
     *           H(N,q,s) = sum_(n=1)^(N) 1/(n+q)^s
     *
     * and s > 1, q >= 0, N > 1
     *
     * Only N and s needs to be specified. The default for q is 0.
     */
    ZipfDistribution(const size_t N, const double s, const double q = 0)
        : N_(N), s_(s), q_(q),
          dist_(make_dist(N, s, q)) { }

    //! pick next random number in the range [1,num)
    template <typename Engine>
    size_t operator () (Engine& eng) { return dist_(eng) + 1; }

    //! deliver population size
    size_t N() const { return N_; }

    //! parameter of distribution
    double q() const { return q_; }

    //! parameter of distribution
    double s() const { return s_; }

    //! minimum value of distribution
    size_t min() const { return 1; }

    //! maximum value (inclusive) of distribution
    size_t max() const { return N_; }

private:
    size_t N_;
    double s_;
    double q_;

    using dist_type = std::discrete_distribution<size_t>;
    dist_type dist_;

    static dist_type make_dist(size_t N, double s, double q) {
        if (!(s > 0.0)) {
            LOG1 << "s (" << s << ") must be greater than 0.0.";
            abort();
        }

        std::vector<double> probs(N);

        double p_sum = 0.0;
        for (size_t k = 1; k < N + 1; ++k) {
            double prob = 1.0 / std::pow(static_cast<double>(k) + q, s);
            p_sum += prob;
            probs[k - 1] = prob;
        }

        double p_norm = 1.0 / p_sum;
        for (size_t i = 0; i < N; ++i) {
            probs[i] *= p_norm;
        }

        return dist_type(probs.begin(), probs.end());
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_ZIPF_DISTRIBUTION_HEADER

/******************************************************************************/
