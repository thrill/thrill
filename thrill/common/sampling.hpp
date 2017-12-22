/*******************************************************************************
 * thrill/common/sampling.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Sebastian Lamm <seba.lamm@gmail.com>
 * Copyright (C) 2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_SAMPLING_HEADER
#define THRILL_COMMON_SAMPLING_HEADER

#include <thrill/common/hypergeometric_distribution.hpp>
#include <thrill/common/logger.hpp>

#include <tlx/define.hpp>
#include <tlx/math.hpp>

#include <cmath>
#include <type_traits>
#include <vector>

namespace thrill {
namespace common {

//! Sampling without replacement, implementing Algorithm R from Sanders, Lamm,
//! Hübschle-Schneider, Schrade, Dachsbacher, ACM TOMS 2017: Efficient Random
//! Sampling - Parallel, Vectorized, Cache-Efficient, and Online
template <typename RNG = std::mt19937>
class Sampling
{
public:
    static constexpr bool debug = false;

    Sampling(RNG& rng) : rng_(rng), hyp_(rng()) { }

    template <typename Iterator,
              typename Type = typename std::iterator_traits<Iterator>::value_type>
    void operator () (Iterator begin, Iterator end, size_t size,
                      std::vector<Type>& samples) {
        samples.resize(size);
        do_sample(begin, end, size, samples.begin());
    }

    template <typename Iterator, typename OutputIterator>
    void operator () (Iterator begin, Iterator end, size_t size,
                      OutputIterator out_begin) {
        do_sample(begin, end, size, out_begin);
    }

private:
    template <typename Iterator, typename OutputIterator>
    void do_sample(Iterator begin, Iterator end, size_t size,
                   OutputIterator out_begin) {
        if (size == 0) return;  // degenerate

        const size_t insize = end - begin;
        if (size == insize) {   // degenerate
            std::copy(begin, end, out_begin);
        }
        else if (insize > 64) { // recursive step
            size_t left_size = insize / 2;
            size_t left = hyp_(left_size, insize - left_size, size);
            sLOG << "Splitting input of size" << insize << "into two, left"
                 << left_size << "elements get" << left << "of"
                 << size << "samples";
            do_sample(begin, begin + left_size, left, out_begin);
            do_sample(begin + left_size, end, size - left, out_begin + left);
        }
        else if (insize > 32 && size > 8) { // hash base case
            sLOG << "Base case for size" << insize << "and" << size << "samples";
            hash_sample(begin, end, size, out_begin);
        }
        else {                              // mini base case
            sLOG << "Mini case for size" << insize << "and" << size << "samples";
            std::vector<size_t> sample;
            sample.reserve(size);
            std::uniform_int_distribution<size_t> dist(0, insize - 1);
            size_t remaining = size;
            while (remaining > 0) {
                size_t elem = dist(rng_);
                if (std::find(sample.begin(), sample.end(), elem) == sample.end()) {
                    sample.push_back(elem);
                    *(out_begin + size - remaining) = *(begin + elem);
                    --remaining;
                }
            }
        }
    }

    template <typename Iterator, typename OutIterator,
              typename Type = typename std::iterator_traits<Iterator>::value_type>
    void hash_sample(Iterator begin, Iterator end, size_t size,
                     OutIterator out_begin) {
        const size_t insize = end - begin;
        if (insize <= size) {
            // degenerate case
            std::copy(begin, end, out_begin);
            return;
        }
        sLOG << "HashSampling" << size << "of" << insize << "elements";

        std::uniform_int_distribution<size_t> dist(0, insize - 1);
        const size_t dummy = -1;
        const size_t population_lg = tlx::integer_log2_floor(insize);
        const size_t table_lg = 3 + tlx::integer_log2_floor(size);
        const size_t table_size = 1ULL << table_lg;
        const size_t address_mask = (table_lg >= population_lg) ? 0 :
                                    population_lg - table_lg;

        sLOG << "Table size:" << table_size << "(lg:" << table_lg << " pop_lg:"
             << population_lg << " mask:" << address_mask << ")";

        if (table_size > hash_table.size()) {
            sLOG << "Resizing table from" << hash_table.size() << "to"
                 << table_size;
            hash_table.resize(table_size, dummy);
        }
        indices.reserve(table_size);

        size_t remaining = size;
        while (remaining > 0) {
            size_t variate, index;
            while (true) {
                // Take sample
                variate = dist(rng_); // N * randblock[array_index++];
                index = variate >> address_mask;
                size_t hash_elem = hash_table[index];

                // Table lookup
                if (TLX_LIKELY(hash_elem == dummy)) break;  // done
                else if (hash_elem == variate) continue;    // already sampled
                else {
increment:
                    ++index;
                    index &= (table_size - 1);
                    hash_elem = hash_table[index];
                    if (hash_elem == dummy) break;            // done
                    else if (hash_elem == variate) continue;  // already sampled
                    goto increment;                           // keep incrementing
                }
            }
            // Add sample
            hash_table[index] = variate;
            *(out_begin + size - remaining) = *(begin + variate);
            sLOG << "sample no" << size - remaining << "= elem" << variate;
            indices.push_back(index);
            remaining--;
        }

        // clear table
        for (size_t index : indices)
            hash_table[index] = dummy;
        indices.clear();
    }

    RNG& rng_;
    common::hypergeometric hyp_;
    std::vector<size_t> hash_table, indices;
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_SAMPLING_HEADER

/******************************************************************************/
