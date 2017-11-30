/*******************************************************************************
 * thrill/common/reservoir_sampling.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2017 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_RESERVOIR_SAMPLING_HEADER
#define THRILL_COMMON_RESERVOIR_SAMPLING_HEADER

#include <thrill/common/logger.hpp>

#include <cassert>
#include <cmath>
#include <vector>

namespace thrill {
namespace common {

/*!
 * Implementation of reservoir sampling using Vitter's Algorithm R. The
 * reservoir size is fixed, new items replace old ones such that all items in
 * the stream are sampled with the same uniform probability.
 */
template <typename Type, typename RNG = std::default_random_engine>
class ReservoirSampling
{
public:
    //! initialize reservoir sampler
    ReservoirSampling(size_t size, std::vector<Type>& samples,
                      RNG& rng)
        : size_(size), samples_(samples), rng_(rng) {
        samples_.reserve(size_);
    }

    //! visit item, maybe add it to the sample.
    void add(const Type& item) {
        ++count_;
        if (count_ <= size_) {
            // if reservoir is too small then store item
            samples_.emplace_back(item);
        }
        else {
            // maybe replace an item
            size_t x = rng_() % count_;
            if (x < size_)
                samples_[x] = item;
        }
    }

    //! size of reservoir
    size_t size() const { return size_; }

    //! number of items seen
    size_t count() const { return count_; }

    //! access to samples
    const std::vector<Type>& samples() const { return samples_; }

private:
    //! size of reservoir
    size_t size_;
    //! number of items seen
    size_t count_ = 0;
    //! reservoir
    std::vector<Type>& samples_;
    //! source of randomness
    RNG& rng_;
};

/*!
 * Fast exact implementation of reservoir sampling using skip values. Algorithm
 * L from Kim-Hung Li: Reservoir Sampling Algorithms of Time Complexity
 * O(n(1+log(N/n))), ACM TOMS 1994. The reservoir size is fixed, new items
 * replace old ones such that all items in the stream are sampled with the same
 * uniform probability.
 */
template <typename Type, typename RNG = std::default_random_engine>
class ReservoirSamplingFast
{
public:
    //! initialize reservoir sampler
    ReservoirSamplingFast(size_t size, std::vector<Type>& samples,
                          RNG& rng)
        : size_(size), samples_(samples), rng_(rng) {
        samples_.reserve(size_);
        W_ = std::exp(std::log(uniform(rng_)) / size);
        gap_ = std::floor(std::log(uniform(rng_)) / std::log(1-W_));
    }

    //! visit item, maybe add it to the sample.
    void add(const Type& item) {
        ++count_;
        if (count_ <= 4 * size_) {
            if (count_ <= size_) {
                // if reservoir is too small then store item
                samples_.emplace_back(item);
            }
            else {
                // use Vitter's algorithm for small count_
                size_t x = uniform(rng_) * count_;
                if (x < size_)
                    samples_[x] = item;

                // when count_ reaches 4 * size_ switch to gap algorithm
                if (count_ == 4 * size_)
                    calc_next_gap();
            }
        }
        else if (gap_ == 0) {
            // gap elapsed, this item is a sample
            size_t x = uniform(rng_) * size_;
            samples_[x] = item;

            // pick gap size: the next gap_ items are not samples
            calc_next_gap();
        }
        else {
            --gap_;
        }
    }

    //! size of reservoir
    size_t size() const { return size_; }

    //! number of items seen
    size_t count() const { return count_; }

    //! access to samples
    const std::vector<Type>& samples() const { return samples_; }

private:
    //! size of reservoir
    size_t size_;
    //! number of items seen
    size_t count_ = 0;
    //! number of items to skip until next sample
    size_t gap_;
    //! random value for gap calculation, distribution: largest value in a
    //! sample of Uniform(0, old_W) of size size_, where old_W is 1 initially
    double W_;
    //! reservoir
    std::vector<Type>& samples_;
    //! source of randomness
    RNG& rng_;
    //! uniform [0.0, 1.0) distribution
    std::uniform_real_distribution<double> uniform;

    //! draw gap size from geometric distribution with p = size_ / count_
    void calc_next_gap() {
        W_ *= std::exp(std::log(uniform(rng_)) / size_);
        gap_ = std::log(uniform(rng_)) / std::log(1 - W_);
    }
};

/*!
 * Implementation of a fast approximation of adaptive reservoir sampling using
 * http://erikerlandson.github.io/blog/2015/11/20/very-fast-reservoir-sampling/
 * The reservoir size grows logarithmically with the number given to the
 * sampler, new items replace old ones such that all items in the stream are
 * sampled with the same approximately uniform probability.
 *
 * Growing of the reservoir is implemented trivially by selecting any current
 * item to expand the array. This works well enough if growing steps become
 * rarer for larger streams.
 */
template <typename Type, typename RNG = std::default_random_engine>
class ReservoirSamplingGrow
{
    static constexpr bool debug = false;

public:
    //! initialize reservoir sampler
    ReservoirSamplingGrow(std::vector<Type>& samples,
                          RNG& rng,
                          double desired_imbalance = 0.05)
        : samples_(samples), rng_(rng), desired_imbalance_(desired_imbalance)
    { }

    //! visit item, maybe add it to the sample.
    void add(const Type& item) {
        sLOG0 << "ReservoirSamplingGrow::add"
              << "count_" << count_
              << "size_" << size_
              << "gap_" << gap_;

        ++count_;

        // check if reservoir should be resize, this is equivalent to the check
        // if (size_ != calc_sample_size())
        if (steps_to_resize == 0)
        {
            // calculate new reservoir size
            size_t target_size = calc_sample_size();
            steps_to_resize = calc_steps_to_next_resize();

            sLOG << "steps_to_resize" << steps_to_resize
                 << "target_size" << target_size
                 << "size_" << size_ << "count_" << count_
                 << "expanded_by" << target_size - size_;
            assert(target_size >= size_);

            // expand reservoir, sample new items from existing and new one
            while (size_ < target_size) {
                size_t x = rng_() % (size_ + 1);
                if (x != size_)
                    samples_.emplace_back(samples_[x]);
                else
                    samples_.emplace_back(item);
                ++size_;
            }
        }
        else {
            --steps_to_resize;
        }

        assert(samples_.size() == size_);
        if (debug && size_ != calc_sample_size())
            LOG0 << "delta: " << (int)size_ - (int)calc_sample_size()
                 << " count_ " << count_;

        if (count_ <= 4 * size_) {
            if (count_ <= size_) {
                // fill slots initially in order
                samples_[count_ - 1] = item;
            }
            else {
                // replace items using Vitter's Algorithm R
                size_t x = rng_() % count_;
                if (x < size_)
                    samples_[x] = item;

                // when count_ reaches 4 * size_ switch to gap algorithm
                if (count_ == 4 * size_)
                    gap_ = calc_next_gap();
            }
        }
        else if (gap_ == 0) {
            // gap elapsed, this item is a sample
            size_t x = rng_() % size_;
            samples_[x] = item;

            // pick gap size: the next gap_ items are not samples
            gap_ = calc_next_gap();
        }
        else {
            --gap_;
        }
    }

    //! size of reservoir
    size_t size() const { return size_; }

    //! number of items seen
    size_t count() const { return count_; }

    //! access to samples
    const std::vector<Type>& samples() const { return samples_; }

    //! desired imbalance
    double desired_imbalance() const { return desired_imbalance_; }

    //! calculate desired sample size
    size_t calc_sample_size(size_t count) const {
        size_t s = static_cast<size_t>(
            std::log2(count)
            * (1.0 / (desired_imbalance_ * desired_imbalance_)));
        return std::max(s, size_t(1));
    }

    //! calculate desired sample size
    size_t calc_sample_size() const {
        return calc_sample_size(count_);
    }

private:
    //! size of reservoir
    size_t size_ = 0;
    //! number of items seen
    size_t count_ = 0;
    //! items to skip until next sample (used in gap algorithm)
    size_t gap_ = 0;
    //! items to process prior to checking for reservoir resize
    size_t steps_to_resize = 0;
    //! reservoir
    std::vector<Type>& samples_;
    //! source of randomness
    RNG& rng_;

    //! epsilon imbalance: this reservoir sampling works well for the range 0.5
    //! to 0.01. Imbalance 0.5 results in 79 samples for 1 million items, 0.1 in
    //! 1992, 0.05 in 6643, 0.02 in 49828, and 0.01 in 199315.
    const double desired_imbalance_;

    //! draw gap size from geometric distribution with p = size_ / count_
    size_t calc_next_gap() {
        if (0) {
            // this is slower than the simpler approximation below
            return std::geometric_distribution<size_t>(
                static_cast<double>(size_) / static_cast<double>(count_))(rng_);
        }
        else {
            // generate a geometric distributed variant with p = size_ / count_
            double p = static_cast<double>(size_) / static_cast<double>(count_);
            double u = std::uniform_real_distribution<double>()(rng_);
            return std::floor(std::log(u) / std::log(1 - p));
        }
    }

    //! calculate number of items/steps to process without checking for sample
    //! resize
    size_t calc_steps_to_next_resize() const {
        return std::floor(
            count_ * (
                std::pow(2.0, desired_imbalance_ * desired_imbalance_) - 1.0));
    }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_RESERVOIR_SAMPLING_HEADER

/******************************************************************************/
