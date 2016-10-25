/*******************************************************************************
 * thrill/common/aggregate.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_AGGREGATE_HEADER
#define THRILL_COMMON_AGGREGATE_HEADER

#include <thrill/common/defines.hpp>

#include <algorithm>
#include <cassert>
#include <cmath>
#include <limits>

namespace thrill {
namespace common {

/*!
 * Calculate running aggregate statistics: feed it with values, and it will keep
 * the minimum, the maximum, the average, the value number, and the standard
 * deviation is values.
 */
template <typename Type_>
class Aggregate
{
public:
    using Type = Type_;

    //! default constructor
    Aggregate() = default;

    //! initializing constructor
    Aggregate(size_t count, const double& mean, const double nvar,
              const Type& min, const Type& max) noexcept
        : count_(count), mean_(mean), nvar_(nvar),
          min_(min), max_(max) { }

    //! add a value to the running aggregation
    Aggregate& Add(const Type& value) noexcept {
        count_++;
        min_ = std::min(min_, value);
        max_ = std::max(max_, value);
        if (THRILL_UNLIKELY(count_ == 1)) {
            mean_ = value;
        }
        else {
            // Single-pass numerically stable mean and standard deviation
            // calculation as described in Donald Knuth: The Art of Computer
            // Programming, Volume 2, Chapter 4.2.2, Equations 15 & 16
            double delta = value - mean_;
            mean_ += delta / count_;
            nvar_ += delta * (value - mean_);
        }
        return *this;
    }

    //! return number of values aggregated
    size_t Count() const noexcept { return count_; }

    //! return sum over all values aggregated
    // can't make noexcept since Type_'s conversion is allowed to throw
    const Type Total() const { return static_cast<Type>(count_ * mean_); }

    //! return the average over all values aggregated
    double Average() const noexcept { return mean_; }

    //! return the average over all values aggregated
    double Avg() const noexcept { return Average(); }

    //! return the average over all values aggregated
    double Mean() const noexcept { return Average(); }

    //! return minimum over all values aggregated
    const Type& Min() const noexcept { return min_; }

    //! return maximum over all values aggregated
    const Type& Max() const noexcept { return max_; }

    //! return the standard deviation of all values aggregated
    double StandardDeviation(size_t ddof = 1) const {
        if (count_ <= 1) return 0.0;
        // ddof = delta degrees of freedom
        // Set to 0 if you have the entire distribution
        // Set to 1 if you have a sample (to correct for bias)
        return std::sqrt(nvar_ / static_cast<double>(count_ - ddof));
    }

    //! return the standard deviation of all values aggregated
    double StDev(size_t ddof = 1) const { return StandardDeviation(ddof); }

    //! operator +
    Aggregate operator + (const Aggregate& a) const noexcept {
        return Aggregate(
            // count
            count_ + a.count_,
            // mean
            (mean_ * count_ + a.mean_ * a.count_) / (count_ + a.count_),
            // merging variance is a bit complicated
            merged_variance(a),
            std::min(min_, a.min_), std::max(max_, a.max_)); // min, max
    }

    //! operator +=
    Aggregate& operator += (const Aggregate& a) noexcept {
        double total = mean_ * count_; // compute before updating count_
        count_ += a.count_;
        mean_ = (total + a.mean_ * a.count_) / count_;
        min_ = std::min(min_, a.min_);
        max_ = std::max(max_, a.max_);
        nvar_ = merged_variance(a);
        return *this;
    }

    //! serialization with Thrill's serializer
    template <typename Archive>
    void ThrillSerialize(Archive& ar) const {
        ar.template Put<size_t>(count_);
        ar.template Put<double>(mean_);
        ar.template Put<double>(nvar_);
        ar.template Put<Type>(min_);
        ar.template Put<Type>(max_);
    }

    //! deserialization with Thrill's serializer
    template <typename Archive>
    static Aggregate ThrillDeserialize(Archive& ar) {
        Aggregate agg;
        agg.count_ = ar.template Get<size_t>();
        agg.mean_ = ar.template Get<double>();
        agg.nvar_ = ar.template Get<double>();
        agg.min_ = ar.template Get<Type>();
        agg.max_ = ar.template Get<Type>();
        return agg;
    }

    static constexpr bool thrill_is_fixed_size = true;
    static constexpr size_t thrill_fixed_size =
        sizeof(size_t) + 2 * sizeof(double) + 2 * sizeof(Type);

private:
    // T. Chan et al 1979, "Updating Formulae and a Pairwise Algorithm for
    // Computing Sample Variances"
    double merged_variance(const Aggregate& other) const noexcept {
        double delta = mean_ - other.mean_;
        return nvar_ + other.nvar_ + (delta * delta) *
               (count_ * other.count_) / (count_ + other.count_);
    }

    //! number of values aggregated
    size_t count_ = 0;

    //! mean of values
    double mean_ = 0;

    //! approximate count * variance; stddev = sqrt(nvar / (count-1))
    double nvar_ = 0.0;

    //! minimum value
    Type min_ = std::numeric_limits<Type>::max();

    //! maximum value
    Type max_ = std::numeric_limits<Type>::lowest();
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_AGGREGATE_HEADER

/******************************************************************************/
