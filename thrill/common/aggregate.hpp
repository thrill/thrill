/*******************************************************************************
 * thrill/common/aggregate.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_AGGREGATE_HEADER
#define THRILL_COMMON_AGGREGATE_HEADER

#include <algorithm>
#include <cmath>
#include <limits>

namespace thrill {
namespace common {

/*!
 * Calculate running aggregate statistics: feed it with values, and it will keep
 * the minimum, the maximum, the average, the value number, and the standard
 * deviation is values.
 */
template <typename _Type>
class Aggregate
{
public:
    using Type = _Type;

    //! default constructor
    Aggregate() = default;

    //! initializing constructor
    Aggregate(size_t count, const Type& total,
              const Type& min, const Type& max, const Type& total_squares)
        : count_(count), total_(total),
          min_(min), max_(max), total_squares_(total_squares) { }

    //! add a value to the running aggregation
    Aggregate & Add(const Type& value) {
        count_++;
        total_ += value;
        min_ = std::min(min_, value);
        max_ = std::max(max_, value);
        total_squares_ += value * value;
        return *this;
    }

    //! return number of values aggregated
    size_t Count() const { return count_; }

    //! return sum over all values aggregated
    const Type & Total() const { return total_; }

    //! return the average over all values aggregated
    double Average() const {
        return static_cast<double>(total_) / static_cast<double>(count_);
    }

    //! return the average over all values aggregated
    double Avg() const { return Average(); }

    //! return minimum over all values aggregated
    const Type & Min() const { return min_; }

    //! return maximum over all values aggregated
    const Type & Max() const { return max_; }

    //! return sum over all squared values aggregated
    const Type & TotalSquares() const { return total_squares_; }

    //! return the standard deviation of all values aggregated
    double StandardDeviation() const {
        return std::sqrt(
            (static_cast<double>(total_squares_)
             - (static_cast<double>(total_) * static_cast<double>(total_)
                / static_cast<double>(count_)))
            / static_cast<double>(count_ - 1));
    }

    //! return the standard deviation of all values aggregated
    double StdDev() const { return StandardDeviation(); }

    //! operator +
    Aggregate operator + (const Aggregate& a) const {
        return Aggregate(count_ + a.count_, total_ + a.total_,
                         std::min(min_, a.min_), std::max(max_, a.max_),
                         total_squares_ + a.total_squares_);
    }

    //! operator +=
    Aggregate& operator += (const Aggregate& a) {
        count_ += a.count_;
        total_ += a.total_;
        min_ = std::min(min_, a.min_);
        max_ = std::max(max_, a.max_);
        total_squares_ += a.total_squares_;
        return *this;
    }

    //! serialization with Thrill's serializer
    template <typename Archive>
    void ThrillSerialize(Archive& ar) const {
        ar.template Put<size_t>(count_);
        ar.template Put<Type>(total_);
        ar.template Put<Type>(min_);
        ar.template Put<Type>(max_);
        ar.template Put<Type>(total_squares_);
    }

    //! deserialization with Thrill's serializer
    template <typename Archive>
    void ThrillDeserialize(Archive& ar) {
        count_ = ar.template Get<size_t>();
        total_ = ar.template Get<Type>();
        min_ = ar.template Get<Type>();
        max_ = ar.template Get<Type>();
        total_squares_ = ar.template Get<Type>();
    }

    static const bool thrill_is_fixed_size = true;
    static const size_t thrill_fixed_size = sizeof(size_t) + 4 * sizeof(Type);

private:
    //! number of values aggregated
    size_t count_ = 0;

    //! total sum of values
    Type total_ = Type();

    //! minimum value
    Type min_ = std::numeric_limits<Type>::max();

    //! maximum value
    Type max_ = std::numeric_limits<Type>::lowest();

    //! total sum of squared values (for StandardDeviation)
    Type total_squares_ = Type();
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_AGGREGATE_HEADER

/******************************************************************************/
