/*******************************************************************************
 * thrill/common/aggregate.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_AGGREGATE_HEADER
#define THRILL_COMMON_AGGREGATE_HEADER

#include <algorithm>
#include <cmath>

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

    //! add a value to the running aggregation
    Aggregate & Add(const Type& value) {
        if (count_ == 0) {
            min_ = value;
            max_ = value;
        }
        else {
            min_ = std::min(min_, value);
            max_ = std::max(max_, value);
        }
        total_ += value;
        total_squares_ += value * value;
        count_++;

        return *this;
    }

    //! return number of values aggregated
    size_t Count() const { return count_; }

    //! return sum over all values aggregated
    const Type & Total() const { return total_; }

    //! return the average over all values aggregated
    double Average() const {
        assert(count_ > 0);
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
        assert(count_ > 0);
        return std::sqrt(
            (static_cast<double>(total_squares_)
             - (static_cast<double>(total_) * static_cast<double>(total_)
                / static_cast<double>(count_)))
            / static_cast<double>(count_ - 1));
    }

    //! return the standard deviation of all values aggregated
    double StdDev() const { return StandardDeviation(); }

protected:
    //! number of values aggregated
    size_t count_ = 0;

    //! total sum of values
    Type total_ = Type();

    //! minimum value
    Type min_ = Type();

    //! maximum value
    Type max_ = Type();

    //! total sum of squared values (for StandardDeviation)
    Type total_squares_ = Type();
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_AGGREGATE_HEADER

/******************************************************************************/
