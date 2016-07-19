/*******************************************************************************
 * examples/select/select.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_SELECT_SELECT_HEADER
#define THRILL_EXAMPLES_SELECT_SELECT_HEADER

#include <thrill/api/bernoulli_sample.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <cmath>
#include <functional>
#include <utility>

namespace examples {
namespace select {

using namespace thrill;              // NOLINT

static constexpr bool debug = false;

static constexpr double delta = 0.1; // 0 < delta < 0.25

static constexpr size_t base_case_size = 1024;

#define LOGM LOGC(debug && ctx.my_rank() == 0)

template <typename ValueType, typename InStack,
          typename Compare = std::less<ValueType> >
std::pair<ValueType, ValueType>
PickPivots(const DIA<ValueType, InStack>& data, size_t size, size_t rank,
           const Compare& compare = Compare()) {
    api::Context& ctx = data.context();

    const size_t num_workers(ctx.num_workers());
    const double size_d = static_cast<double>(size);

    const double p = 20 * sqrt(static_cast<double>(num_workers)) / size_d;

    // materialized at worker 0
    auto sample = data.Keep().BernoulliSample(p).Gather();

    std::pair<ValueType, ValueType> pivots;
    if (ctx.my_rank() == 0) {
        LOG << "got " << sample.size() << " samples (p = " << p << ")";
        // Sort the samples
        std::sort(sample.begin(), sample.end(), compare);

        const double base_pos =
            static_cast<double>(rank * sample.size()) / size_d;
        const double offset = pow(size_d, 0.25 + delta);

        long lower_pos = static_cast<long>(floor(base_pos - offset));
        long upper_pos = static_cast<long>(floor(base_pos + offset));

        size_t lower = static_cast<size_t>(std::max(0L, lower_pos));
        size_t upper = static_cast<size_t>(
            std::min(upper_pos, static_cast<long>(sample.size() - 1)));

        assert(0 <= lower && lower < sample.size());
        assert(0 <= upper && upper < sample.size());

        LOG << "Selected pivots at positions " << lower << " and " << upper
            << ": " << sample[lower] << " and " << sample[upper];

        pivots = std::make_pair(sample[lower], sample[upper]);
    }

    pivots = ctx.net.Broadcast(pivots);

    LOGM << "pivots: " << pivots.first << " and " << pivots.second;

    return pivots;
}

template <typename ValueType, typename InStack,
          typename Compare = std::less<ValueType> >
ValueType Select(const DIA<ValueType, InStack>& data, size_t rank,
                 const Compare& compare = Compare()) {
    api::Context& ctx = data.context();
    const size_t size = data.Keep().Size();

    assert(0 <= rank && rank < size);

    if (size <= base_case_size) {
        // base case, gather all data at worker with rank 0
        ValueType result = ValueType();
        auto elements = data.Gather();

        if (ctx.my_rank() == 0) {
            assert(rank < elements.size());
            std::nth_element(elements.begin(), elements.begin() + rank,
                             elements.end(), compare);

            result = elements[rank];

            LOG << "base case: " << size << " elements remaining, result is "
                << result;
        }

        result = ctx.net.Broadcast(result);
        return result;
    }

    ValueType left_pivot, right_pivot;
    std::tie(left_pivot, right_pivot) = PickPivots(data, size, rank, compare);

    size_t left_size, middle_size, right_size;

    using PartSizes = std::pair<size_t, size_t>;
    std::tie(left_size, middle_size) =
        data.Keep().Map(
            [&](const ValueType& elem) -> PartSizes {
                if (compare(elem, left_pivot))
                    return PartSizes { 1, 0 };
                else if (!compare(right_pivot, elem))
                    return PartSizes { 0, 1 };
                else
                    return PartSizes { 0, 0 };
            })
        .Sum(
            [](const PartSizes& a, const PartSizes& b) -> PartSizes {
                return PartSizes { a.first + b.first, a.second + b.second };
            },
            PartSizes { 0, 0 });
    right_size = size - left_size - middle_size;

    LOGM << "left_size = " << left_size << ", middle_size = " << middle_size
         << ", right_size = " << right_size << ", rank = " << rank;

    if (rank == left_size) {
        // all the elements strictly smaller than the left pivot are on the left
        // side -> left_size-th element is the left pivot
        LOGM << "result is left pivot: " << left_pivot;
        return left_pivot;
    }
    else if (rank == left_size + middle_size - 1) {
        // only the elements strictly greater than the right pivot are on the
        // right side, so the result is the right pivot in this case
        LOGM << "result is right pivot: " << right_pivot;
        return right_pivot;
    }
    else if (rank < left_size) {
        // recurse on the left partition
        LOGM << "Recursing left, " << left_size
             << " elements remaining (rank = " << rank << ")\n";

        auto left = data.Keep().Filter(
            [&](const ValueType& elem) -> bool {
                return compare(elem, left_pivot);
            }).Collapse();
        assert(left.Size() == left_size);

        return Select(left, rank, compare);
    }
    else if (left_size + middle_size <= rank) {
        // recurse on the right partition
        LOGM << "Recursing right, " << right_size
             << " elements remaining (rank = " << rank - left_size - middle_size
             << ")\n";

        auto right = data.Keep().Filter(
            [&](const ValueType& elem) -> bool {
                return compare(right_pivot, elem);
            }).Collapse();
        assert(right.Size() == right_size);

        return Select(right, rank - left_size - middle_size, compare);
    }
    else {
        // recurse on the middle partition
        LOGM << "Recursing middle, " << middle_size
             << " elements remaining (rank = " << rank - left_size << ")\n";

        auto middle = data.Keep().Filter(
            [&](const ValueType& elem) -> bool {
                return !compare(elem, left_pivot) &&
                !compare(right_pivot, elem);
            }).Collapse();
        assert(middle.Size() == middle_size);

        return Select(middle, rank - left_size, compare);
    }
}

} // namespace select
} // namespace examples

#endif // !THRILL_EXAMPLES_SELECT_SELECT_HEADER

/******************************************************************************/
