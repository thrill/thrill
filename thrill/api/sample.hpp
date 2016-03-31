/*******************************************************************************
 * thrill/api/sample.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SAMPLE_HEADER
#define THRILL_API_SAMPLE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/common/functional.hpp>

#include <random>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class SampleNode
{
    static const bool debug = false;

    using SkipDistValueType = int;

public:
    explicit SampleNode(double p)
        : p_(p), use_skip_(p < 0.1) { // use skip values if p < 0.1
        assert(p >= 0.0 && p <= 1.0);

        if (use_skip_) {
            skip_dist_ = std::geometric_distribution<SkipDistValueType>(p);
            skip_remaining_ = skip_dist_(engine_);

            LOG << "Skip value initialised with " << skip_remaining_;
        }
        else {
            simple_dist_ = std::bernoulli_distribution(p);
        }
    }

    template <typename Emitter>
    inline void operator () (const ValueType& item, Emitter&& emit) {
        if (use_skip_) {
            // use geometric distribution and skip values
            if (skip_remaining_ == 0) {
                // sample element
                LOG << "sampled item " << item;
                emit(item);
                skip_remaining_ = skip_dist_(engine_);
            }
            else {
                --skip_remaining_;
            }
        }
        else {
            // use bernoulli distribution
            if (simple_dist_(engine_)) {
                LOG << "sampled item " << item;
                emit(item);
            }
        }
    }

    bool use_skip() const {
        return use_skip_;
    }

private:
    // Sampling rate
    const double p_;
    // Whether to generate skip values with a geometric distribution or to use
    // the naive method
    const bool use_skip_;
    // Random generator
    std::default_random_engine engine_ { std::random_device { } () };
    std::bernoulli_distribution simple_dist_;
    std::geometric_distribution<SkipDistValueType> skip_dist_;
    SkipDistValueType skip_remaining_ = -1;
};

template <typename ValueType, typename Stack>
auto DIA<ValueType, Stack>::Sample(const double p) const {
    assert(IsValid());

    size_t new_id = context().next_dia_id();

    node_->context().logger_
        << "id" << new_id
        << "label" << "Sample"
        << "class" << "DIA"
        << "event" << "create"
        << "type" << "LOp"
        << "parents" << (common::Array<size_t>{ id_ });

    auto new_stack = stack_.push(SampleNode<ValueType>(p));
    return DIA<ValueType, decltype(new_stack)>(
        node_, new_stack, new_id, "Sample");
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SAMPLE_HEADER

/******************************************************************************/
