/*******************************************************************************
 * examples/stochastic_gradient_descent/stochastic_gradient_descent.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Clemens Wallrath <clemens.wallrath@nerven.gift>
 * Copyright (C) 2017 Alina Saalfeld <alina.saalfeld@ymail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_STOCHASTIC_GRADIENT_DESCENT_STOCHASTIC_GRADIENT_DESCENT_HEADER
#define THRILL_EXAMPLES_STOCHASTIC_GRADIENT_DESCENT_STOCHASTIC_GRADIENT_DESCENT_HEADER

#include <thrill/api/bernoulli_sample.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/vector.hpp>

#include <cereal/types/vector.hpp>
#include <thrill/data/serialization_cereal.hpp>

#include <algorithm>
#include <utility>

namespace examples {
namespace stochastic_gradient_descent {

using thrill::DIA;

template <size_t D>
using Vector = thrill::common::Vector<D, double>;

using VVector = thrill::common::VVector<double>;

//! Model for one point consisting of a d-dimensional position and a label
template <typename Vector>
struct DataPoint {
    Vector data;
    double label;

    DataPoint()
        : data(), label(0) { }
    DataPoint(Vector data, double label)
        : data(data), label(label) { }

    template <typename Archive>
    void serialize(Archive& archive) {
        archive(data, label);
    }
};

template <typename Vector>
std::ostream& operator << (std::ostream& os, const DataPoint<Vector>& p) {
    return os << "data: " << p.data << ", label: " << p.label;
}

// weights, loss
template <typename Vector>
using GradientResult = std::pair<Vector, double>;

// size_t for counting the actual number of data points
template <typename Vector>
using SumResult = std::pair<GradientResult<Vector>, size_t>;

//! simple implementation of a gradient computation class using a least squares
//! cost function and a linear model (y = w*x)
template <typename Vector>
class LeastSquaresGradient
{
public:
    static GradientResult<Vector> Compute(Vector data, double label, Vector weights) {
        auto diff = data.dot(weights) - label;
        auto loss = 0.5 * diff * diff;
        auto gradient = diff * data;
        return GradientResult<Vector>(gradient, loss);
    }
};

template <typename Vector>
class StochasticGradientDescent
{
public:
    StochasticGradientDescent(
        size_t num_iterations, double mini_batch_fraction,
        double step_size, double tolerance)
        : num_iterations(num_iterations),
          mini_batch_fraction(mini_batch_fraction),
          step_size(step_size), tolerance(tolerance)
    { }

    //! do the actual computation
    Vector optimize(const DIA<DataPoint<Vector> >& input_points,
                    Vector& initial_weights) {
        auto weights = initial_weights;
        bool converged = false;
        size_t i = 1;
        while (!converged && i <= num_iterations) {
            LOG1 << "weights: " << weights;
            auto old_weights = weights;
            auto sample = input_points.BernoulliSample(mini_batch_fraction);
            auto sum_result =
                sample
                .Map([&weights](const DataPoint<Vector> p) {
                         auto grad = LeastSquaresGradient<Vector>::Compute(
                             p.data, p.label, weights);
                         return SumResult<Vector>(grad, 1);
                     })
                .Sum(
                    [](SumResult<Vector> a, SumResult<Vector> b) {
                        auto grad_a = a.first;
                        auto n_a = a.second;
                        auto grad_b = b.first;
                        auto n_b = b.second;
                        return SumResult<Vector>(
                            // gradient, loss
                            GradientResult<Vector>(
                                grad_a.first + grad_b.first,
                                grad_a.second + grad_b.second),
                            // number of data points (BernoulliSample yields
                            // only an approximate fraction)
                            n_a + n_b);
                    },
                    SumResult<Vector>(
                        GradientResult<Vector>(
                            Vector::Make(weights.size()).fill(0.0), 0.0), 0));

            auto weight_gradient_sum = sum_result.first;
            size_t num_points = sum_result.second;

            LOG1 << "n: " << num_points;
            LOG1 << "grad: " << weight_gradient_sum.first;
            LOG1 << "loss: " << weight_gradient_sum.second;

            // w = w - eta sum_i=0^n Q(w_i) / n
            // with
            // adaptive step_size η
            // gradient Q(w_i)
            weights = weights - (step_size / sqrt(i)) * weight_gradient_sum.first / num_points;
            ++i;
            converged = is_converged(old_weights, weights, tolerance);
        }
        LOG1 << "iterations: " << i;
        return weights;
    }

private:
    size_t num_iterations;
    double mini_batch_fraction;
    double step_size;
    double tolerance;

    bool is_converged(Vector& old, Vector& current, double tolerance) {
        return (old.Distance(current) < tolerance * std::max(current.Norm(), 1.0));
    }
};

} // namespace stochastic_gradient_descent
} // namespace examples

#endif // !THRILL_EXAMPLES_STOCHASTIC_GRADIENT_DESCENT_STOCHASTIC_GRADIENT_DESCENT_HEADER

/******************************************************************************/
