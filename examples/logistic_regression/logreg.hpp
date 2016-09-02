/*******************************************************************************
 * examples/logistic_regression/logreg.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 * Copyright (C) 2015-16 Utkarsh Bhardwaj <haptork@gmail.com>
 *
 * Based on examples/logreg.cpp of https://github.com/haptork/easyLambda
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/


#pragma once

#ifndef THRILL_EXAMPLES_LOGISTIC_REGRESSION_LOGREG_HEADER
#define THRILL_EXAMPLES_LOGISTIC_REGRESSION_LOGREG_HEADER

#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <functional>
#include <numeric>
#include <utility>

#define LOGM LOGC(debug && ctx.my_rank() == 0)

namespace examples {
namespace logistic_regression {

using namespace thrill; // NOLINT
static constexpr bool debug = true;


template <typename T>
inline T sigmoid(const T &x) { // can't make it constexpr, exp isn't one
    return 1.0 / (1.0 + exp(-x));
}


template <typename T, size_t dim>
T calc_norm(const std::array<T, dim> &weights,
                const std::array<T, dim> &new_weights) {
  T sum = 0.;
  for (size_t i = 0; i < dim; ++i) {
    T diff = weights[i] - new_weights[i];
    sum += (diff * diff);
  }
  return std::sqrt(sum);
}

template <typename T, size_t dim>
auto gradient(const T &y, const std::array<T, dim> &x,
              const std::array<T, dim> &w) {
    std::array<T, dim> grad;
    T dot_product = std::inner_product(w.begin(), w.end(), x.begin(), T{0.0});
    T s = sigmoid(dot_product) - y;
    for (size_t i = 0; i < dim; ++i) {
        grad[i] = s * x[i];
    }
    return grad;
}

template <typename T, size_t dim, typename InStack,
          typename Element = std::array<T, dim>>
auto train_logreg(const DIA<std::pair<T, Element>, InStack> &data,
                     size_t max_iterations, double gamma = 0.002,
                     double epsilon = 0.0001)
{
    Element weights{{0.0}}, new_weights; // weights, initialized to zero
    size_t iter = 0;
    T norm = 0.;

    while (iter < max_iterations) {
        Element grad = data
            .Map(
                [&weights](const std::pair<T, Element> &elem) -> Element {
                    return gradient(elem.first, elem.second, weights);
                })
            .Sum([](const Element &a, const Element &b) -> Element {
                    Element result;
                    std::transform(a.begin(), a.end(), b.begin(),
                                   result.begin(), std::plus<T>());
                    return result;
                });

        std::transform(weights.begin(), weights.end(), grad.begin(),
                       new_weights.begin(),
                       [&gamma](const T &a, const T&b) -> T
                       { return a - gamma * b; });

        norm = calc_norm(new_weights, weights);
        weights = new_weights;

        iter++;
        if (norm < epsilon) break;
    }

    return std::make_tuple(weights, norm, iter);
}


template <typename T, size_t dim, typename InStack,
          typename Element = std::array<T, dim>>
auto test_logreg(const DIA<std::pair<T, Element>, InStack> &data,
                 const Element &weights)
{
    size_t expected_true = data
        .Filter(
            [](const std::pair<T, Element> &elem) -> bool {
                return static_cast<bool>(elem.first);
            })
        .Size();
    size_t expected_false = data.Size() - expected_true;

    using Prediction = std::pair<bool, bool>;
    auto classification = data
        .Map(
            [&weights](const std::pair<T, Element> &elem) -> Prediction {
                const Element &coords = elem.second;
                T predicted_y = std::inner_product(
                    weights.begin(), weights.end(), coords.begin(), T{0.0});

                bool prediction = (sigmoid(predicted_y) > 0.5);
                return Prediction{elem.first, prediction};
            })
        .Cache(); // don't evaluate this twice

    size_t true_trues = classification
        .Filter([](const Prediction &p) { return p.first && p.second; })
        .Size();

    size_t true_falses = classification
        .Filter([](const Prediction &p) { return !p.first && !p.second; })
        .Size();

    return std::make_tuple(expected_true, true_trues,
                           expected_false, true_falses);

}

} // namespace logistic_regression
} // namespace examples

#endif // !THRILL_EXAMPLES_LOGISTIC_REGRESSION_LOGREG_HEADER

/******************************************************************************/
