/*******************************************************************************
 * thrill/api/function_stack.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_FUNCTION_STACK_HEADER
#define THRILL_API_FUNCTION_STACK_HEADER

#include <thrill/common/function_traits.hpp>
#include <thrill/common/functional.hpp>

#include <array>
#include <cassert>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace thrill {
namespace api {

//! \ingroup api
//! \{

/*!
 * Base case for the chaining of lambda functions.  The last lambda function
 * receives an input element, no emitter and should have no return type.  It
 * should therefore store the input parameter externally.
 *
 * \param lambda Lambda function that represents the chain end.
 */
template <typename Lambda>
auto RunEmitter(const Lambda& lambda) {
    // lambda is captured by non-const copy so that we can use functors with
    // non-const operator(), i.e. stateful functors (e.g. for sampling)
    return [ =, lambda = lambda](const auto& input) mutable->void {
               lambda(input);
    };
}

/*!
 * Recursive case for the chaining of lambda functions.  The given lambda
 * function receives an input element, an emitter and should have no return
 * type.  The emitter will be built using the chain of remaining lambda
 * functions.
 *
 * \param lambda Current lambda function to be chained.
 *
 * \param rest Remaining lambda functions.
 */
template <typename Lambda, typename ... MoreLambdas>
auto RunEmitter(const Lambda& lambda, const MoreLambdas& ... rest) {
    // lambda is captured by non-const copy so that we can use functors with
    // non-const operator(), i.e. stateful functors (e.g. for sampling)
    return [ =, lambda = lambda](const auto& input) mutable->void {
               lambda(input, RunEmitter(rest ...));
    };
}

/*!
 * A FunctionStack is a chain of lambda functions that can be folded to a single
 * lambda functions.  The FunctionStack basically consists of a tuple that
 * contains lambda functions of varying types.  All lambda functions within the
 * chain receive a single input value and a emitter function.  The emitter
 * function is used for chaining lambdas together.  The single exception to this
 * is the last lambda function, which receives no emitter.
 *
 * \tparam Lambdas Types of the different lambda functions.
 */
template <typename Input_, typename ... Lambdas>
class FunctionStack
{
public:
    using Input = Input_;

    FunctionStack() = default;

    /*!
     * Initialize the function chain with a given tuple of functions.
     *
     * \param stack Tuple of lambda functions.
     */
    explicit FunctionStack(const std::tuple<Lambdas ...>& stack)
        : stack_(stack) { }

    /*!
     * Add a lambda function to the end of the chain.
     *
     * \tparam Function Type of the lambda functions.
     *
     * \param append_func Lambda function that should be added to the chain.
     *
     * \return New chain containing the previous and new lambda function(s).
     */
    template <typename Function>
    auto push(const Function& append_func) const {
        // append to function stack's type the new function: we prepend it to
        // the type line because later we will
        std::tuple<Lambdas ..., Function> new_stack
            = std::tuple_cat(stack_, std::make_tuple(append_func));

        return FunctionStack<Input, Lambdas ..., Function>(new_stack);
    }

    /*!
     * Build a single lambda function by "folding" the chain.  Folding means
     * that the chain is processed from back to front and each emitter is
     * composed using previous lambda functions.
     *
     * \return Single "folded" lambda function representing the chain.
     */
    auto fold() const {
        const size_t Size = sizeof ... (Lambdas);
        return EmitSequence(common::make_index_sequence<Size>{ });
    }

    //! Is true if the FunctionStack is empty.
    static constexpr bool empty = (sizeof ... (Lambdas) == 0);

private:
    //! Tuple of varying type that stores all lambda functions.
    std::tuple<Lambdas ...> stack_;

    /*!
     * Auxilary function for "folding" the chain.  This is needed to send all
     * lambda functions as parameters to the function that folds them together.
     *
     * \return Single "folded" lambda function representing the chain.
     */
    template <size_t ... Is>
    auto EmitSequence(common::index_sequence<Is ...>) const {
        return RunEmitter(std::get<Is>(stack_) ...);
    }
};

template <typename Input, typename Lambda>
static inline auto MakeFunctionStack(const Lambda& lambda) {
    return FunctionStack<Input, Lambda>(std::make_tuple(lambda));
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_FUNCTION_STACK_HEADER

/******************************************************************************/
