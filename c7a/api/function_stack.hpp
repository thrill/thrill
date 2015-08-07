/*******************************************************************************
 * c7a/api/function_stack.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_API_FUNCTION_STACK_HEADER
#define C7A_API_FUNCTION_STACK_HEADER

#include <c7a/common/function_traits.hpp>
#include <c7a/common/functional.hpp>

#include <array>
#include <cassert>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace c7a {
namespace api {

//! \defgroup api_internal API Internals
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
auto run_emitter(Lambda lambda)
{
    return [=](const auto & input)->void {
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
auto run_emitter(Lambda lambda, MoreLambdas ... rest)
{
    return [=](const auto & input)->void {
               lambda(input, run_emitter(rest ...));
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
template <typename _Input, typename ... Lambdas>
class FunctionStack
{
public:
    using Input = _Input;

    FunctionStack()
        : stack_(std::make_tuple()) { }

    /*!
     * Initialize the function chain with a given tuple of functions.
     *
     * \param stack Tuple of lambda functions.
     */
    explicit FunctionStack(std::tuple<Lambdas ...> stack)
        : stack_(stack) { }

    virtual ~FunctionStack() { }

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
    auto push(Function append_func) const
    {
        // append to function stack's type the new function: we prepend it to
        // the type line because later we will
        std::tuple<Lambdas ..., Function> new_stack
            = std::tuple_cat(stack_, std::make_tuple(append_func));

        return FunctionStack<Input, Lambdas ..., Function>(new_stack);
    }

    /*!
     * Build a single lambda function by "folding" the chain.
     * Folding means that the chain is processed from back to front
     * and each emitter is composed using previous lambda functions.
     *
     * \return Single "folded" lambda function representing the chain.
     */
    auto emit() const {
        const size_t Size = sizeof ... (Lambdas);
        return emit_sequence(common::make_index_sequence<Size>{ });
    }

private:
    //! Tuple of varying type that stores all lambda functions.
    std::tuple<Lambdas ...> stack_;

    /*!
     * Auxilary function for "folding" the chain.
     * This is needed to send all lambda functions as parameters to the
     * function that folds them together.
     *
     * \return Single "folded" lambda function representing the chain.
     */
    template <std::size_t ... Is>
    auto emit_sequence(common::index_sequence<Is ...>) const {
        return run_emitter(std::get<Is>(stack_) ...);
    }
};

template <typename Input, typename Lambda>
static inline auto MakeFunctionStack(Lambda lambda) {
    return FunctionStack<Input, Lambda>(std::make_tuple(lambda));
}

//! \}

} // namespace api
} // namespace c7a

#endif // !C7A_API_FUNCTION_STACK_HEADER

/******************************************************************************/
