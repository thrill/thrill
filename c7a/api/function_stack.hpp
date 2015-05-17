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

#include <cassert>
#include <string>
#include <vector>
#include <tuple>
#include <type_traits>
#include <array>
#include <utility>

#include "function_traits.hpp"

namespace c7a {

//! \addtogroup api Interface
//! \{

/*!
 * Base case for the chaining of lambda functions.
 * The last lambda function receives an input element, no emitter and should have no return type.
 * It should therefore store the input parameter externally.
 *
 * \param lambda Lambda function that represents the chain end.
 *
 */
template <typename L>
auto run_emitter(L lambda)
{
    using param_t = typename FunctionTraits<L>::template arg<0>;
    // auto arity = FunctionTraits<L>::arity;

    return [ = ](param_t i)->void {
               lambda(i);
    };
}

/*!
 * Recursive case for the chaining of lambda functions.
 * The given lambda function receives an input element, an emitter and should have no return type.
 * The emitter will be built using the chain of remaining lambda functions.
 *
 * \param lambda Current lambda function to be chained.
 *
 * \param rest Remaining lambda functions.
 */
template <typename L, typename ... Ls>
auto run_emitter(L lambda, Ls ... rest)
{
    using param_t = typename FunctionTraits<L>::template arg<0>;

    return [ = ](param_t i)->void {
               lambda(i, run_emitter(rest ...));
    };
}

/*!
 * A FunctionStack is a chain of lambda functions that can be folded to a single lambda functions.
 * The FunctionStack basically consists of a tuple that contains lambda functions of varying types.
 * All lambda functions within the chain receive a single input value and a emitter function.
 * The emitter function is used for chaining lambdas together.
 * The single exception to this is the last lambda function, which receives no emitter.
 *
 * \tparam Types Types of the different lambda functions.
 */
template <typename ... Types>
class FunctionStack
{
public:
    /*!
     * Default constructor that initializes an empty tuple of functions.
     */
    FunctionStack() { stack_ = std::make_tuple(); }

    /*!
     * Initialize the function chain with a fiven tuple of functions.
     *
     * \param stack Tuple of lambda functions.
     */
    FunctionStack(std::tuple<Types ...> stack)
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
    auto push(Function append_func)
    {
        // append to function stack's type the new function: we prepend it to
        // the type line because later we will
        std::tuple<Types ..., Function> new_stack
            = std::tuple_cat(stack_, std::make_tuple(append_func));

        return FunctionStack<Types ..., Function>(new_stack);
    }

    /*!
     * Build a single lambda function by "folding" the chain.
     * Folding means that the chain is processed from back to front
     * and each emitter is composed using previous lambda functions.
     *
     * \return Single "folded" lambda function representing the chain.
     */
    auto emit() {
        typedef std::tuple<Types ...> StackType;

        const size_t Size = std::tuple_size<StackType>::value;

        return emit_sequence(std::make_index_sequence<Size>{ });
    }

private:
    //! Tuple of varying type that stores all lambda functions.
    std::tuple<Types ...> stack_;

    /*!
     * Auxilary function for "folding" the chain.
     * This is needed to send all lambda functions as parameters to the
     * function that folds them together.
     *
     * \return Single "folded" lambda function representing the chain.
     */
    template <std::size_t ... Is>
    auto emit_sequence(std::index_sequence<Is ...>)
    {
        return run_emitter(std::get<Is>(stack_) ...);
    }
};

} // namespace c7a

#endif // !C7A_API_FUNCTION_STACK_HEADER

/******************************************************************************/
