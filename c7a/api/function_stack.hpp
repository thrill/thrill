/*******************************************************************************
 * c7a/api/function_stack.hpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
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

template <typename L>
auto run_emitter(L lambda)
{
    using param_t = typename FunctionTraits<L>::template arg<0>;
    // auto arity = FunctionTraits<L>::arity;

    return [ = ](param_t i)->void {
               lambda(i);
    };
}

template <typename L, typename ... Ls>
auto run_emitter(L lambda, Ls ... rest)
{
    using param_t = typename FunctionTraits<L>::template arg<0>;
    // auto arity = FunctionTraits<L>::arity;

    return [ = ](param_t i)->void {
               lambda(i, run_emitter(rest ...));
    };
}

template <typename ... Types>
class FunctionStack
{
public:
    FunctionStack() { stack_ = std::make_tuple(); }
    FunctionStack(std::tuple<Types ...> stack)
        : stack_(stack) { }
    virtual ~FunctionStack() { }

    template <typename Function>
    auto push(Function append_func)
    {
        // append to function stack's type the new function: we prepend it to
        // the type line because later we will
        std::tuple<Types ..., Function> new_stack
            = std::tuple_cat(stack_, std::make_tuple(append_func));

        return FunctionStack<Types ..., Function>(new_stack);
    }

    template <std::size_t ... Is>
    auto emit_sequence(std::index_sequence<Is ...>)
    {
        return run_emitter(std::get<Is>(stack_) ...);
    }

    auto emit() {
        typedef std::tuple<Types ...> StackType;

        const size_t Size = std::tuple_size<StackType>::value;

        return emit_sequence(std::make_index_sequence<Size>{ });
    }

private:
    std::tuple<Types ...> stack_;
};

} // namespace c7a

#endif // !C7A_API_FUNCTION_STACK_HEADER

/******************************************************************************/
