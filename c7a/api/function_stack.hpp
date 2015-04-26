/*******************************************************************************
 * c7a/api/function_stack.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_FUNCTION_STACK
#define C7A_API_FUNCTION_STACK

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
    auto base_emit = [](param_t i) {
        std::cout << "Base got: " << i << "\n";
    };

    return [=](param_t i) {
        lambda(i, base_emit);
    };
}

template <typename L, typename... Ls>
auto run_emitter(L lambda, Ls... rest)
{
    using param_t = typename FunctionTraits<L>::template arg<0>;
    std::cout << "Recurse\n";

    return [=](param_t i){
        lambda(i, run_emitter(rest...));
    };
}

template <typename ...Types>
class FunctionStack {
public:
    FunctionStack() { stack_ = std::make_tuple(); }
    FunctionStack(std::tuple<Types...> stack)
        : stack_(stack) {};
    virtual ~FunctionStack() { }

    template <typename Function>
    auto push(Function append_func)
    {
        // append to function stack's type the new function: we prepend it to
        // the type line because later we will
        std::tuple<Types..., Function> new_stack
            = std::tuple_cat(stack_, std::make_tuple(append_func));

        return FunctionStack<Types..., Function>(new_stack);
    }

    template <std::size_t ...Is>
    auto emit_sequence( std::index_sequence<Is...>)
    {
        return run_emitter(std::get<Is>(stack_)...);
    }

    auto emit() {
        typedef std::tuple<Types...> StackType;

        const size_t Size = std::tuple_size<StackType>::value;

        return emit_sequence(std::make_index_sequence<Size>{});
    }

private:
    std::tuple<Types...> stack_;
};

} // namespace c7a

#endif // !C7A_API_FUNCTION_STACK

/******************************************************************************/
