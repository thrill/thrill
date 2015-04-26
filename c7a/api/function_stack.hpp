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

namespace c7a {

template <typename ...Types>
class FunctionStack {
public:
    FunctionStack() {
        stack_ = std::make_tuple();
    }
    FunctionStack(std::tuple<Types...> stack) 
        : stack_(stack) {};
    virtual ~FunctionStack() { }

    template <typename Function>
    auto push(Function append_func) {
        std::tuple<Function, Types...> new_stack = std::tuple_cat(std::make_tuple(append_func), stack_);
        std::cout << "Stack Size(PUSH): " <<
            std::tuple_size<decltype(new_stack)>::value << std::endl;
        return FunctionStack<Function, Types...>(new_stack);
    } 

    template<typename ...Elements>
    auto pop() {
        // Create new stack type
        typedef std::tuple<Types...> StackType;

        // Create new stack
        constexpr auto Size = std::tuple_size<StackType>::value;
        auto func_and_stack 
            = PopFromStack(stack_, std::make_index_sequence<Size>{});

        auto pop_func = func_and_stack.first;
        auto new_stack = std::make_tuple(1);
        auto new_func_stack = FunctionStack<int>(new_stack);
        // std::tuple<Elements...> new_stack = func_and_stack.second;
        // auto new_func_stack = FunctionStack<Elements...>(new_stack);

        std::cout << "Stack Size(POP): " << 
            std::tuple_size<decltype(new_stack)>::value << std::endl;

        return std::make_pair(pop_func, new_func_stack);
    }

    template<typename T, std::size_t I0, std::size_t ...I>
    auto PopFromStack(T&& t, std::index_sequence<I0, I...>) {
        return std::make_pair(std::get<I0>(t), 
                std::make_tuple(std::get<I>(t)...));
    }

private:
    std::tuple<Types...> stack_;
};

} // namespace c7a

#endif // !C7A_API_FUNCTION_STACK

/******************************************************************************/
