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

void run_emitter()
{
    std::cout << "base\n";
}

template <typename T1, typename... T>
void run_emitter(T1 first_thing, T... rest)
{
    std::cout << "nicht base\n";
    first_thing();

    run_emitter(rest...);
}

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
    auto push(Function append_func)
    {
        // append to function stack's type the new function: we prepend it to
        // the type line because later we will
        std::tuple<Function, Types...> new_stack
            = std::tuple_cat(std::make_tuple(append_func), stack_);

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

    template <std::size_t ...Is>
    void emit2( std::index_sequence<Is...>)
    {
        run_emitter(std::get<Is>(stack_)...);
    }

    void emit(int i) {
        typedef std::tuple<Types...> StackType;

        const size_t Size = std::tuple_size<StackType>::value;

        emit2(std::make_index_sequence<Size>{});

        //run_emitter(1,2,34,45);
    }

private:
    std::tuple<Types...> stack_;
};

} // namespace c7a

#endif // !C7A_API_FUNCTION_STACK

/******************************************************************************/
