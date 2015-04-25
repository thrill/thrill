/*******************************************************************************
 * c7a/api/function_traits.hpp
 *
 * Model real-time or backtesting Portfolio with Positions, TradeLog and more.
 ******************************************************************************/

#ifndef C7A_API_FUNCTION_TRAITS_HEADER
#define C7A_API_FUNCTION_TRAITS_HEADER

#include <tuple>

namespace c7a {

// taken from: http://stackoverflow.com/questions/7943525/is-it-possible-to-figure-out-the-parameter-type-and-return-type-of-a-lambda
template <typename T>
struct FunctionTraits : public FunctionTraits<decltype(& T::operator ())>{ };
// For generic types, directly use the result of the signature of its 'operator()'

template <typename ClassType, typename ReturnType, typename ... Args>
struct FunctionTraits<ReturnType (ClassType::*)(Args ...) const>{
    // we specialize for pointers to member function
    enum { arity = sizeof ... (Args) };
    // arity is the number of arguments.

    using result_type = ReturnType;

    template <size_t i>
    using arg = typename std::tuple_element<i, std::tuple<Args ...> >::type;
    // the i-th argument is equivalent to the i-th tuple element of a tuple
    // composed of those arguments.
};

} // namespace c7a

#endif // !C7A_API_FUNCTION_TRAITS_HEADER

/******************************************************************************/
