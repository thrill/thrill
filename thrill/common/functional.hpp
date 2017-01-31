/*******************************************************************************
 * thrill/common/functional.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_FUNCTIONAL_HEADER
#define THRILL_COMMON_FUNCTIONAL_HEADER

#include <algorithm>
#include <array>
#include <cassert>
#include <functional>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace thrill {
namespace common {

//! Identity functor, very useful for default parameters.
struct Identity {
    template <typename Type>
    constexpr auto operator () (Type&& v) const noexcept
    ->decltype(std::forward<Type>(v)) {
        return std::forward<Type>(v);
    }
};

//! The noop functor, which takes any arguments and does nothing. This is a good
//! default argument for lambda function parameters.
template <typename ReturnType>
struct NoOperation {
    ReturnType return_value_;

    explicit NoOperation(ReturnType return_value = ReturnType())
        : return_value_(return_value) { }

    ReturnType operator () (...) const noexcept {
        return return_value_;
    }
};

//! Specialized noop functor which returns a void.
template <>
struct NoOperation<void>{
    void operator () (...) const noexcept { }
};

// thanks to http://stackoverflow.com/a/7127988
template <typename T>
struct is_std_pair : public std::false_type { };

template <typename S, typename T>
struct is_std_pair<std::pair<S, T> >: public std::true_type { };

//! test if is a std::tuple<...>
template <typename T>
struct is_std_tuple : public std::false_type { };

template <typename ... Ts>
struct is_std_tuple<std::tuple<Ts ...> >: public std::true_type { };

//! test if is std::vector<T>
template <typename T>
struct is_std_vector : public std::false_type { };

template <typename T>
struct is_std_vector<std::vector<T> >: public std::true_type { };

//! test if is std::array<T>
template <typename T>
struct is_std_array : public std::false_type { };

template <typename T, size_t N>
struct is_std_array<std::array<T, N> >: public std::true_type { };

//! template for constexpr min, because std::min is not good enough.
template <typename T>
constexpr static inline const T& min(const T& a, const T& b) {
    return a < b ? a : b;
}

//! template for constexpr max, because std::max is not good enough.
template <typename T>
constexpr static inline const T& max(const T& a, const T& b) {
    return a > b ? a : b;
}

/******************************************************************************/

//! Compute the maximum of two values. This is a class, while std::max is a
//! function.
template <typename T>
class maximum : public std::binary_function<T, T, T>
{
public:
    const T& operator () (const T& x, const T& y) const {
        return std::max<T>(x, y);
    }
};

//! Compute the minimum of two values. This is a class, while std::min is a
//! function.
template <typename T>
class minimum : public std::binary_function<T, T, T>
{
public:
    const T& operator () (const T& x, const T& y) const {
        return std::min<T>(x, y);
    }
};

/******************************************************************************/

//! apply the Functor to each item in a std::vector<T> and return a new
//! std::vector<U> with different type.
template <typename Type, typename Functor>
inline auto MapVector(const std::vector<Type>&input, const Functor &f)
->std::vector<typename std::result_of<Functor(Type)>::type>
{
    std::vector<typename std::result_of<Functor(Type)>::type> output;
    output.reserve(input.size());
    for (typename std::vector<Type>::const_iterator it = input.begin();
         it != input.end(); ++it) {
        output.emplace_back(f(*it));
    }
    return output;
}

/******************************************************************************/

//! template for computing the component-wise sum of std::array or std::vector.
template <typename ArrayType,
          typename Operation = std::plus<typename ArrayType::value_type> >
class ComponentSum;

//! Compute the component-wise sum of two std::array<T,N> of same sizes.
template <typename Type, size_t N, typename Operation>
class ComponentSum<std::array<Type, N>, Operation>
    : public std::binary_function<
          std::array<Type, N>, std::array<Type, N>, std::array<Type, N> >
{
public:
    using ArrayType = std::array<Type, N>;
    explicit ComponentSum(const Operation& op = Operation()) : op_(op) { }
    ArrayType operator () (const ArrayType& a, const ArrayType& b) const {
        ArrayType out;
        for (size_t i = 0; i < N; ++i) out[i] = op_(a[i], b[i]);
        return out;
    }

private:
    Operation op_;
};

//! Compute the component-wise sum of two std::vector<T> of same sizes.
template <typename Type, typename Operation>
class ComponentSum<std::vector<Type>, Operation>
    : public std::binary_function<
          std::vector<Type>, std::vector<Type>, std::vector<Type> >
{
public:
    using VectorType = std::vector<Type>;
    explicit ComponentSum(const Operation& op = Operation()) : op_(op) { }
    VectorType operator () (const VectorType& a, const VectorType& b) const {
        assert(a.size() == b.size());
        VectorType out;
        out.reserve(std::min(a.size(), b.size()));
        for (size_t i = 0; i < min(a.size(), b.size()); ++i)
            out.emplace_back(op_(a[i], b[i]));
        return out;
    }

private:
    Operation op_;
};

//! Compute the concatenation of two std::vector<T>s.
template <typename Type>
class VectorConcat
    : public std::binary_function<
          std::vector<Type>, std::vector<Type>, std::vector<Type> >
{
public:
    using VectorType = std::vector<Type>;
    VectorType operator () (const VectorType& a, const VectorType& b) const {
        VectorType out;
        out.reserve(a.size() + b.size());
        out.insert(out.end(), a.begin(), a.end());
        out.insert(out.end(), b.begin(), b.end());
        return out;
    }
};

/******************************************************************************/

// Compile-time integer sequences, an implementation of std::index_sequence and
// std::make_index_sequence, as these are not available in many current
// libraries.
template <size_t ... Indexes>
struct index_sequence {
    static size_t size() { return sizeof ... (Indexes); }
};

template <size_t CurrentIndex, size_t ... Indexes>
struct make_index_sequence_helper;

template <size_t ... Indexes>
struct make_index_sequence_helper<0, Indexes ...>{
    using type = index_sequence<Indexes ...>;
};

template <size_t CurrentIndex, size_t ... Indexes>
struct make_index_sequence_helper {
    using type = typename make_index_sequence_helper<
              CurrentIndex - 1, CurrentIndex - 1, Indexes ...>::type;
};

template <size_t Length>
struct make_index_sequence : public make_index_sequence_helper<Length>::type
{ };

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_FUNCTIONAL_HEADER

/******************************************************************************/
