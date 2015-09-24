/*******************************************************************************
 * thrill/common/functional.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_FUNCTIONAL_HEADER
#define THRILL_COMMON_FUNCTIONAL_HEADER

#include <algorithm>
#include <functional>
#include <utility>

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

// thanks to http://stackoverflow.com/a/7127988
template <typename T>
struct is_pair : public std::false_type { };

template <typename S, typename T>
struct is_pair<std::pair<S, T> >: public std::true_type { };

//! test if is std::array<T>
template <typename T>
struct is_std_array : public std::false_type { };

template <typename T, size_t N>
struct is_std_array<std::array<T, N> >: public std::true_type { };

//! template for constexpr min, because std::min is not good enough.
template <typename T>
constexpr static inline const T & min(const T& a, const T& b) {
    return a < b ? a : b;
}

//! template for constexpr max, because std::max is not good enough.
template <typename T>
constexpr static inline const T & max(const T& a, const T& b) {
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
