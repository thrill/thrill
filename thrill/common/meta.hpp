/*******************************************************************************
 * thrill/common/meta.hpp
 *
 * Meta Template Programming Helpers. Also: mind-boggling template
 * meta-programming for variadic template functions.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_META_HEADER
#define THRILL_COMMON_META_HEADER

#include <thrill/common/functional.hpp>

#include <cstdlib>
#include <tuple>
#include <utility>

namespace thrill {
namespace common {

/******************************************************************************/
// SFINAE check whether a class method exists.

// based on http://stackoverflow.com/questions/257288/is-it-possible
// -to-write-a-c-template-to-check-for-a-functions-existence

//! macro template for class method / attribute SFINAE test
#define THRILL_MAKE_METHOD_TEST(Method)                         \
    template <typename Type>                                    \
    class has_method_ ## Method                                 \
    {                                                           \
        template <typename C>                                   \
        static char test(decltype(&C::Method));                 \
        template <typename C>                                   \
        static int test(...);                                   \
    public:                                                     \
        enum { value = sizeof(test<Type>(0)) == sizeof(char) }; \
    };

//! macro template for class template method SFINAE test
#define THRILL_MAKE_TEMPLATE_METHOD_TEST(Method)                \
    template <typename Type, typename Param>                    \
    class has_method_ ## Method                                 \
    {                                                           \
        template <typename C>                                   \
        static char test(decltype(&C::template Method<Param>)); \
        template <typename C>                                   \
        static int test(...);                                   \
    public:                                                     \
        enum { value = sizeof(test<Type>(0)) == sizeof(char) }; \
    };

/******************************************************************************/
// Tuple Applier: takes a std::tuple<> and applies a variadic template function
// to it. Hence, this expands the content of the tuple as the arguments.

template <typename Functor, typename Tuple, std::size_t ... Is>
auto ApplyTupleImpl(Functor && f, Tuple && t, common::index_sequence<Is ...>) {
    return std::forward<Functor>(f)(
        std::get<Is>(std::forward<Tuple>(t)) ...);
}

//! Call the functor f with the contents of t as arguments.
template <typename Functor, typename Tuple>
auto ApplyTuple(Functor && f, Tuple && t) {
    using Indices = common::make_index_sequence<
              std::tuple_size<std::decay_t<Tuple> >::value>;
    return ApplyTupleImpl(std::forward<Functor>(f), std::forward<Tuple>(t),
                          Indices());
}

/******************************************************************************/
// Meta-template programming if statement.

//! If Flag is true then If<>::type is of type TypeTrue otherwise of If<>::type
//! is of type TypeFalse.
template <bool Flag, typename TypeTrue, typename TypeFalse>
struct If
{
    using type = TypeTrue;
};

template <typename TypeTrue, typename TypeFalse>
struct If<false, TypeTrue, TypeFalse>
{
    using type = TypeFalse;
};

/******************************************************************************/
// Meta-template programming log_2(n) calculation.

template <size_t Input>
class Log2Floor
{
public:
    static constexpr size_t value = Log2Floor<Input / 2>::value + 1;
};

template <>
class Log2Floor<1>
{
public:
    static constexpr size_t value = 0;
};

template <>
class Log2Floor<0>
{
public:
    static constexpr size_t value = 0;
};

template <size_t Input>
class Log2
{
public:
    static constexpr size_t floor = Log2Floor<Input>::value;
    static constexpr size_t ceil = Log2Floor<Input - 1>::value + 1;
};

template <>
class Log2<1>
{
public:
    static constexpr size_t floor = 0;
    static constexpr size_t ceil = 0;
};

template <>
class Log2<0>
{
public:
    static constexpr size_t floor = 0;
    static constexpr size_t ceil = 0;
};

/******************************************************************************/
// Variadic Template Expander: run a generic templated functor (like a generic
// lambda) for each of the variadic template parameters. Called with
// func(IndexSaver<> index, Argument arg).

//! Helper for VariadicCallForeachIndex to save the index as a compile-time
//! index
template <size_t Index>
struct IndexSaver {
    //! compile-time index
    static constexpr size_t index = Index;

    //! implicit conversion to a run-time index.
    operator size_t () const { return index; }
};

//! helper for VariadicCallForeachIndex: base case
template <size_t Index, typename Functor, typename Arg>
void VariadicCallForeachIndexImpl(Functor&& f, Arg&& arg) {
    std::forward<Functor>(f)(IndexSaver<Index>(), std::forward<Arg>(arg));
}

//! helper for VariadicCallForeachIndex: general recursive case
template <size_t Index, typename Functor, typename Arg, typename ... MoreArgs>
void VariadicCallForeachIndexImpl(
    Functor&& f, Arg&& arg, MoreArgs&& ... rest) {
    std::forward<Functor>(f)(IndexSaver<Index>(), std::forward<Arg>(arg));
    VariadicCallForeachIndexImpl<Index + 1>(
        std::forward<Functor>(f), std::forward<MoreArgs>(rest) ...);
}

//! Call a generic functor (like a generic lambda) for each variadic template
//! argument together with its zero-based index.
template <typename Functor, typename ... Args>
void VariadicCallForeachIndex(Functor&& f, Args&& ... args) {
    VariadicCallForeachIndexImpl<0>(
        std::forward<Functor>(f), std::forward<Args>(args) ...);
}

/******************************************************************************/
// Variadic Template Enumerator: run a generic templated functor (like a generic
// lambda) for the integers 0 .. Size-1 or more general [Begin,End). Called with
// func(IndexSaver<> index).

//! helper for VariadicCallEnumerate: general recursive case
template <size_t Index, size_t Size, typename Functor>
class VariadicCallEnumerateImpl
{
public:
    static void Call(Functor&& f) {
        std::forward<Functor>(f)(IndexSaver<Index>());
        VariadicCallEnumerateImpl<Index + 1, Size - 1, Functor>::Call(
            std::forward<Functor>(f));
    }
};

//! helper for VariadicCallEnumerate: base case
template <size_t Index, typename Functor>
class VariadicCallEnumerateImpl<Index, 0, Functor>
{
public:
    static void Call(Functor&& /* f */) { }
};

//! Call a generic functor (like a generic lambda) for the integers [0,Size).
template <size_t Size, typename Functor>
void VariadicCallEnumerate(Functor&& f) {
    VariadicCallEnumerateImpl<0, Size, Functor>::Call(
        std::forward<Functor>(f));
}

//! Call a generic functor (like a generic lambda) for the integers [Begin,End).
template <size_t Begin, size_t End, typename Functor>
void VariadicCallEnumerate(Functor&& f) {
    VariadicCallEnumerateImpl<Begin, End - Begin, Functor>::Call(
        std::forward<Functor>(f));
}

/******************************************************************************/
// Variadic Template Mapper: run a generic templated functor (like a generic
// lambda) for each of the variadic template parameters, and collect the return
// values in a generic std::tuple. Called with func(IndexSaver<> index, Argument
// arg).

//! helper for VariadicMapIndex: base case
template <size_t Index, typename Functor, typename Arg>
auto VariadicMapIndexImpl(Functor && f, Arg && arg)
{
    return std::make_tuple(
        std::forward<Functor>(f)(IndexSaver<Index>(), std::forward<Arg>(arg)));
}

//! helper for VariadicMapIndex: general recursive case
template <size_t Index, typename Functor, typename Arg, typename ... MoreArgs>
auto VariadicMapIndexImpl(Functor && f, Arg && arg, MoreArgs && ... rest)
{
    return std::tuple_cat(
        std::make_tuple(
            std::forward<Functor>(f)(IndexSaver<Index>(),
                                     std::forward<Arg>(arg))),
        VariadicMapIndexImpl<Index + 1>(
            std::forward<Functor>(f), std::forward<MoreArgs>(rest) ...));
}

//! Collect a generic functor (like a generic lambda) for each variadic template
//! argument together with its zero-based index.
template <typename Functor, typename ... Args>
auto VariadicMapIndex(Functor && f, Args && ... args)
{
    return VariadicMapIndexImpl<0>(
        std::forward<Functor>(f), std::forward<Args>(args) ...);
}

/******************************************************************************/
// Variadic Template Enumerate Mapper: run a generic templated functor (like a
// generic lambda) for each value from [Begin,End), and collect the return
// values in a generic std::tuple. Called with func(IndexSaver<> index).

//! helper for VariadicMapEnumerate: general recursive case
template <size_t Index, size_t Size, typename Functor>
class VariadicMapEnumerateImpl
{
public:
    static auto Map(Functor && f) {
        return std::tuple_cat(
            std::make_tuple(std::forward<Functor>(f)(IndexSaver<Index>())),
            VariadicMapEnumerateImpl<Index + 1, Size - 1, Functor>::Map(
                std::forward<Functor>(f)));
    }
};

//! helper for VariadicMapEnumerate: base case
template <size_t Index, typename Functor>
class VariadicMapEnumerateImpl<Index, 0, Functor>
{
public:
    static std::tuple<> Map(Functor&& /* f */) {
        return std::tuple<>();
    }
};

//! Call a generic functor (like a generic lambda) for the integers [0,Size),
//! and collect the return values in a generic std::tuple.
template <size_t Size, typename Functor>
auto VariadicMapEnumerate(Functor && f)
{
    return VariadicMapEnumerateImpl<0, Size, Functor>::Map(
        std::forward<Functor>(f));
}

//! Call a generic functor (like a generic lambda) for the integers [Begin,End),
//! and collect the return values in a generic std::tuple.
template <size_t Begin, size_t End, typename Functor>
auto VariadicMapEnumerate(Functor && f)
{
    return VariadicMapEnumerateImpl<Begin, End - Begin, Functor>::Map(
        std::forward<Functor>(f));
}

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_META_HEADER

/******************************************************************************/
