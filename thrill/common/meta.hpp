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

// based on http://stackoverflow.com/questions/257288/is-it-possible-to-write-a-c-template-to-check-for-a-functions-existence

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
// Variadic Template Expander: run a generic templated functor (like a generic
// lambda) for each of the variadic template parameters. Called with
// func(IndexSaver<> index, Argument arg).

//! Helper for VarCallForeachIndex to save the index as a compile-time index
template <size_t Index>
struct IndexSaver {
    //! compile-time index
    static const size_t index = Index;

    //! implicit conversion to a run-time index.
    operator size_t () const { return index; }
};

//! helper for VarCallForeachIndex: base case
template <size_t Index, typename Functor, typename Arg>
void VarCallForeachIndexImpl(Functor&& f, Arg&& arg) {
    std::forward<Functor>(f)(IndexSaver<Index>(), std::forward<Arg>(arg));
}

//! helper for VarCallForeachIndex: general recursive case
template <size_t Index, typename Functor, typename Arg, typename ... MoreArgs>
void VarCallForeachIndexImpl(
    Functor&& f, Arg&& arg, MoreArgs&& ... rest) {
    std::forward<Functor>(f)(IndexSaver<Index>(), std::forward<Arg>(arg));
    VarCallForeachIndexImpl<Index + 1>(
        std::forward<Functor>(f), std::forward<MoreArgs>(rest) ...);
}

//! Call a generic functor (like a generic lambda) for each variadic template
//! argument together with its zero-based index.
template <typename Functor, typename ... Args>
void VarCallForeachIndex(Functor&& f, Args&& ... args) {
    VarCallForeachIndexImpl<0>(
        std::forward<Functor>(f), std::forward<Args>(args) ...);
}

/******************************************************************************/
// Variadic Template Enumerator: run a generic templated functor (like a generic
// lambda) for the integers 0 .. Size-1 or more general [Begin,End). Called with
// func(IndexSaver<> index).

//! helper for VarCallEnumerate: general recursive case
template <size_t Index, size_t Size, typename Functor>
class VarCallEnumerateImpl
{
public:
    static void Call(Functor&& f) {
        std::forward<Functor>(f)(IndexSaver<Index>());
        VarCallEnumerateImpl<Index + 1, Size - 1, Functor>::Call(
            std::forward<Functor>(f));
    }
};

//! helper for VarCallEnumerate: base case
template <size_t Index, typename Functor>
class VarCallEnumerateImpl<Index, 0, Functor>
{
public:
    static void Call(Functor&& /* f */) { }
};

//! Call a generic functor (like a generic lambda) for the integers [0,Size).
template <size_t Size, typename Functor>
void VarCallEnumerate(Functor&& f) {
    VarCallEnumerateImpl<0, Size, Functor>::Call(
        std::forward<Functor>(f));
}

//! Call a generic functor (like a generic lambda) for the integers [Begin,End).
template <size_t Begin, size_t End, typename Functor>
void VarCallEnumerate(Functor&& f) {
    VarCallEnumerateImpl<Begin, End - Begin, Functor>::Call(
        std::forward<Functor>(f));
}

/******************************************************************************/
// Variadic Template Mapper: run a generic templated functor (like a generic
// lambda) for each of the variadic template parameters, and collect the return
// values in a generic std::tuple. Called with func(IndexSaver<> index, Argument
// arg).

//! helper for VarMapIndex: base case
template <size_t Index, typename Functor, typename Arg>
auto VarMapIndexImpl(Functor && f, Arg && arg)
{
    return std::make_tuple(
        std::forward<Functor>(f)(IndexSaver<Index>(), std::forward<Arg>(arg)));
}

//! helper for VarMapIndex: general recursive case
template <size_t Index, typename Functor, typename Arg, typename ... MoreArgs>
auto VarMapIndexImpl(Functor && f, Arg && arg, MoreArgs && ... rest)
{
    return std::tuple_cat(
        std::make_tuple(
            std::forward<Functor>(f)(IndexSaver<Index>(),
                                     std::forward<Arg>(arg))),
        VarMapIndexImpl<Index + 1>(
            std::forward<Functor>(f), std::forward<MoreArgs>(rest) ...));
}

//! Collect a generic functor (like a generic lambda) for each variadic template
//! argument together with its zero-based index.
template <typename Functor, typename ... Args>
auto VarMapIndex(Functor && f, Args && ... args)
{
    return VarMapIndexImpl<0>(
        std::forward<Functor>(f), std::forward<Args>(args) ...);
}

/******************************************************************************/
// Variadic Template Enumerate Mapper: run a generic templated functor (like a
// generic lambda) for each value from [Begin,End), and collect the return
// values in a generic std::tuple. Called with func(IndexSaver<> index).

//! helper for VarMapEnumerate: general recursive case
template <size_t Index, size_t Size, typename Functor>
class VarMapEnumerateImpl
{
public:
    static auto Map(Functor && f) {
        return std::tuple_cat(
            std::make_tuple(std::forward<Functor>(f)(IndexSaver<Index>())),
            VarMapEnumerateImpl<Index + 1, Size - 1, Functor>::Map(
                std::forward<Functor>(f)));
    }
};

//! helper for VarMapEnumerate: base case
template <size_t Index, typename Functor>
class VarMapEnumerateImpl<Index, 0, Functor>
{
public:
    static std::tuple<> Map(Functor&& /* f */) {
        return std::tuple<>();
    }
};

//! Call a generic functor (like a generic lambda) for the integers [0,Size),
//! and collect the return values in a generic std::tuple.
template <size_t Size, typename Functor>
auto VarMapEnumerate(Functor && f)
{
    return VarMapEnumerateImpl<0, Size, Functor>::Map(std::forward<Functor>(f));
}

//! Call a generic functor (like a generic lambda) for the integers [Begin,End),
//! and collect the return values in a generic std::tuple.
template <size_t Begin, size_t End, typename Functor>
auto VarMapEnumerate(Functor && f)
{
    return VarMapEnumerateImpl<Begin, End - Begin, Functor>::Map(
        std::forward<Functor>(f));
}

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_META_HEADER

/******************************************************************************/
