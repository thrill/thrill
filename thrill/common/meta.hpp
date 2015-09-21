/*******************************************************************************
 * thrill/common/meta.hpp
 *
 * Meta Template Programming Helpers
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_META_HEADER
#define THRILL_COMMON_META_HEADER

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

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_META_HEADER

/******************************************************************************/
