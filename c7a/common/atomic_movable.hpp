/*******************************************************************************
 * c7a/common/atomic_movable.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_ATOMIC_MOVABLE_HEADER
#define C7A_COMMON_ATOMIC_MOVABLE_HEADER

#include <atomic>
#include <utility>

namespace c7a {
namespace common {

/*!
 * This is a derivative of std::atomic which enables easier and less error-prone
 * writing of move-only classes by implementing a move constructor.
 *
 * std::atomic does not have a move constructor for a good reason: atomicity
 * cannot be guaranteed. The problem is that then all move-only classes
 * containing a std::atomic must implement custom move operations. However, in
 * all our cases we only move objects during initialization. Custom move
 * operations are trivial to write but error-prone to maintain, since they must
 * contain all member variables. Missing variables create very subtle bugs,
 * hence it is better to use this atomic_movable class.
 */
template <typename T>
class atomic_movable : public std::atomic<T>
{
public:
    //! default initialization (same as std::atomic)
    atomic_movable() = default;

    //! value initialization (same as std::atomic)
    constexpr atomic_movable(T desired)
        : std::atomic<T>(desired) { }

    //! copy-construction (same as std::atomic)
    atomic_movable(const atomic_movable&) = default;

    //! move-construction NOT same as std::atomic: load and move.
    atomic_movable(const atomic_movable&& rhs)
        : std::atomic<T>(std::move(rhs)) { }

    //! assignment operator (same as std::atomic)
    T operator = (T desired) { return std::atomic<T>::operator = (desired); }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_ATOMIC_MOVABLE_HEADER

/******************************************************************************/
