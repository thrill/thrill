/*******************************************************************************
 * thrill/common/atomic_movable.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_ATOMIC_MOVABLE_HEADER
#define THRILL_COMMON_ATOMIC_MOVABLE_HEADER

#include <atomic>
#include <utility>

namespace thrill {
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
 * hence it is better to use this AtomicMovable class.
 */
template <typename T>
class AtomicMovable : public std::atomic<T>
{
public:
    //! default initialization (same as std::atomic)
    AtomicMovable() = default;

    //! value initialization (same as std::atomic)
    constexpr AtomicMovable(T desired)
        : std::atomic<T>(desired) { }

    //! copy-construction (same as std::atomic)
    AtomicMovable(const AtomicMovable&) = default;

    //! move-construction NOT same as std::atomic: load and move.
    //! Requires T to have an ctor that takes an instance of T for
    //! initialization.
    AtomicMovable(const AtomicMovable&& rhs)
        : std::atomic<T>(T(std::move(rhs))) { }

    //! assignment operator (same as std::atomic)
    T operator = (T desired) { return std::atomic<T>::operator = (desired); }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_ATOMIC_MOVABLE_HEADER

/******************************************************************************/
