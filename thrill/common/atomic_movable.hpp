/*******************************************************************************
 * thrill/common/atomic_movable.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
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
    AtomicMovable(const AtomicMovable& rhs) noexcept
        : std::atomic<T>(T(rhs.load())) { }

    //! move-construction NOT same as std::atomic: load and move.
    //! Requires T to have an ctor that takes an instance of T for
    //! initialization.
    AtomicMovable(const AtomicMovable&& rhs) noexcept
        : std::atomic<T>(T(std::move(rhs.load()))) { }

    //! copy-assignment (same as std::atomic)
    AtomicMovable& operator = (const AtomicMovable& rhs) noexcept {
        std::atomic<T>::operator = (rhs.load());
        return *this;
    }

    //! move-assignment NOT same as std::atomic: load and move.
    AtomicMovable& operator = (AtomicMovable&& rhs) noexcept {
        std::atomic<T>::operator = (std::move(rhs.load()));
        return *this;
    }

    //! assignment operator (same as std::atomic)
    T operator = (T desired) noexcept { return std::atomic<T>::operator = (desired); }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_ATOMIC_MOVABLE_HEADER

/******************************************************************************/
