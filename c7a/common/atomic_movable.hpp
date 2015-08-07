/*******************************************************************************
 * c7a/common/atomic_movable.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_ATOMIC_MOVABLE_HEADER
#define C7A_COMMON_ATOMIC_MOVABLE_HEADER

#include <atomic>
#include <condition_variable>
#include <mutex>
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

/*! PSEUDO Movable Mutex.
 * As described above, we do require the move semantic only during
 * initialization. This mutex breaks any semantics during operation because
 * the state of the mutex is lost, when move-constructed
 */
class MutexMovable : public std::mutex
{
public:
    //! default initialization (same as std::mutex)
    MutexMovable() = default;

    //! copy-construction (same as std::mutex)
    MutexMovable(const MutexMovable&) = default;

    //! move-construction NOT same as std::mutex: load and move.
    MutexMovable(const MutexMovable&& /*rhs*/)
        : std::mutex() { }
};

/*! PSEUDO movable ConditionVariableAny.
 * As described above, we do require the move semantic only during
 * initialization. This condition variable breaks any semantics during
 * operation because the state of the cv is lost, when move-constructed
 */
class ConditionVariableAnyMovable : public std::condition_variable_any
{
public:
    //! default initialization (same as std::condition_variable_any)
    ConditionVariableAnyMovable() = default;

    //! copy-construction (same as std::condition_variable_any)
    ConditionVariableAnyMovable(const ConditionVariableAnyMovable&) = default;

    //! move-construction NOT same as std::condition_variable_any: load and move.
    ConditionVariableAnyMovable(const ConditionVariableAnyMovable&& /*rhs*/)
        : std::condition_variable_any() { }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_ATOMIC_MOVABLE_HEADER

/******************************************************************************/
