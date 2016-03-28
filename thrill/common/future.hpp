/*******************************************************************************
 * thrill/common/future.hpp
 *
 * std::promise and std::future implementations borrowed from libc++ under the
 * MIT license.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_FUTURE_HEADER
#define THRILL_COMMON_FUTURE_HEADER

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <system_error>
#include <thread>

namespace thrill {
namespace common {

template <typename Type>
class promise;
template <typename Type>
class future;
template <typename Type>
class shared_future;

void rethrow_exception(std::exception_ptr p);

/******************************************************************************/

class shared_count
{
    shared_count(const shared_count&);
    shared_count& operator = (const shared_count&);

protected:
    std::atomic<long> shared_owners_;
    virtual ~shared_count() { }

private:
    virtual void on_zero_shared() noexcept = 0;

public:
    explicit shared_count(long refs = 0) noexcept
        : shared_owners_(refs) { }

    void add_shared() noexcept {
        shared_owners_.fetch_add(1, std::memory_order_relaxed);
    }

    bool release_shared() noexcept {
        if (shared_owners_.fetch_sub(1, std::memory_order_acq_rel) == 0) {
            on_zero_shared();
            return true;
        }
        return false;
    }

    long use_count() const noexcept {
        return shared_owners_.load(std::memory_order_relaxed) + 1;
    }
};

template <class Alloc>
class allocator_destructor
{
    using alloc_traits = std::allocator_traits<Alloc>;

public:
    using pointer = typename alloc_traits::pointer;
    using size_type = typename alloc_traits::size_type;

private:
    Alloc& alloc_;
    size_type s_;

public:
    allocator_destructor(Alloc& a, size_type s) noexcept
        : alloc_(a), s_(s) { }

    void operator () (pointer p) noexcept {
        alloc_traits::deallocate(alloc_, p, s_);
    }
};

enum class future_errc
{
    future_already_retrieved = 1,
    promise_already_satisfied,
    no_state,
    broken_promise
};

enum class future_status
{
    ready,
    timeout
};

const std::error_category& future_category() noexcept;

inline std::error_code
make_error_code(future_errc e) noexcept {
    return std::error_code(static_cast<int>(e), future_category());
}

inline std::error_condition
make_error_condition(future_errc e) noexcept {
    return std::error_condition(static_cast<int>(e), future_category());
}

class future_error : public std::logic_error
{
    std::error_code ec_;

public:
    explicit future_error(std::error_code ec)
        : logic_error(ec.message()), ec_(ec)
    { }

    future_error(const future_error&) = default;

    const std::error_code& code() const noexcept { return ec_; }

    virtual ~future_error() noexcept { }
};

inline void throw_future_error(future_errc Ev) {
    throw future_error(make_error_code(Ev));
}

/******************************************************************************/
// assoc_sub_state

class assoc_sub_state : public shared_count
{
protected:
    std::exception_ptr exception_;
    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;

    unsigned state_;

    virtual void on_zero_shared() noexcept {
        delete this;
    }

    void sub_wait(std::unique_lock<std::mutex>& lock) {
        if (!is_ready()) {
            while (!is_ready())
                cv_.wait(lock);
        }
    }

public:
    enum
    {
        constructed = 1,
        future_attached = 2,
        ready = 4
    };

    assoc_sub_state() : state_(0) { }

    bool has_value() const {
        return (state_ & constructed) || (exception_ != nullptr);
    }

    void set_future_attached() {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ |= future_attached;
    }

    bool has_future_attached() const { return (state_ & future_attached) != 0; }

    void make_ready() {
        std::unique_lock<std::mutex> lock(mutex_);
        state_ |= ready;
        cv_.notify_all();
    }

    bool is_ready() const { return (state_ & ready) != 0; }

    void set_value() {
        std::unique_lock<std::mutex> lock(mutex_);
        if (has_value())
            throw future_error(make_error_code(future_errc::promise_already_satisfied));
        state_ |= constructed | ready;
        cv_.notify_all();
    }

    void set_exception(std::exception_ptr p) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (has_value())
            throw future_error(make_error_code(future_errc::promise_already_satisfied));
        exception_ = p;
        state_ |= ready;
        cv_.notify_all();
    }

    void copy() {
        std::unique_lock<std::mutex> lock(mutex_);
        sub_wait(lock);
        if (exception_ != nullptr)
            rethrow_exception(exception_);
    }

    void wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        sub_wait(lock);
    }

    template <class Rep, class Period>
    future_status
    wait_for(const std::chrono::duration<Rep, Period>& rel_time) const {
        return wait_until(std::chrono::steady_clock::now() + rel_time);
    }

    template <class Clock, class Duration>
    future_status
    wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) const {
        std::unique_lock<std::mutex> lock(mutex_);
        while (!(state_ & ready) && Clock::now() < abs_time)
            cv_.wait_until(lock, abs_time);
        if (state_ & ready)
            return future_status::ready;
        return future_status::timeout;
    }

    virtual void execute() {
        throw future_error(make_error_code(future_errc::no_state));
    }
};

/******************************************************************************/
// assoc_state

template <typename Type>
class assoc_state : public assoc_sub_state
{
    using base = assoc_sub_state;
    using Up = typename std::aligned_storage<
              sizeof(Type), std::alignment_of<Type>::value>::type;

protected:
    Up value_;

    virtual void on_zero_shared() noexcept {
        if (this->state_ & base::constructed)
            reinterpret_cast<Type*>(&value_)->~Type();
        delete this;
    }

public:
    template <class Arg>
    void set_value(Arg&& arg) {
        std::unique_lock<std::mutex> lock(this->mutex_);
        if (this->has_value())
            throw_future_error(future_errc::promise_already_satisfied);
        ::new (&value_)Type(std::forward<Arg>(arg));
        this->state_ |= base::constructed | base::ready;
        cv_.notify_all();
    }

    Type move() {
        std::unique_lock<std::mutex> lock(this->mutex_);
        this->sub_wait(lock);
        if (this->exception_ != nullptr)
            rethrow_exception(this->exception_);
        return std::move(*reinterpret_cast<Type*>(&value_));
    }

    typename std::add_lvalue_reference<Type>::type copy() {
        std::unique_lock<std::mutex> lock(this->mutex_);
        this->sub_wait(lock);
        if (this->exception_ != nullptr)
            rethrow_exception(this->exception_);
        return *reinterpret_cast<Type*>(&value_);
    }
};

/******************************************************************************/
// assoc_state<R&>

template <typename Type>
class assoc_state<Type&>
    : public assoc_sub_state
{
    using base = assoc_sub_state;
    using Up = Type *;

protected:
    Up value_;

    virtual void on_zero_shared() noexcept {
        delete this;
    }

public:
    void set_value(Type& arg) {
        std::unique_lock<std::mutex> lock(this->mutex_);
        if (this->has_value())
            throw_future_error(future_errc::promise_already_satisfied);
        value_ = std::addressof(arg);
        this->state_ |= base::constructed | base::ready;
        cv_.notify_all();
    }

    Type& copy() {
        std::unique_lock<std::mutex> lock(this->mutex_);
        this->sub_wait(lock);
        if (this->exception_ != nullptr)
            rethrow_exception(this->exception_);
        return *value_;
    }
};

/******************************************************************************/
// assoc_state_alloc<R>, assoc_state_alloc<R&>

template <typename Type, class Alloc>
class assoc_state_alloc : public assoc_state<Type>
{
    using base = assoc_state<Type>;
    Alloc alloc_;

    virtual void on_zero_shared() noexcept {
        if (this->state_ & base::constructed)
            reinterpret_cast<Type*>(std::addressof(this->value_))->~Type();
        using Al = typename Alloc::template rebind<assoc_state_alloc>::other;
        using ATraits = std::allocator_traits<Al>;
        using PTraits = std::pointer_traits<typename ATraits::pointer>;
        Al a(alloc_);
        this->~assoc_state_alloc();
        a.deallocate(PTraits::pointer_to(*this), 1);
    }

public:
    explicit assoc_state_alloc(const Alloc& a)
        : alloc_(a) { }
};

template <typename Type, class Alloc>
class assoc_state_alloc<Type&, Alloc>: public assoc_state<Type&>
{
    using base = assoc_state<Type&>;
    Alloc alloc_;

    virtual void on_zero_shared() noexcept {
        using Al = typename Alloc::template rebind<assoc_state_alloc>::other;
        using ATraits = std::allocator_traits<Al>;
        using PTraits = std::pointer_traits<typename ATraits::pointer>;
        Al a(alloc_);
        this->~assoc_state_alloc();
        a.deallocate(PTraits::pointer_to(*this), 1);
    }

public:
    explicit assoc_state_alloc(const Alloc& a)
        : alloc_(a) { }
};

struct release_shared_count
{
    void operator () (shared_count* p) { p->release_shared(); }
};

/******************************************************************************/
// future<R>

template <typename Type>
class future
{
    assoc_state<Type>* state_;

    explicit future(assoc_state<Type>* state)
        : state_(state) {
        if (state_->has_future_attached())
            throw_future_error(future_errc::future_already_retrieved);
        state_->add_shared();
        state_->set_future_attached();
    }

    template <class>
    friend class promise;
    template <class>
    friend class shared_future;

public:
    future() noexcept : state_(nullptr) { }

    future(future&& rhs) noexcept
        : state_(rhs.state_) { rhs.state_ = nullptr; }
    future(const future&) = delete;
    future& operator = (const future&) = delete;

    future& operator = (future&& rhs) noexcept {
        future(std::move(rhs)).swap(*this); // NOLINT
        return *this;
    }
    ~future() {
        if (state_)
            state_->release_shared();
    }

    shared_future<Type> share();

    // retrieving the value
    Type get() {
        std::unique_ptr<shared_count, release_shared_count> x(state_);
        assoc_state<Type>* s = state_;
        state_ = nullptr;
        return s->move();
    }

    void swap(future& rhs) noexcept { std::swap(state_, rhs.state_); }

    // functions to check state

    bool valid() const noexcept { return state_ != nullptr; }

    void wait() const { state_->wait(); }

    template <class Rep, class Period>
    future_status
    wait_for(const std::chrono::duration<Rep, Period>& rel_time) const
    { return state_->wait_for(rel_time); }

    template <class Clock, class Duration>
    future_status
    wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) const
    { return state_->wait_until(abs_time); }
};

/******************************************************************************/
// future<R&>

template <typename Type>
class future<Type&>
{
    assoc_state<Type&>* state_;

    explicit future(assoc_state<Type&>* state)
        : state_(state) {
        if (state_->has_future_attached())
            throw_future_error(future_errc::future_already_retrieved);
        state_->add_shared();
        state_->set_future_attached();
    }

    template <class>
    friend class promise;
    template <class>
    friend class shared_future;

public:
    future() noexcept : state_(nullptr) { }

    future(future&& rhs) noexcept
        : state_(rhs.state_) { rhs.state_ = nullptr; }
    future(const future&) = delete;
    future& operator = (const future&) = delete;

    future& operator = (future&& rhs) noexcept {
        future(std::move(rhs)).swap(*this); // NOLINT
        return *this;
    }
    ~future() {
        if (state_)
            state_->release_shared();
    }

    shared_future<Type&> share();

    // retrieving the value
    Type& get() {
        std::unique_ptr<shared_count, release_shared_count>(state_);
        assoc_state<Type&>* s = state_;
        state_ = nullptr;
        return s->copy();
    }

    void swap(future& rhs) noexcept { std::swap(state_, rhs.state_); }

    // functions to check state

    bool valid() const noexcept { return state_ != nullptr; }

    void wait() const { state_->wait(); }

    template <class Rep, class Period>
    future_status
    wait_for(const std::chrono::duration<Rep, Period>& rel_time) const
    { return state_->wait_for(rel_time); }

    template <class Clock, class Duration>
    future_status
    wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) const
    { return state_->wait_until(abs_time); }
};

/******************************************************************************/
// future<void>

template <>
class future<void>
{
    assoc_sub_state* state_;

    explicit future(assoc_sub_state* state);

    template <class>
    friend class promise;
    template <class>
    friend class shared_future;

public:
    future() noexcept;

    future(future&& rhs) noexcept;
    future(const future&) = delete;
    future& operator = (const future&) = delete;

    future& operator = (future&& rhs) noexcept;
    ~future();

    shared_future<void> share();

    // retrieving the value
    void get();

    void swap(future& rhs) noexcept;

    // functions to check state

    bool valid() const noexcept;

    void wait() const;

    template <class Rep, class Period>
    future_status
    wait_for(const std::chrono::duration<Rep, Period>& rel_time) const
    { return state_->wait_for(rel_time); }

    template <class Clock, class Duration>
    future_status
    wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) const
    { return state_->wait_until(abs_time); }
};

template <typename Type>
inline void swap(future<Type>& x, future<Type>& y) noexcept {
    x.swap(y);
}

/******************************************************************************/
// promise<R>

template <typename Type>
class promise
{
    assoc_state<Type>* state_;

    explicit promise(std::nullptr_t) noexcept : state_(nullptr) { }

public:
    promise()
        : state_(new assoc_state<Type>)
    { }

    template <class Alloc>
    promise(std::allocator_arg_t, const Alloc& a0) {
        using State = assoc_state_alloc<Type, Alloc>;
        using A2 = typename Alloc::template rebind<State>::other;
        using D2 = allocator_destructor<A2>;
        A2 a(a0);
        std::unique_ptr<State, D2> hold(a.allocate(1), D2(a, 1));
        ::new (static_cast<void*>(std::addressof(*hold.get())))State(a0);
        state_ = std::addressof(*hold.release());
    }

    promise(promise&& rhs) noexcept
        : state_(rhs.state_) { rhs.state_ = nullptr; }
    promise(const promise& rhs) = delete;

    ~promise() {
        if (!state_) return;

        if (!state_->has_value() && state_->use_count() > 1) {
            state_->set_exception(
                make_exception_ptr(
                    future_error(make_error_code(future_errc::broken_promise))));
        }
        state_->release_shared();
    }

    // assignment

    promise& operator = (promise&& rhs) noexcept {
        promise(std::move(rhs)).swap(*this); // NOLINT
        return *this;
    }
    promise& operator = (const promise& rhs) = delete;

    void swap(promise& rhs) noexcept { std::swap(state_, rhs.state_); }

    // retrieving the result
    future<Type> get_future() {
        if (state_ == nullptr)
            throw_future_error(future_errc::no_state);
        return future<Type>(state_);
    }

    // setting the result
    void set_value(const Type& r) {
        if (state_ == nullptr)
            throw_future_error(future_errc::no_state);
        state_->set_value(r);
    }

    void set_value(Type&& r) {
        if (state_ == nullptr)
            throw_future_error(future_errc::no_state);
        state_->set_value(std::move(r));
    }

    void set_exception(std::exception_ptr p) {
        if (state_ == nullptr)
            throw_future_error(future_errc::no_state);
        state_->set_exception(p);
    }
};

/******************************************************************************/
// promise<R&>

template <typename Type>
class promise<Type&>
{
    assoc_state<Type&>* state_;

    explicit promise(std::nullptr_t) noexcept : state_(nullptr) { }

public:
    promise()
        : state_(new assoc_state<Type&>)
    { }

    template <class Alloc>
    promise(std::allocator_arg_t, const Alloc& a0) {
        using State = assoc_state_alloc<Type&, Alloc>;
        using A2 = typename Alloc::template rebind<State>::other;
        using D2 = allocator_destructor<A2>;
        A2 a(a0);
        std::unique_ptr<State, D2> hold(a.allocate(1), D2(a, 1));
        ::new (static_cast<void*>(std::addressof(*hold.get())))State(a0);
        state_ = std::addressof(*hold.release());
    }

    promise(promise&& rhs) noexcept
        : state_(rhs.state_) { rhs.state_ = nullptr; }
    promise(const promise& rhs) = delete;

    ~promise() {
        if (!state_) return;

        if (!state_->has_value() && state_->use_count() > 1) {
            state_->set_exception(
                make_exception_ptr(
                    future_error(make_error_code(future_errc::broken_promise))));
        }
        state_->release_shared();
    }

    // assignment

    promise& operator = (promise&& rhs) noexcept {
        promise(std::move(rhs)).swap(*this); // NOLINT
        return *this;
    }
    promise& operator = (const promise& rhs) = delete;

    void swap(promise& rhs) noexcept { std::swap(state_, rhs.state_); }

    // retrieving the result
    future<Type&> get_future() {
        if (state_ == nullptr)
            throw_future_error(future_errc::no_state);
        return future<Type&>(state_);
    }

    // setting the result
    void set_value(Type& r) {
        if (state_ == nullptr)
            throw_future_error(future_errc::no_state);
        state_->set_value(r);
    }
    void set_exception(std::exception_ptr p) {
        if (state_ == nullptr)
            throw_future_error(future_errc::no_state);
        state_->set_exception(p);
    }
};

/******************************************************************************/
// promise<void>

template <>
class promise<void>
{
    assoc_sub_state* state_;

    explicit promise(std::nullptr_t) noexcept;

public:
    promise();
    template <class Allocator>
    promise(std::allocator_arg_t, const Allocator& a);

    promise(promise&& rhs) noexcept;
    promise(const promise& rhs) = delete;
    ~promise();

    // assignment

    promise& operator = (promise&& rhs) noexcept;
    promise& operator = (const promise& rhs) = delete;

    void swap(promise& rhs) noexcept;

    // retrieving the result
    future<void> get_future();

    // setting the result
    void set_value();
    void set_exception(std::exception_ptr p);
};

template <typename Type>
inline void swap(promise<Type>& x, promise<Type>& y) noexcept {
    x.swap(y);
}

/******************************************************************************/
// shared_future<R>

template <typename Type>
class shared_future
{
    assoc_state<Type>* state_;

public:
    shared_future() noexcept : state_(nullptr) { }

    shared_future(const shared_future& rhs) : state_(rhs.state_)
    { if (state_) state_->add_shared(); }

    shared_future(future<Type>&& f) noexcept // NOLINT
        : state_(f.state_)
    { f.state_ = nullptr; }

    shared_future(shared_future&& rhs) noexcept : state_(rhs.state_)
    { rhs.state_ = nullptr; }

    ~shared_future() {
        if (state_)
            state_->release_shared();
    }

    shared_future& operator = (const shared_future& rhs) {
        if (rhs.state_)
            rhs.state_->add_shared();
        if (state_)
            state_->release_shared();
        state_ = rhs.state_;
        return *this;
    }

    shared_future& operator = (shared_future&& rhs) noexcept {
        shared_future(std::move(rhs)).swap(*this); // NOLINT
        return *this;
    }

    // retrieving the value

    const Type& get() const { return state_->copy(); }

    void swap(shared_future& rhs) noexcept { std::swap(state_, rhs.state_); }

    // functions to check state

    bool valid() const noexcept { return state_ != nullptr; }

    void wait() const { state_->wait(); }

    template <class Rep, class Period>
    future_status
    wait_for(const std::chrono::duration<Rep, Period>& rel_time) const
    { return state_->wait_for(rel_time); }

    template <class Clock, class Duration>
    future_status
    wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) const
    { return state_->wait_until(abs_time); }
};

/******************************************************************************/
// shared_future<R&>

template <typename Type>
class shared_future<Type&>
{
    assoc_state<Type&>* state_;

public:
    shared_future() noexcept : state_(nullptr) { }

    shared_future(const shared_future& rhs) : state_(rhs.state_)
    { if (state_) state_->add_shared(); }

    shared_future(future<Type&>&& f) noexcept // NOLINT
        : state_(f.state_)
    { f.state_ = nullptr; }

    shared_future(shared_future&& rhs) noexcept : state_(rhs.state_)
    { rhs.state_ = nullptr; }

    ~shared_future() {
        if (state_)
            state_->release_shared();
    }

    shared_future& operator = (const shared_future& rhs) {
        if (rhs.state_)
            rhs.state_->add_shared();
        if (state_)
            state_->release_shared();
        state_ = rhs.state_;
        return *this;
    }

    shared_future& operator = (shared_future&& rhs) noexcept {
        shared_future(std::move(rhs)).swap(*this); // NOLINT
        return *this;
    }

    // retrieving the value

    Type& get() const { return state_->copy(); }

    void swap(shared_future& rhs) noexcept { std::swap(state_, rhs.state_); }

    // functions to check state

    bool valid() const noexcept { return state_ != nullptr; }

    void wait() const { state_->wait(); }

    template <class Rep, class Period>
    future_status
    wait_for(const std::chrono::duration<Rep, Period>& rel_time) const
    { return state_->wait_for(rel_time); }

    template <class Clock, class Duration>
    future_status
    wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) const
    { return state_->wait_until(abs_time); }
};

/******************************************************************************/
// shared_future<void>

template <>
class shared_future<void>
{
    assoc_sub_state* state_;

public:
    shared_future() noexcept;

    shared_future(const shared_future& rhs);

    explicit shared_future(future<void>&& f) noexcept;

    shared_future(shared_future&& rhs) noexcept;

    ~shared_future();
    shared_future& operator = (const shared_future& rhs);

    shared_future& operator = (shared_future&& rhs) noexcept;

    // retrieving the value

    void get() const;

    void swap(shared_future& rhs) noexcept;

    // functions to check state

    bool valid() const noexcept;

    void wait() const;

    template <class Rep, class Period>
    future_status
    wait_for(const std::chrono::duration<Rep, Period>& rel_time) const
    { return state_->wait_for(rel_time); }

    template <class Clock, class Duration>
    future_status
    wait_until(const std::chrono::time_point<Clock, Duration>& abs_time) const
    { return state_->wait_until(abs_time); }
};

template <typename Type>
inline void swap(shared_future<Type>& x, shared_future<Type>& y) noexcept {
    x.swap(y);
}

template <typename Type>
inline shared_future<Type> future<Type>::share() {
    return shared_future<Type>(std::move(*this));
}

template <typename Type>
inline shared_future<Type&> future<Type&>::share() {
    return shared_future<Type&>(std::move(*this));
}

inline shared_future<void>
future<void>::share() {
    return shared_future<void>(std::move(*this));
}

} // namespace common
} // namespace thrill

namespace std {

template <>
struct is_error_code_enum<thrill::common::future_errc>: public true_type { };

} // namespace std

#endif // !THRILL_COMMON_FUTURE_HEADER

/******************************************************************************/
