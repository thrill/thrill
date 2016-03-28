/*******************************************************************************
 * thrill/common/future.cpp
 *
 * std::promise and std::future implementations borrowed from libc++ under the
 * MIT license. Modified due to data race conditions detected by
 * ThreadSanitizer.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/future.hpp>

#include <algorithm>
#include <cstdlib>
#include <string>

namespace thrill {
namespace common {

class future_error_category : public std::error_category
{
public:
    virtual const char * name() const noexcept { return "future"; }
    virtual std::string message(int ev) const;
};

std::string
future_error_category::message(int ev) const {
    switch (static_cast<future_errc>(ev))
    {
    case future_errc::broken_promise:
        return std::string("The associated promise has been destructed prior "
                           "to the associated state becoming ready.");
    case future_errc::future_already_retrieved:
        return std::string("The future has already been retrieved from "
                           "the promise or packaged_task.");
    case future_errc::promise_already_satisfied:
        return std::string("The state of the promise has already been set.");
    case future_errc::no_state:
        return std::string("Operation not permitted on an object without "
                           "an associated state.");
    }
    return std::string("unspecified future_errc value\n");
}

const std::error_category& future_category() noexcept {
    static future_error_category f;
    return f;
}

void rethrow_exception(std::exception_ptr) {
    fprintf(stderr, "exception_ptr not yet implemented\n");
    ::abort();
}

/******************************************************************************/
// future<void>

future<void>::future(assoc_sub_state* state)
    : state_(state) {
    if (state_->has_future_attached())
        throw future_error(make_error_code(future_errc::future_already_retrieved));
    state_->add_shared();
    state_->set_future_attached();
}

future<void>::future() noexcept : state_(nullptr) { }

future<void>::future(future<void>&& rhs) noexcept
    : state_(rhs.state_) {
    rhs.state_ = nullptr;
}

future<void>& future<void>::operator = (future&& rhs) noexcept {
    future(std::move(rhs)).swap(*this);
    return *this;
}
future<void>::~future() {
    if (state_)
        state_->release_shared();
}

void future<void>::get() {
    std::unique_ptr<shared_count, release_shared_count> x(state_);
    assoc_sub_state* s = state_;
    state_ = nullptr;
    s->copy();
}

void future<void>::swap(future& rhs) noexcept {
    std::swap(state_, rhs.state_);
}

bool future<void>::valid() const noexcept {
    return state_ != nullptr;
}

void future<void>::wait() const {
    state_->wait();
}

/******************************************************************************/
// assoc_sub_state_alloc

template <class Alloc>
class assoc_sub_state_alloc : public assoc_sub_state
{
    using base = assoc_sub_state;
    Alloc alloc_;

    virtual void on_zero_shared() noexcept {
        using Al = typename Alloc::template rebind<assoc_sub_state_alloc>::other;
        using ATraits = std::allocator_traits<Al>;
        using PTraits = std::pointer_traits<typename ATraits::pointer>;
        Al a(alloc_);
        this->~assoc_sub_state_alloc();
        a.deallocate(PTraits::pointer_to(*this), 1);
    }

public:
    explicit assoc_sub_state_alloc(const Alloc& a)
        : alloc_(a) { }
};

/******************************************************************************/
// promise<void>

template <class Alloc>
promise<void>::promise(std::allocator_arg_t, const Alloc& a0) {
    using State = assoc_sub_state_alloc<Alloc>;
    using A2 = typename Alloc::template rebind<State>::other;
    using D2 = allocator_destructor<A2>;
    A2 a(a0);
    std::unique_ptr<State, D2> hold(a.allocate(1), D2(a, 1));
    ::new (static_cast<void*>(std::addressof(*hold.get())))State(a0);
    state_ = std::addressof(*hold.release());
}

promise<void>::promise(std::nullptr_t) noexcept
    : state_(nullptr) { }

promise<void>::promise()
    : state_(new assoc_sub_state) { }

promise<void>::promise(promise&& rhs) noexcept
    : state_(rhs.state_) {
    rhs.state_ = nullptr;
}

promise<void>& promise<void>::operator = (promise&& rhs) noexcept {
    promise(std::move(rhs)).swap(*this);
    return *this;
}

promise<void>::~promise() {
    if (!state_) return;

    if (!state_->has_value() && state_->use_count() > 1) {
        state_->set_exception(
            make_exception_ptr(
                future_error(make_error_code(future_errc::broken_promise))));
    }
    state_->release_shared();
}

void promise<void>::swap(promise& rhs) noexcept {
    std::swap(state_, rhs.state_);
}

future<void> promise<void>::get_future() {
    if (state_ == nullptr)
        throw future_error(make_error_code(future_errc::no_state));
    return future<void>(state_);
}

void promise<void>::set_value() {
    if (state_ == nullptr)
        throw future_error(make_error_code(future_errc::no_state));
    state_->set_value();
}

void promise<void>::set_exception(std::exception_ptr p) {
    if (state_ == nullptr)
        throw future_error(make_error_code(future_errc::no_state));
    state_->set_exception(p);
}

/******************************************************************************/
// shared_future<void>

shared_future<void>::shared_future() noexcept
    : state_(nullptr) { }

shared_future<void>::shared_future(const shared_future& rhs)
    : state_(rhs.state_) {
    if (state_) state_->add_shared();
}

shared_future<void>::shared_future(future<void>&& f) noexcept
    : state_(f.state_) {
    f.state_ = nullptr;
}

shared_future<void>::shared_future(shared_future&& rhs) noexcept
    : state_(rhs.state_) {
    rhs.state_ = nullptr;
}

shared_future<void>&
shared_future<void>::operator = (shared_future&& rhs) noexcept {
    shared_future(std::move(rhs)).swap(*this);
    return *this;
}

shared_future<void>::~shared_future() {
    if (state_)
        state_->release_shared();
}

shared_future<void>&
shared_future<void>::operator = (const shared_future& rhs) {
    if (rhs.state_)
        rhs.state_->add_shared();
    if (state_)
        state_->release_shared();
    state_ = rhs.state_;
    return *this;
}

void shared_future<void>::get() const {
    state_->copy();
}

void shared_future<void>::swap(shared_future& rhs) noexcept {
    std::swap(state_, rhs.state_);
}

bool shared_future<void>::valid() const noexcept {
    return state_ != nullptr;
}

void shared_future<void>::wait() const {
    state_->wait();
}

} // namespace common
} // namespace thrill

/******************************************************************************/
