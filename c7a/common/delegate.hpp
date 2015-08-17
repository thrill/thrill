/*******************************************************************************
 * c7a/common/delegate.hpp
 *
 * Replacement for std::function borrowed from
 * http://codereview.stackexchange.com/questions/14730/impossibly-fast-delegate-in-c11
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_COMMON_DELEGATE_HEADER
#define C7A_COMMON_DELEGATE_HEADER

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

namespace c7a {
namespace common {

template <typename T>
class delegate;

/*!
 * This is a faster replacement for std::function. Besides being faster, we use
 * it in places where move-only lambda captures are necessary. std::function is
 * required to be copy-constructible, and hence does not allow move-only
 * captures.
 */
template <class R, class ... A>
class delegate<R(A ...)>
{
    //! typedef of the function caller stub pointer.
    using stub_ptr_type = R (*)(void*, A&& ...);

    delegate(void* const o, const stub_ptr_type m) noexcept
        : object_ptr_(o), stub_ptr_(m) { }

public:
    //! default constructor
    delegate() = default;

    //! copy constructor
    delegate(const delegate&) = default;

    //! move constructor
    delegate(delegate&&) = default;

    //! constructor for nullptr
    delegate(const std::nullptr_t) noexcept : delegate() { }

    //! constructor for a class method C::operator() given by pointer
    template <class C, typename =
                  typename std::enable_if < std::is_class<C>{ } > ::type>
    explicit delegate(const C* const o) noexcept
        : object_ptr_(const_cast<C*>(o)) { }

    //! constructor for a class method C::operator() given by reference
    template <class C, typename =
                  typename std::enable_if < std::is_class<C>{ } > ::type>
    explicit delegate(const C& o) noexcept
        : object_ptr_(const_cast<C*>(&o)) { }

    //! constructor from a class method C::method with given pointer.
    template <class C>
    delegate(C* const object_ptr, R(C::* const method_ptr)(A ...))
        : delegate(member_pair<C>(object_ptr, method_ptr)) { }

    //! constructor from a const class method C::method with given pointer.
    template <class C>
    delegate(C* const object_ptr, R(C::* const method_ptr)(A ...) const)
        : delegate(const_member_pair<C>(object_ptr, method_ptr)) { }

    //! constructor from a class method C::method with given reference.
    template <class C>
    delegate(C& object, R(C::* const method_ptr)(A ...))
        : delegate(member_pair<C>(&object, method_ptr)) { }

    //! constructor from a const class method C::method with given reference.
    template <class C>
    delegate(C const& object, R(C::* const method_ptr)(A ...) const)
        : delegate(const_member_pair<C>(&object, method_ptr)) { }

    //! constructor from a functor object, which may be a lambda or a
    //! member_pair or const_member_pair wrapper.
    template <
        typename T,
        typename = typename std::enable_if <
                   !std::is_same<delegate, typename std::decay<T>::type>{ }
        > ::type
        >
    delegate(T&& f)
        : store_(operator new (sizeof(typename std::decay<T>::type)),
                 functor_deleter<typename std::decay<T>::type>),
          store_size_(sizeof(typename std::decay<T>::type)) {

        using functor_type = typename std::decay<T>::type;

        new (store_.get())functor_type(std::forward<T>(f));

        object_ptr_ = store_.get();

        stub_ptr_ = functor_stub<functor_type>;
        deleter_ = deleter_stub<functor_type>;
    }

    //! copy assignment operator
    delegate& operator = (const delegate&) = default;

    //! move assignment operator
    delegate& operator = (delegate&&) = default;

    //! operator to change the function to call via assignment?
    template <class C>
    delegate& operator = (R(C::* const rhs)(A ...)) {
        return *this = from(static_cast<C*>(object_ptr_), rhs);
    }

    //! operator to change the function to call via assignment?
    template <class C>
    delegate& operator = (R(C::* const rhs)(A ...) const) {
        return *this = from(static_cast<C const*>(object_ptr_), rhs);
    }

    //! operator to change the functor via assignment?
    template <
        typename T,
        typename = typename std::enable_if <
                   !std::is_same<delegate, typename std::decay<T>::type>{ }
        > ::type
        >
    delegate& operator = (T&& f) {
        using functor_type = typename std::decay<T>::type;

        if ((sizeof(functor_type) > store_size_) || !store_.unique())
        {
            store_.reset(operator new (sizeof(functor_type)),
                         functor_deleter<functor_type>);

            store_size_ = sizeof(functor_type);
        }
        else
        {
            deleter_(store_.get());
        }

        new (store_.get())functor_type(std::forward<T>(f));

        object_ptr_ = store_.get();

        stub_ptr_ = functor_stub<functor_type>;
        deleter_ = deleter_stub<functor_type>;

        return *this;
    }

    //! construction from a plain function
    template <R(* const function_ptr)(A ...)>
    static delegate from() noexcept {
        // calls protected constructor.
        return delegate(nullptr, function_stub<function_ptr>);
    }

    //! construction for a class method C::operator() given by pointer
    template <class C, R(C::* const method_ptr)(A ...)>
    static delegate from(C* const object_ptr) noexcept {
        return { object_ptr, method_stub<C, method_ptr>};
    }

    //! construction for a const class method C::operator() given by pointer
    template <class C, R(C::* const method_ptr)(A ...) const>
    static delegate from(C const* const object_ptr) noexcept {
        return { const_cast<C*>(object_ptr), const_method_stub<C, method_ptr>};
    }

    //! construction for a class method C::operator() given by reference
    template <class C, R(C::* const method_ptr)(A ...)>
    static delegate from(C& object) noexcept {
        return { &object, method_stub<C, method_ptr>};
    }

    //! construction for a const class method C::operator() given by reference
    template <class C, R(C::* const method_ptr)(A ...) const>
    static delegate from(C const& object) noexcept {
        return { const_cast<C*>(&object), const_method_stub<C, method_ptr>};
    }

    //! construction from a plain function
    template <typename T>
    static delegate from(T&& f) {
        return std::forward<T>(f);
    }

    static delegate from(R(*const function_ptr)(A ...)) {
        return function_ptr;
    }

    template <class C>
    using member_pair =
              std::pair<C* const, R(C::* const)(A ...)>;

    template <class C>
    using const_member_pair =
              std::pair<C const* const, R(C::* const)(A ...) const>;

    //! construction from a class method C::method with given pointer.
    template <class C>
    static delegate from(C* const object_ptr,
                         R(C::* const method_ptr)(A ...)) {
        return member_pair<C>(object_ptr, method_ptr);
    }

    //! construction from a const class method C::method with given pointer.
    template <class C>
    static delegate from(C const* const object_ptr,
                         R(C::* const method_ptr)(A ...) const) {
        return const_member_pair<C>(object_ptr, method_ptr);
    }

    //! construction from a class method C::method with given reference.
    template <class C>
    static delegate from(C& object, R(C::* const method_ptr)(A ...)) {
        return member_pair<C>(&object, method_ptr);
    }

    //! construction from a const class method C::method with given reference.
    template <class C>
    static delegate from(C const& object,
                         R(C::* const method_ptr)(A ...) const) {
        return const_member_pair<C>(&object, method_ptr);
    }

    //! reset delegate to invalid.
    void reset() { stub_ptr_ = nullptr; store_.reset(); }

    void reset_stub() noexcept { stub_ptr_ = nullptr; }

    //! swap delegates
    void swap(delegate& other) noexcept { std::swap(*this, other); }

    //! compare delegate with another
    bool operator == (delegate const& rhs) const noexcept {
        return (object_ptr_ == rhs.object_ptr_) && (stub_ptr_ == rhs.stub_ptr_);
    }

    //! compare delegate with another
    bool operator != (delegate const& rhs) const noexcept {
        return !operator == (rhs);
    }

    //! compare delegate with another
    bool operator < (delegate const& rhs) const noexcept {
        return (object_ptr_ < rhs.object_ptr_) ||
               ((object_ptr_ == rhs.object_ptr_) && (stub_ptr_ < rhs.stub_ptr_));
    }

    //! compare delegate with another
    bool operator == (std::nullptr_t const) const noexcept {
        return !stub_ptr_;
    }

    //! compare delegate with another
    bool operator != (std::nullptr_t const) const noexcept {
        return stub_ptr_;
    }

    //! explicit conversion to bool -> valid or invalid.
    explicit operator bool () const noexcept { return stub_ptr_; }

    //! most important: caller method, the call is forwarded to constructed
    //! function caller stub.
    R operator () (A ... args) const {
        assert(stub_ptr_);
        return stub_ptr_(object_ptr_, std::forward<A>(args) ...);
    }

private:
    using deleter_type = void (*)(void*);

    //! pointer to function held by the delegate: either a plain function or a
    //! class method.
    void* object_ptr_;

    //! function caller stub which depends on the type of function in
    //! object_ptr_.
    stub_ptr_type stub_ptr_ { };

    deleter_type deleter_ = nullptr;

    std::shared_ptr<void> store_ = nullptr;
    std::size_t store_size_ = 0;

    //! deleter for functor closures
    template <class T>
    static void functor_deleter(void* const p) {
        static_cast<T*>(p)->~T();

        operator delete (p);
    }

    template <class T>
    static void deleter_stub(void* const p) {
        static_cast<T*>(p)->~T();
    }

    //! function caller stub for plain functions.
    template <R(* function_ptr)(A ...)>
    static R function_stub(void* const, A&& ... args) {
        return function_ptr(std::forward<A>(args) ...);
    }

    //! function caller stub for class::method functions.
    template <class C, R(C::* method_ptr)(A ...)>
    static R method_stub(void* const object_ptr, A&& ... args) {
        return (static_cast<C*>(object_ptr)->*method_ptr)(
            std::forward<A>(args) ...);
    }

    //! function caller stub for const class::method functions.
    template <class C, R(C::* method_ptr)(A ...) const>
    static R const_method_stub(void* const object_ptr, A&& ... args) {
        return (static_cast<C const*>(object_ptr)->*method_ptr)(
            std::forward<A>(args) ...);
    }

    //! template for class::function selector
    template <typename>
    struct is_member_pair : std::false_type { };

    //! specialization for class::function selector
    template <class C>
    struct is_member_pair<
        std::pair<C* const, R(C::* const)(A ...)> >
        : std::true_type
    { };

    //! template for const class::function selector
    template <typename>
    struct is_const_member_pair : std::false_type { };

    //! specialization for const class::function selector
    template <class C>
    struct is_const_member_pair<
        std::pair<C const* const, R(C::* const)(A ...) const> >
        : std::true_type
    { };

    //! function caller stub for functor class.
    template <typename T>
    static typename std::enable_if <
    !(is_member_pair<T>{ } || is_const_member_pair<T>{ }), R
    > ::type
    functor_stub(void* const object_ptr, A&& ... args) {
        return (*static_cast<T*>(object_ptr))(std::forward<A>(args) ...);
    }

    //! function caller stub for const functor class.
    template <typename T>
    static typename std::enable_if <
    (is_member_pair<T>{ } || is_const_member_pair<T>{ }), R
    > ::type
    functor_stub(void* const object_ptr, A&& ... args) {
        return (static_cast<T*>(object_ptr)->first->*
                static_cast<T*>(object_ptr)->second)(std::forward<A>(args) ...);
    }
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_DELEGATE_HEADER

/******************************************************************************/
