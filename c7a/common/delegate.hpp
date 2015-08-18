/*******************************************************************************
 * c7a/common/delegate.hpp
 *
 * Replacement for std::function with some code borrowed from
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

template <typename T, typename Allocator = std::allocator<void> >
class delegate;

/*!
 * This is a faster replacement for std::function. Besides being faster, we use
 * it in places where move-only lambda captures are necessary. std::function is
 * required to be copy-constructible, and hence does not allow move-only
 * captures.
 */
template <class R, class ... A, typename Allocator>
class delegate<R(A ...), Allocator>
{
public:
    //! default constructor
    delegate() = default;

    //! copy constructor
    delegate(const delegate&) = default;

    //! move constructor
    delegate(delegate&&) = default;

    //! constructor of invalid delegate from nullptr
    delegate(const std::nullptr_t) noexcept { }

    //! copy assignment operator
    delegate& operator = (const delegate&) = default;

    //! move assignment operator
    delegate& operator = (delegate&&) = default;

    //! \name Immediate Function Calls
    //! \{

    //! construction from an immediate function with no object or pointer.
    template <R(* const Function)(A ...)>
    static delegate from() noexcept {
        return delegate(function_caller<Function>, nullptr);
    }

    //! \}

    //! \name Function Pointer Calls
    //! \{

    //! constructor from a plain function pointer with no object.
    explicit delegate(R(*const function_ptr)(A ...)) noexcept
        : delegate(function_ptr_caller,
                   reinterpret_cast<void*>(function_ptr)) { }

    //! construction from a plain function pointer with no object.
    static delegate from(R(*const function_ptr)(A ...)) noexcept {
        return delegate(function_ptr);
    }

    //! \}

    //! \name Immediate Class::Method Calls with Objects
    //! \{

    //! construction for an immediate class::method with class object
    template <class C, R(C::* const Method)(A ...)>
    static delegate from(C* const object_ptr) noexcept {
        return delegate(method_caller<C, Method>, object_ptr);
    }

    //! construction for an immediate class::method with class object
    template <class C, R(C::* const Method)(A ...) const>
    static delegate from(C const* const object_ptr) noexcept {
        return delegate(const_method_caller<C, Method>,
                        const_cast<C*>(object_ptr));
    }

    //! construction for an immediate class::method with class object by
    //! reference
    template <class C, R(C::* const Method)(A ...)>
    static delegate from(C& object) noexcept {
        return delegate(method_caller<C, Method>, &object);
    }

    //! construction for an immediate class::method with class object by
    //! reference
    template <class C, R(C::* const Method)(A ...) const>
    static delegate from(C const& object) noexcept {
        return delegate(const_method_caller<C, Method>,
                        const_cast<C*>(&object));
    }

    //! \}

    //! \name Lambdas with Captures and Wrapped Class::Method Calls with Objects
    //! \{

    //! constructor from any functor object T, which may be a lambda with
    //! capture or a member_pair or const_member_pair wrapper.
    template <
        typename T,
        typename = typename std::enable_if <
                   !std::is_same<delegate, typename std::decay<T>::type>{ }
        > ::type
        >
    delegate(T&& f)
        : store_(
              // allocate memory for T in shared_ptr with appropriate deleter
              typename Allocator::template rebind<
                  typename std::decay<T>::type>::other().allocate(1),
              store_deleter<typename std::decay<T>::type>),
          store_size_(sizeof(typename std::decay<T>::type)) {

        using Functor = typename std::decay<T>::type;
        using Rebind = typename Allocator::template rebind<Functor>::other;

        // copy-construct T into shared_ptr memory.
        Rebind().construct(
            static_cast<Functor*>(store_.get()), Functor(std::forward<T>(f)));

        object_ptr_ = store_.get();

        caller_ = functor_caller<Functor>;
        deleter_ = deleter_caller<Functor>;
    }

    //! constructor from any functor object T, which may be a lambda with
    //! capture or a member_pair or const_member_pair wrapper.
    template <typename T>
    static delegate from(T&& f) {
        return std::forward<T>(f);
    }

    //! constructor for wrapping a class::method with object pointer.
    template <class C>
    delegate(C* const object_ptr, R(C::* const method_ptr)(A ...))
        : delegate(member_pair<C>(object_ptr, method_ptr)) { }

    //! constructor for wrapping a const class::method with object pointer.
    template <class C>
    delegate(C* const object_ptr, R(C::* const method_ptr)(A ...) const)
        : delegate(const_member_pair<C>(object_ptr, method_ptr)) { }

    //! constructor for wrapping a class::method with object reference.
    template <class C>
    delegate(C& object, R(C::* const method_ptr)(A ...))
        : delegate(member_pair<C>(&object, method_ptr)) { }

    //! constructor for wrapping a const class::method with object reference.
    template <class C>
    delegate(C const& object, R(C::* const method_ptr)(A ...) const)
        : delegate(const_member_pair<C>(&object, method_ptr)) { }

    //! constructor for wrapping a class::method with object pointer.
    template <class C>
    static delegate from(C* const object_ptr,
                         R(C::* const method_ptr)(A ...)) {
        return member_pair<C>(object_ptr, method_ptr);
    }

    //! constructor for wrapping a const class::method with object pointer.
    template <class C>
    static delegate from(C const* const object_ptr,
                         R(C::* const method_ptr)(A ...) const) {
        return const_member_pair<C>(object_ptr, method_ptr);
    }

    //! constructor for wrapping a class::method with object reference.
    template <class C>
    static delegate from(C& object, R(C::* const method_ptr)(A ...)) {
        return member_pair<C>(&object, method_ptr);
    }

    //! constructor for wrapping a const class::method with object reference.
    template <class C>
    static delegate from(C const& object,
                         R(C::* const method_ptr)(A ...) const) {
        return const_member_pair<C>(&object, method_ptr);
    }

    //! \}

#if 0
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

    //! assignment operator to change the class::method to call, the object
    //! itself remains unchanged.
    template <class C>
    delegate& operator = (R(C::* const method_ptr)(A ...)) {
        return *this = delegate(static_cast<C*>(object_ptr_), method_ptr);
    }

    //! assignment operator to change the class::method to call, the object
    //! itself remains unchanged.
    template <class C>
    delegate& operator = (R(C::* const method_ptr)(A ...) const) {
        return *this = delegate(static_cast<C const*>(object_ptr_), method_ptr);
    }

    //! operator to change the functor via assignment?
    template <
        typename T,
        typename = typename std::enable_if <
                   !std::is_same<delegate, typename std::decay<T>::type>{ }
        > ::type
        >
    delegate& operator = (T&& f) {
        using Functor = typename std::decay<T>::type;
        using Rebind = typename Allocator::template rebind<Functor>::other;

        // if ((sizeof(Functor) > store_size_) || !store_.unique())
        // {
        store_.reset(Rebind().allocate(1), store_deleter<Functor>);

        //     store_size_ = sizeof(Functor);
        // }
        // else
        // {
        //     deleter_(store_.get());
        // }

        // copy-construct T into shared_ptr memory.
        Rebind().construct(
            static_cast<Functor*>(store_.get()), Functor(std::forward<T>(f)));

        object_ptr_ = store_.get();

        caller_ = functor_caller<Functor>;
        deleter_ = deleter_caller<Functor>;

        return *this;
    }
#endif

    //! \name Miscellaneous
    //! \{

    //! reset delegate to invalid.
    void reset() { caller_ = nullptr; store_.reset(); }

    void reset_caller() noexcept { caller_ = nullptr; }

    //! swap delegates
    void swap(delegate& other) noexcept { std::swap(*this, other); }

    //! compare delegate with another
    bool operator == (delegate const& rhs) const noexcept {
        return (object_ptr_ == rhs.object_ptr_) && (caller_ == rhs.caller_);
    }

    //! compare delegate with another
    bool operator != (delegate const& rhs) const noexcept {
        return !operator == (rhs);
    }

    //! compare delegate with another
    bool operator < (delegate const& rhs) const noexcept {
        return (object_ptr_ < rhs.object_ptr_) ||
               ((object_ptr_ == rhs.object_ptr_) && (caller_ < rhs.caller_));
    }

    //! compare delegate with another
    bool operator == (std::nullptr_t const) const noexcept {
        return !caller_;
    }

    //! compare delegate with another
    bool operator != (std::nullptr_t const) const noexcept {
        return caller_;
    }

    //! explicit conversion to bool -> valid or invalid.
    explicit operator bool () const noexcept { return caller_; }

    //! most important method: call. The call is forwarded to the selected
    //! function caller.
    R operator () (A ... args) const {
        assert(caller_);
        return caller_(object_ptr_, std::forward<A>(args) ...);
    }

    //! \}

private:
    //! type of the function caller pointer.
    using Caller = R (*)(void*, A&& ...);

    using Deleter = void (*)(void*);

    //! pointer to function caller which depends on the type in object_ptr_. The
    //! caller_ contains a plain pointer to either function_caller, a
    //! function_ptr_caller, a method_caller, a const_method_caller, or a
    //! functor_caller.
    Caller caller_ = nullptr;

    //! pointer to object held by the delegate: for plain function pointers it
    //! is the function pointer, for class::methods it is a pointer to the class
    //! instance, for functors it is a pointer to the shared_ptr store_
    //! contents.
    void* object_ptr_;

    Deleter deleter_ = nullptr;

    std::shared_ptr<void> store_;
    std::size_t store_size_ = 0;

    //! private constructor for plain
    delegate(const Caller& m, void* const o) noexcept
        : caller_(m), object_ptr_(o) { }

    //! deleter for stored functor closures
    template <class T>
    static void store_deleter(void* const p) {
        using Rebind = typename Allocator::template rebind<T>::other;

        Rebind().destroy(static_cast<T*>(p));
        Rebind().deallocate(static_cast<T*>(p), 1);
    }

    template <class T>
    static void deleter_caller(void* const p) {
        using Rebind = typename Allocator::template rebind<T>::other;

        Rebind().destroy(static_cast<T*>(p));
    }

    //! \name Callers for simple function and immediate class::method calls.
    //! \{

    //! caller for an immediate function with no object or pointer.
    template <R(* Function)(A ...)>
    static R function_caller(void* const, A&& ... args) {
        return Function(std::forward<A>(args) ...);
    }

    //! caller for a plain function pointer.
    static R function_ptr_caller(void* const object_ptr, A&& ... args) {
        return reinterpret_cast<R(* const)(A ...)>(object_ptr)(args ...);
    }

    //! function caller for immediate class::method function calls
    template <class C, R(C::* method_ptr)(A ...)>
    static R method_caller(void* const object_ptr, A&& ... args) {
        return (static_cast<C*>(object_ptr)->*method_ptr)(
            std::forward<A>(args) ...);
    }

    //! function caller for immediate const class::method functions calls.
    template <class C, R(C::* method_ptr)(A ...) const>
    static R const_method_caller(void* const object_ptr, A&& ... args) {
        return (static_cast<C const*>(object_ptr)->*method_ptr)(
            std::forward<A>(args) ...);
    }

    //! \}

    //! \name Wrappers for indirect class::method calls.
    //! \{

    //! wrappers for indirect class::method calls containing (object,
    //! method_ptr)
    template <class C>
    using member_pair =
              std::pair<C* const, R(C::* const)(A ...)>;

    //! wrappers for indirect const class::method calls containing (object,
    //! const method_ptr)
    template <class C>
    using const_member_pair =
              std::pair<C const* const, R(C::* const)(A ...) const>;

    //! template for class::function selector
    template <typename>
    struct is_member_pair : std::false_type { };

    //! specialization for class::function selector
    template <class C>
    struct is_member_pair<member_pair<C> >: std::true_type { };

    //! template for const class::function selector
    template <typename>
    struct is_const_member_pair : std::false_type { };

    //! specialization for const class::function selector
    template <class C>
    struct is_const_member_pair<const_member_pair<C> >: std::true_type { };

    //! function caller for functor class.
    template <typename T>
    static typename std::enable_if <
    !(is_member_pair<T>{ } || is_const_member_pair<T>{ }), R
    > ::type
    functor_caller(void* const object_ptr, A&& ... args) {
        return (*static_cast<T*>(object_ptr))(std::forward<A>(args) ...);
    }

    //! function caller for const functor class.
    template <typename T>
    static typename std::enable_if <
    (is_member_pair<T>{ } || is_const_member_pair<T>{ }), R
    > ::type
    functor_caller(void* const object_ptr, A&& ... args) {
        return (static_cast<T*>(object_ptr)->first->*
                static_cast<T*>(object_ptr)->second)(std::forward<A>(args) ...);
    }

    //! \}
};

} // namespace common
} // namespace c7a

#endif // !C7A_COMMON_DELEGATE_HEADER

/******************************************************************************/
