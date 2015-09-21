/*******************************************************************************
 * thrill/common/delegate.hpp
 *
 * Replacement for std::function with ideas and base code borrowed from
 * http://codereview.stackexchange.com/questions/14730/impossibly-fast-delegate
 *
 * Massively rewritten, commented, simplified, and improved.
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_DELEGATE_HEADER
#define THRILL_COMMON_DELEGATE_HEADER

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <memory>
#include <type_traits>
#include <utility>

namespace thrill {
namespace common {

template <typename T, typename Allocator = std::allocator<void> >
class delegate;

/*!
 * This is a faster replacement than std::function. Besides being faster and
 * doing less allocations when used correctly, we use it in places where
 * move-only lambda captures are necessary. std::function is required by the
 * standard to be copy-constructible, and hence does not allow move-only
 * lambda captures.
 *
 * A delegate contains a reference to any of the following callable objects:
 * - an immediate function (called via one indirection)
 * - a mutable function pointer (copied into the delegate)
 * - an immediate class::method call (called via one indirection)
 * - a functor object (the whole object is copied into the delegate)
 *
 * All callable objects must have the signature ReturnType(Arguments ...). If a
 * callable has this signature, it can be bound to the delegate.
 *
 * To implement all this the delegate contains one pointer to a "caller stub"
 * function, which depends on the contained object and can be an immediate
 * function call, a pointer to the object associated with the callable, and a
 * memory pointer (managed by shared_ptr) for holding larger callables that need
 * to be copied.
 *
 * A functor object can be a lambda function with its capture, an internally
 * wrapped mutable class::method class stored as pair<object, method_ptr>, or
 * any other old-school functor object.
 *
 * Delegates can be constructed similar to std::function.
\code
// in defining the delegate we decide the ReturnType(Arguments ...) signature
using MyDelegate = delegate<int(double)>;

// this is a plain function bound to the delegate as a function pointer
int func(double a) { return a + 10; }
MyDelegate d1 = MyDelegate(func);

class AClass {
public:
    int method(double d) { return d * d; }
};

AClass a;

// this is class::method bound to the delegate via indirection, warning: this
// creates a needless allocation, because it is stored as pair<Class,Method>
MyDelegate d2 = MyDelegate(a, &AClass::method);
// same as above
MyDelegate d3 = MyDelegate::from(a, &AClass::method);

// class::method bound to the delegate via instantiation of an immediate caller
// to the method AClass::method. this is preferred and does not require any
// memory allocation!
MyDelegate d4 = MyDelegate::from<AClass, &AClass::method>(a);

// a lambda with capture bound to the delegate, this always performs a memory
// allocation to copy the capture closure.
double offset = 42.0;
MyDelegate d5 = [&](double a) { return a + offset; };
\endcode
 *
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
              store_deleter<typename std::decay<T>::type>) {

        using Functor = typename std::decay<T>::type;
        using Rebind = typename Allocator::template rebind<Functor>::other;

        // copy-construct T into shared_ptr memory.
        Rebind().construct(
            static_cast<Functor*>(store_.get()), Functor(std::forward<T>(f)));

        object_ptr_ = store_.get();

        caller_ = functor_caller<Functor>;
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
    explicit operator bool () const noexcept { return caller_ != nullptr; }

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

    //! shared_ptr used to contain a memory object containing the callable, like
    //! lambdas with closures, or our own wrappers.
    std::shared_ptr<void> store_;

    //! private constructor for plain
    delegate(const Caller& m, void* const obj) noexcept
        : caller_(m), object_ptr_(obj) { }

    //! deleter for stored functor closures
    template <class T>
    static void store_deleter(void* const ptr) {
        using Rebind = typename Allocator::template rebind<T>::other;

        Rebind().destroy(static_cast<T*>(ptr));
        Rebind().deallocate(static_cast<T*>(ptr), 1);
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
    static typename std::enable_if<
        !(is_member_pair<T>::value || is_const_member_pair<T>::value), R
        >::type
    functor_caller(void* const object_ptr, A&& ... args) {
        return (*static_cast<T*>(object_ptr))(std::forward<A>(args) ...);
    }

    //! function caller for const functor class.
    template <typename T>
    static typename std::enable_if<
        (is_member_pair<T>::value || is_const_member_pair<T>::value), R
        >::type
    functor_caller(void* const object_ptr, A&& ... args) {
        return (static_cast<T*>(object_ptr)->first->*
                static_cast<T*>(object_ptr)->second)(std::forward<A>(args) ...);
    }

    //! \}
};

//! constructor for wrapping a class::method with object pointer.
template <class C, typename R, typename ... A>
delegate<R(A ...)>
make_delegate(
    C* const object_ptr, R(C::* const method_ptr)(A ...)) noexcept {
    return delegate<R(A ...)>::template from<C>(object_ptr, method_ptr);
}

//! constructor for wrapping a const class::method with object pointer.
template <class C, typename R, typename ... A>
delegate<R(A ...)>
make_delegate(
    C* const object_ptr, R(C::* const method_ptr)(A ...) const) noexcept {
    return delegate<R(A ...)>::template from<C>(object_ptr, method_ptr);
}

//! constructor for wrapping a class::method with object reference.
template <class C, typename R, typename ... A>
delegate<R(A ...)>
make_delegate(
    C& object_ptr, R(C::* const method_ptr)(A ...)) noexcept {
    return delegate<R(A ...)>::template from<C>(object_ptr, method_ptr);
}

//! constructor for wrapping a const class::method with object reference.
template <class C, typename R, typename ... A>
delegate<R(A ...)>
make_delegate(
    C const& object_ptr, R(C::* const method_ptr)(A ...) const) noexcept {
    return delegate<R(A ...)>::template from<C>(object_ptr, method_ptr);
}

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_DELEGATE_HEADER

/******************************************************************************/
