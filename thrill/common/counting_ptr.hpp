/*******************************************************************************
 * thrill/common/counting_ptr.hpp
 *
 * An intrusive reference counting pointer which is much more light-weight than
 * std::shared_ptr.
 *
 * Borrowed of STXXL. See http://stxxl.sourceforge.net
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2010-2011 Raoul Steffen <R-Steffen@gmx.de>
 * Copyright (C) 2013-2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_COMMON_COUNTING_PTR_HEADER
#define THRILL_COMMON_COUNTING_PTR_HEADER

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>

namespace thrill {
namespace common {

/*!
 * High-performance smart pointer used as a wrapping reference counting pointer.
 *
 * This smart pointer class requires two functions in the template type: void
 * IncReference() and void DecReference(). These must increment and decrement a
 * reference count inside the templated object. When initialized, the type must
 * have reference count zero. Each new object referencing the data calls
 * IncReference() and each destroying holder calls del_reference(). When the
 * data object determines that it's internal count is zero, then it must destroy
 * itself.
 *
 * Accompanying the CountingPtr is a class ReferenceCount, from which reference
 * counted classes may be derive from. The class ReferenceCount implement all
 * methods required for reference counting.
 *
 * The whole method is more similar to boost's instrusive_ptr, but also yields
 * something resembling std::shared_ptr.
 */
template <class Type>
class CountingPtr
{
public:
    //! contained type.
    typedef Type element_type;

private:
    //! the pointer to the currently referenced object.
    Type* ptr_;

protected:
    //! increment reference count for current object.
    void IncReference()
    { IncReference(ptr_); }

    //! increment reference count of other object.
    void IncReference(Type* o)
    { if (o) o->IncReference(); }

    //! decrement reference count of current object and maybe delete it.
    void DecReference()
    { if (ptr_ && ptr_->DecReference()) delete ptr_; }

public:
    //! default constructor: contains a nullptr pointer.
    CountingPtr() : ptr_(nullptr)
    { }

    //! constructor with pointer: initializes new reference to ptr.
    CountingPtr(Type* ptr) : ptr_(ptr)
    { IncReference(); }

    //! copy-constructor: also initializes new reference to ptr.
    CountingPtr(const CountingPtr& other_ptr) : ptr_(other_ptr)
    { IncReference(); }

    //! assignment operator: dereference current object and acquire reference on
    //! new one.
    CountingPtr& operator = (const CountingPtr& other_ptr)
    { return operator = (other_ptr.ptr_); }

    //! assignment to pointer: dereference current and acquire reference to new
    //! ptr.
    CountingPtr& operator = (Type* ptr) {
        IncReference(ptr);
        DecReference();
        ptr_ = ptr;
        return *this;
    }

    //! destructor: decrements reference count in ptr.
    ~CountingPtr()
    { DecReference(); }

    //! return the enclosed object as reference.
    Type& operator * () const {
        assert(ptr_);
        return *ptr_;
    }

    //! return the enclosed pointer.
    Type* operator -> () const {
        assert(ptr_);
        return ptr_;
    }

    //! implicit cast to the enclosed pointer.
    operator Type* () const
    { return ptr_; }

    //! return the enclosed pointer.
    Type * get() const
    { return ptr_; }

    //! test equality of only the pointer values.
    bool operator == (const CountingPtr& other_ptr) const
    { return ptr_ == other_ptr.ptr_; }

    //! test inequality of only the pointer values.
    bool operator != (const CountingPtr& other_ptr) const
    { return ptr_ != other_ptr.ptr_; }

    //! cast to bool check for a nullptr pointer
    operator bool () const
    { return valid(); }

    //! test for a non-nullptr pointer
    bool valid() const
    { return (ptr_ != nullptr); }

    //! test for a nullptr pointer
    bool empty() const
    { return (ptr_ == nullptr); }

    //! if the object is referred by this CountingPtr only
    bool unique() const
    { return ptr_ && ptr_->unique(); }

    //! make and refer a copy if the original object was shared.
    void unify() {
        if (ptr_ && ! ptr_->unique())
            operator = (new Type(*ptr_));
    }

    //! swap enclosed object with another counting pointer (no reference counts
    //! need change)
    void swap(CountingPtr& b) {
        std::swap(ptr_, b.ptr_);
    }
};

//! swap enclosed object with another counting pointer (no reference counts need
//! change)
template <class A>
void swap(CountingPtr<A>& a1, CountingPtr<A>& a2) {
    a1.swap(a2);
}

/*!
 * Provides reference counting abilities for use with CountingPtr.
 *
 * Use as superclass of the actual object, this adds a reference_count
 * value. Then either use CountingPtr as pointer to manage references and
 * deletion, or just do normal new and delete.
 */
class ReferenceCount
{
private:
    //! the reference count is kept mutable for CountingPtr<const Type> to
    //! change the reference count.
    mutable std::atomic<size_t> reference_count_;

public:
    //! new objects have zero reference count
    ReferenceCount()
        : reference_count_(0) { }

    //! coping still creates a new object with zero reference count
    ReferenceCount(const ReferenceCount&)
        : reference_count_(0) { }

    //! assignment operator, leaves pointers unchanged
    ReferenceCount& operator = (const ReferenceCount&)
    { return *this; } // changing the contents leaves pointers unchanged

    ~ReferenceCount()
    { assert(reference_count_ == 0); }

public:
    //! Call whenever setting a pointer to the object
    void IncReference() const
    { ++reference_count_; }

    /*!
     * Call whenever resetting (i.e. overwriting) a pointer to the object.
     * IMPORTANT: In case of self-assignment, call AFTER IncReference().
     *
     * \return if the object has to be deleted (i.e. if it's reference count
     * dropped to zero)
     */
    bool DecReference() const
    { return (--reference_count_ == 0); }

    //! Test if the ReferenceCount is referenced by only one CountingPtr.
    bool unique() const
    { return (reference_count_ == 1); }

    //! Return the number of references to this object (for debugging)
    size_t reference_count() const
    { return reference_count_; }
};

} // namespace common
} // namespace thrill

#endif // !THRILL_COMMON_COUNTING_PTR_HEADER

/******************************************************************************/
