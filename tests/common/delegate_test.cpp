/*******************************************************************************
 * tests/common/delegate_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/common/delegate.hpp>
#include <c7a/mem/allocator_base.hpp>
#include <gtest/gtest.h>

using namespace c7a;
using common::delegate;

using TestDelegate = delegate<int(int), mem::BypassAllocator<void> >;

int func1(int a) {
    return a + 5;
}

int func2(int a) {
    return a + 10;
}

TEST(Delegate, TestSimpleFunction) {
    {
        // construction from a immediate function with no object or pointer.
        TestDelegate d = TestDelegate::from<func1>();
        ASSERT_EQ(42, d(37));
    }
    {
        // construction from a plain function pointer.
        TestDelegate d = TestDelegate(func1);
        ASSERT_EQ(42, d(37));
    }
    {
        // construction from a plain function pointer.
        TestDelegate d = TestDelegate::from(func1);
        ASSERT_EQ(42, d(37));
    }
}

class A
{
public:
    int x;

    int func(int a) {
        return a + x;
    }

    int const_func(int a) const {
        return a + x;
    }

    int func2(int a) {
        return a + x + x;
    }
};

TEST(Delegate, TestClassFunction) {
    A a = { 2 };
    {
        // construction for an immediate class::method with class object
        TestDelegate d = TestDelegate::from<A, & A::func>(&a);
        ASSERT_EQ(42, d(40));
    }
    {
        // construction for an immediate class::method with class object
        TestDelegate d = TestDelegate::from<A, & A::const_func>(&a);
        ASSERT_EQ(42, d(40));
    }
    {
        // construction for an immediate class::method with class object by
        // reference
        TestDelegate d = TestDelegate::from<A, & A::func>(a);
        ASSERT_EQ(42, d(40));
    }
    {
        // construction for an immediate class::method with class object by
        // reference
        TestDelegate d = TestDelegate::from<A, & A::const_func>(a);
        ASSERT_EQ(42, d(40));
    }

    {
        // constructor from an indirect class::method with object pointer.
        TestDelegate d = TestDelegate(&a, &A::func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object pointer.
        TestDelegate d = TestDelegate(&a, &A::const_func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object reference.
        TestDelegate d = TestDelegate(a, &A::func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object reference.
        TestDelegate d = TestDelegate(a, &A::const_func);
        ASSERT_EQ(42, d(40));
    }

    {
        // constructor from an indirect class::method with object pointer.
        TestDelegate d = TestDelegate::from(&a, &A::func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object pointer.
        TestDelegate d = TestDelegate::from(&a, &A::const_func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object reference.
        TestDelegate d = TestDelegate::from(a, &A::func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object reference.
        TestDelegate d = TestDelegate::from(a, &A::const_func);
        ASSERT_EQ(42, d(40));
    }

    {
        // constructor from an indirect class::method with object pointer.
        TestDelegate d = common::make_delegate(&a, &A::func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object pointer.
        TestDelegate d = common::make_delegate(&a, &A::const_func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object reference.
        TestDelegate d = common::make_delegate(a, &A::func);
        ASSERT_EQ(42, d(40));
    }
    {
        // constructor from an indirect class::method with object reference.
        TestDelegate d = common::make_delegate(a, &A::const_func);
        ASSERT_EQ(42, d(40));
    }
}

class Functor
{
public:
    int x;

    int operator () (int a) {
        return a + x;
    }
};

TEST(Delegate, TestFunctorClass) {
    Functor f = { 12 };

    {
        // calls general functor constructor
        TestDelegate d = TestDelegate(f);
        ASSERT_EQ(42, d(30));
    }
    {
        // calls general functor constructor
        TestDelegate d = TestDelegate::from(f);
        ASSERT_EQ(42, d(30));
    }
}

TEST(Delegate, TestLambda) {
    {
        TestDelegate d = TestDelegate([](int x) { return x + 1; });
        ASSERT_EQ(42, d(41));
    }
    {
        TestDelegate d = TestDelegate::from([](int x) { return x + 1; });
        ASSERT_EQ(42, d(41));
    }
    {
        // test a lambda with capture
        int val = 10;
        TestDelegate d = TestDelegate::from([&](int x) { return x + val; });
        ASSERT_EQ(42, d(32));
    }
    {
        TestDelegate d = [](int x) { return x + 1; };
        ASSERT_EQ(42, d(41));
    }
}

/******************************************************************************/
