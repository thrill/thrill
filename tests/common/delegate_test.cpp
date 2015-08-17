/*******************************************************************************
 * tests/common/delegate_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/common/delegate.hpp>
#include <gtest/gtest.h>

using namespace c7a::common;

using TestDelegate = delegate<int(int)>;

class A
{
public:
    int x;

    int func(int a) {
        return a + x;
    }

    int func2(int a) {
        return a + x + x;
    }

    int operator () (int a) {
        return a + 32;
    }
};

TEST(Delegate, TestClassFunction) {
    A a = { 2 };

    {
        TestDelegate d1 = TestDelegate::from(a, &A::func);
        ASSERT_EQ(d1(40), 42);

        TestDelegate d2 = TestDelegate(a, &A::func);
        ASSERT_EQ(d2(40), 42);

        TestDelegate d3 = TestDelegate::from(&a, &A::func);
        ASSERT_EQ(d3(40), 42);

        TestDelegate d4 = TestDelegate(&a, &A::func);
        ASSERT_EQ(d4(40), 42);
    }
    {
        TestDelegate d1 = TestDelegate::from(a);
        ASSERT_EQ(d1(10), 42);

        TestDelegate d2 = TestDelegate(a);
        ASSERT_EQ(d2(10), 42);
    }
}

class Ac
{
public:
    int x;

    int func(int a) const {
        return a + x;
    }

    int func2(int a) const {
        return a + x + x;
    }

    int operator () (int a) const {
        return a + 32;
    }
};

TEST(Delegate, TestClassConstFunction) {
    Ac a = { 2 };

    {
        TestDelegate d1 = TestDelegate(a, &Ac::func);
        ASSERT_EQ(d1(40), 42);

        TestDelegate d2 = TestDelegate::from(a, &Ac::func);
        ASSERT_EQ(d2(40), 42);

        TestDelegate d3 = TestDelegate(&a, &Ac::func);
        ASSERT_EQ(d3(40), 42);

        TestDelegate d4 = TestDelegate::from(&a, &Ac::func);
        ASSERT_EQ(d4(40), 42);
    }
    {
        TestDelegate d1 = TestDelegate(a);
        ASSERT_EQ(d1(10), 42);

        TestDelegate d2 = TestDelegate::from(a);
        ASSERT_EQ(d2(10), 42);
    }
}

int func1(int a) {
    return a + 5;
}

int func2(int a) {
    return a + 10;
}

TEST(Delegate, TestSimpleFunction) {
    TestDelegate d1 = TestDelegate(func1);
    ASSERT_EQ(d1(37), 42);

    TestDelegate d2 = TestDelegate::from(func1);
    ASSERT_EQ(d2(37), 42);

    d1 = TestDelegate::from(func2);
    ASSERT_EQ(d1(32), 42);

    d2 = TestDelegate(func2);
    ASSERT_EQ(d2(32), 42);

    d1 = TestDelegate::from<func1>();
    ASSERT_EQ(d1(37), 42);
}

TEST(Delegate, TestLambda) {
    TestDelegate d1 = TestDelegate([](int x) { return x + 1; });
    ASSERT_EQ(d1(42), 43);

    TestDelegate d2 = [](int x) { return x + 1; };
    ASSERT_EQ(d2(42), 43);
}

/******************************************************************************/
