/*******************************************************************************
 * tests/common/counting_ptr_test.cpp
 *
 * Small test case for reference counting in stxxl::counting_ptr.
 *
 * Borrowed of STXXL. See http://stxxl.sourceforge.net
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2013 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/common/counting_ptr.hpp>

#include <gtest/gtest.h>

using namespace thrill; // NOLINT

static unsigned int count_deletes = 0;

// derive from counted_object to include reference counter
struct MyInteger : public common::ReferenceCount
{
    int i;

    explicit MyInteger(int _i) : i(_i) { }

    // count number of destructor calls
    ~MyInteger()
    { ++count_deletes; }
};

using IntegerPtr = common::CountingPtr<MyInteger>;
using IntegerCPtr = common::CountingPtr<const MyInteger>;

IntegerPtr MakeIntegerPtr() {
    return IntegerPtr(new MyInteger(24));
}

TEST(CountingPtr, RunTest) {
    {
        {
            // create object and pointer to it
            IntegerPtr i1 = IntegerPtr(new MyInteger(42));

            ASSERT_EQ(42, i1->i);
            ASSERT_EQ(42, (*i1).i);
            ASSERT_EQ(42, i1.get()->i);
            ASSERT_TRUE(i1->unique());

            // make pointer sharing same object
            IntegerPtr i2 = i1;

            ASSERT_EQ(42, i2->i);
            ASSERT_TRUE(!i1->unique());
            ASSERT_EQ(i2, i1);
            ASSERT_EQ(2, i1->reference_count());

            // make another pointer sharing the same object
            IntegerPtr i3 = i2;

            ASSERT_EQ(42, i3->i);
            ASSERT_EQ(3, i3->reference_count());

            // replace object in i3 with new integer
            i3 = new MyInteger(5);
            ASSERT_NE(i3, i1);
            ASSERT_EQ(2, i1->reference_count());
        }

        // check number of objects destructed
        ASSERT_EQ(2, count_deletes);

        // create a const pointer from a normal ptr.
        IntegerCPtr i4 = MakeIntegerPtr();

        // quitting the block will release the ptr
    }

    ASSERT_EQ(3, count_deletes);
}

/******************************************************************************/
