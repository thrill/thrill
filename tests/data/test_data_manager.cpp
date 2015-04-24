#include "gtest/gtest.h"
#include "c7a/data/data_manager.cpp"

using namespace c7a::data;

TEST(SampleTest, AssertionTrue) {
    DataManager manager;
    manager.foo();
    ASSERT_EQ(1, 1);
}
