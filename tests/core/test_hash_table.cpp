/*******************************************************************************
 * tests/core/test_hash_table.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_pre_table.hpp>
#include <c7a/core/reduce_post_table.hpp>
#include <tests/c7a_tests.hpp>
#include <c7a/api/context.hpp>

#include <functional>
#include <cstdio>
#include <utility>
#include <vector>
#include <string>

#include "gtest/gtest.h"

TEST(PreTable, CreateEmptyTable) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    ASSERT_EQ(0, table.Size());
}

TEST(PostTable, CreateEmptyTable) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(key_ex, red_fn, { emit });

    ASSERT_EQ(0, table.Size());
}

TEST(PreTable, AddIntegers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3, table.Size());

    table.Insert(2);

    ASSERT_EQ(3, table.Size());
}

TEST(PostTable, AddIntegers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    table.Print();

    ASSERT_EQ(3, table.Size());

    table.Insert(2);

    table.Print();

    ASSERT_EQ(3, table.Size());
}

TEST(PreTable, PopIntegers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.SetMaxSize(3);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);
    table.Insert(4);

    ASSERT_EQ(0, table.Size());

    table.Insert(1);

    ASSERT_EQ(1, table.Size());
}

TEST(PreTable, FlushIntegers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3, table.Size());

    table.Flush();

    ASSERT_EQ(0, table.Size());

    table.Insert(1);

    ASSERT_EQ(1, table.Size());
}

TEST(PostTable, FlushIntegers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(key_ex, red_fn, { emit });

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3, table.Size());

    table.Flush();

    ASSERT_EQ(0, table.Size());

    table.Insert(1);

    ASSERT_EQ(1, table.Size());
}

TEST(PostTable, MultipleEmitters) {
    std::vector<int> vec1;

    auto emit1 = [&vec1](int in) {
                     vec1.push_back(in);
                 };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    std::vector<decltype(emit1)> emitters;
    emitters.push_back(emit1);
    emitters.push_back(emit1);
    emitters.push_back(emit1);

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit1)>
    table(key_ex, red_fn, emitters);

    table.Insert(1);
    table.Insert(2);
    table.Insert(3);

    ASSERT_EQ(3, table.Size());

    table.Flush();

    ASSERT_EQ(0, table.Size());

    table.Insert(1);

    ASSERT_EQ(1, table.Size());

    ASSERT_EQ(9, vec1.size());
}

TEST(PreTable, ComplexType) {
    using StringPair = std::pair<std::string, double>;

    auto emit = [](StringPair in) {
                    std::cout << in.second << std::endl;
                };

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, 2, 2, 10, 3, key_ex, red_fn, { emit });

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("hello", 5));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("baguette", 42));

    ASSERT_EQ(0, table.Size());
}

TEST(PreTable, Resize) {
    using StringPair = std::pair<std::string, double>;

    auto emit = [](StringPair in) {
                    std::cout << in.second << std::endl;
                };

    auto key_ex = [](StringPair in) { return in.first; };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(1, 10, 2, 1, 10, key_ex, red_fn, { emit });

    ASSERT_EQ(10, table.NumBuckets());

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));

    ASSERT_EQ(10, table.NumBuckets());

    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(20, table.NumBuckets());
}

TEST(PostTable, ComplexType) {
    using StringPair = std::pair<std::string, double>;

    auto emit = [](StringPair in) {
                    std::cout << in.second << std::endl;
                };

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(key_ex, red_fn, { emit });

    table.Insert(std::make_pair("hallo", 1));
    table.Insert(std::make_pair("hello", 2));
    table.Insert(std::make_pair("bonjour", 3));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("hello", 5));

    ASSERT_EQ(3, table.Size());

    table.Insert(std::make_pair("baguette", 42));

    ASSERT_EQ(4, table.Size());
}

TEST(PreTable, MultipleWorkers) {
    auto emit = [](int in) {
                    std::cout << in << std::endl;
                };

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePreTable<decltype(key_ex), decltype(red_fn), decltype(emit)>
    table(2, key_ex, red_fn, { emit });

    ASSERT_EQ(0, table.Size());
    table.SetMaxSize(5);

    for (int i = 0; i < 6; i++) {
        table.Insert(i * 35001);
    }

    ASSERT_LE(table.Size(), 3);
    ASSERT_GT(table.Size(), 0);
}

// TODO(ms): add one test with a for loop inserting 10000 items. -> trigger

/******************************************************************************/
