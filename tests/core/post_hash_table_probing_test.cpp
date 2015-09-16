/*******************************************************************************
 * tests/core/post_hash_table_probing_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/context.hpp>
#include <thrill/core/reduce_post_probing_table.hpp>
#include <thrill/net/manager.hpp>

#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;

struct PostTable : public::testing::Test { };

std::pair<int, int> pair(int ele) {
    return std::make_pair(ele, ele);
}

template <typename Key, typename HashFunction = std::hash<Key> >
class CustomKeyHashFunction
    : public core::PostProbingReduceByHashKey<int>
{
public:
    explicit CustomKeyHashFunction(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePostProbingTable>
    size_t
    operator () (const Key& v, ReducePostProbingTable* ht, const size_t& size) const {

        (void)ht;
        (void)size;

        return v / 2;
    }

private:
    HashFunction hash_function_;
};

TEST_F(PostTable, CustomHashFunction) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            emitters.push_back([&writer1](const int value) {
                                   writer1.push_back(value);
                               });

            CustomKeyHashFunction<int> cust_hash;
            core::PostProbingReduceFlushToDefault<int, decltype(red_fn), true> flush_func;
            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false, true,
                                         core::PostProbingReduceFlushToDefault<int, decltype(red_fn), true>, CustomKeyHashFunction<int> >
            table(ctx, key_ex, red_fn, emitters, -1, cust_hash, flush_func);

            ASSERT_EQ(0u, writer1.size());
            ASSERT_EQ(0u, table.NumItems());

            for (int i = 0; i < 16; i++) {
                table.Insert(std::move(pair(i)));
            }

            ASSERT_EQ(0u, writer1.size());
            ASSERT_EQ(16u, table.NumItems());

            table.Flush();

            ASSERT_EQ(16u, writer1.size());
            ASSERT_EQ(0u, table.NumItems());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, AddIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            emitters.push_back([&writer1](const int value) {
                                   writer1.push_back(value);
                               });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emitters, -1);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(3u, table.NumItems());

            table.Insert(pair(2));

            ASSERT_EQ(3u, table.NumItems());

            table.Flush();

            ASSERT_EQ(3u, writer1.size());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, CreateEmptyTable) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            emitters.push_back([&writer1](const int value) {
                                   writer1.push_back(value);
                               });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emitters, -1);

            ASSERT_EQ(0u, table.NumItems());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, FlushIntegers) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };
            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            emitters.push_back([&writer1](const int value) {
                                   writer1.push_back(value);
                               });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false, true>
            table(ctx, key_ex, red_fn, emitters, -1);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(3u, table.NumItems());

            table.Flush();

            ASSERT_EQ(3u, writer1.size());
            ASSERT_EQ(0u, table.NumItems());

            table.Insert(pair(1));

            ASSERT_EQ(1u, table.NumItems());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, FlushIntegersInSequence) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };
            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            emitters.push_back([&writer1](const int value) {
                                   writer1.push_back(value);
                               });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false, true>
            table(ctx, key_ex, red_fn, emitters, -1);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(3u, table.NumItems());

            table.Flush();

            ASSERT_EQ(3u, writer1.size());
            ASSERT_EQ(0u, table.NumItems());

            table.Insert(pair(1));

            ASSERT_EQ(1u, table.NumItems());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, MultipleEmitters) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            std::vector<int> vec1;

            auto key_ex = [](int in) {
                              return in;
                          };

            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            std::vector<int> writer2;
            emitters.push_back([&writer1](const int value) {
                                   writer1.push_back(value);
                               });
            emitters.push_back([&writer2](const int value) {
                                   writer2.push_back(value);
                               });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false, true>
            table(ctx, key_ex, red_fn, emitters, -1);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(3u, table.NumItems());

            table.Flush();

            ASSERT_EQ(0u, table.NumItems());
            ASSERT_EQ(3u, writer1.size());
            ASSERT_EQ(3u, writer2.size());

            table.Insert(pair(1));

            ASSERT_EQ(1u, table.NumItems());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, ComplexType) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            using StringPair = std::pair<std::string, int>;

            auto key_ex = [](StringPair in) {
                              return in.first;
                          };

            auto red_fn = [](StringPair in1, StringPair in2) {
                              return std::make_pair(in1.first, in1.second + in2.second);
                          };

            using EmitterFunction = std::function<void(const StringPair&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<StringPair> writer1;
            emitters.push_back([&writer1](const StringPair value) {
                                   writer1.push_back(value);
                               });

            core::ReducePostProbingTable<StringPair, std::string, StringPair, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emitters, "");

            table.Insert(std::make_pair("hallo", std::make_pair("hallo", 1)));
            table.Insert(std::make_pair("hello", std::make_pair("hello", 2)));
            table.Insert(std::make_pair("bonjour", std::make_pair("bonjour", 3)));

            ASSERT_EQ(3u, table.NumItems());

            table.Insert(std::make_pair("hello", std::make_pair("hello", 5)));

            ASSERT_EQ(3u, table.NumItems());

            table.Insert(std::make_pair("baguette", std::make_pair("baguette", 42)));

            ASSERT_EQ(4u, table.NumItems());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, WithinTableItemsLimit) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };
            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            emitters.push_back([&writer1](const int value) {
                                   writer1.push_back(value);
                               });

            size_t byte_size = 4 * 32 * 1024;
            size_t total_items = 32 * 1024;
            double fill_rate = 0.5;

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false, true,
                                         core::PostProbingReduceFlushToDefault<int, decltype(red_fn), true>,
                                         core::PostProbingReduceByHashKey<int>, std::equal_to<int> >
            table(ctx, key_ex, red_fn, emitters, -1, core::PostProbingReduceByHashKey<int>(),
                  core::PostProbingReduceFlushToDefault<int, decltype(red_fn), true>(), 0, 0, 0, byte_size, fill_rate,
                  1,
                  std::equal_to<int>());

            ASSERT_EQ(0u, table.NumItems());

            size_t num_items = (size_t)(static_cast<double>(total_items) * fill_rate);

            for (size_t i = 0; i < num_items; ++i) {
                table.Insert(pair(static_cast<int>(i)));
            }
            ASSERT_EQ(num_items, table.NumItems());

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(0u, table.NumItems());
            ASSERT_EQ(num_items, writer1.size());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, WithinTableItemsLimit2) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };
            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            emitters.push_back([&writer1](const int value) {
                                   writer1.push_back(value);
                               });

            size_t byte_size = 4 * 32 * 1024 - 1;
            size_t total_items = 32 * 1024;
            double fill_rate = 0.5;

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false, true,
                                         core::PostProbingReduceFlushToDefault<int, decltype(red_fn), true>,
                                         core::PostProbingReduceByHashKey<int>, std::equal_to<int> >
            table(ctx, key_ex, red_fn, emitters, -1, core::PostProbingReduceByHashKey<int>(),
                  core::PostProbingReduceFlushToDefault<int, decltype(red_fn), true>(), 0, 0, 0, byte_size, fill_rate,
                  1,
                  std::equal_to<int>());

            ASSERT_EQ(0u, table.NumItems());

            size_t num_items = (size_t)(static_cast<double>(total_items) * fill_rate);

            for (size_t i = 0; i < num_items; ++i) {
                table.Insert(pair(static_cast<int>(i)));
            }
            ASSERT_EQ(num_items - 1, table.NumItems());

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(0u, table.NumItems());
            ASSERT_EQ(num_items, writer1.size());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, AboveTableItemsLimit) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            auto key_ex = [](int in) {
                              return in;
                          };
            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<EmitterFunction> emitters;
            std::vector<int> writer1;
            emitters.push_back(
                [&writer1](const int value) {
                    writer1.push_back(value);
                });

            size_t byte_size = 4 * 32 * 1024;
            size_t total_items = 32 * 1024;
            double fill_rate = 0.5;

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false, true,
                                         core::PostProbingReduceFlushToDefault<int, decltype(red_fn), true>,
                                         core::PostProbingReduceByHashKey<int>, std::equal_to<int> >
            table(ctx, key_ex, red_fn, emitters, -1,
                  core::PostProbingReduceByHashKey<int>(),
                  core::PostProbingReduceFlushToDefault<int, decltype(red_fn), true>(),
                  0, 0, 0, byte_size, fill_rate, 1,
                  std::equal_to<int>());

            size_t num_items = (size_t)(static_cast<double>(total_items) * fill_rate);

            ASSERT_EQ(0u, table.NumItems());

            for (size_t i = 0; i < num_items; ++i) {
                table.Insert(pair(static_cast<int>(i)));
            }

            ASSERT_EQ(num_items, table.NumItems());

            size_t on_top = 10;

            for (size_t i = num_items; i < num_items + on_top; ++i) {
                table.Insert(pair(static_cast<int>(i)));
            }

            ASSERT_TRUE(table.NumItems() <= num_items);

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(num_items + on_top, writer1.size());
            ASSERT_EQ(0u, table.NumItems());
        };

    api::RunSameThread(start_func);
}

/******************************************************************************/
