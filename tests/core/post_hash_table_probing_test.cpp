/*******************************************************************************
 * tests/core/post_hash_table_probing_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/context.hpp>
#include <thrill/core/reduce_post_table.hpp>
#include <thrill/core/reduce_pre_stage.hpp>
#include <thrill/net/manager.hpp>

#include <algorithm>
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
class CustomKeyHashFunction : public core::ReduceByHashKey<int>
{
public:
    struct IndexResult {
    public:
        //! which partition number the item belongs to.
        size_t partition_id;
        //! index within the whole hashtable
        size_t global_index;

        IndexResult(size_t p_id, size_t g_id) {
            partition_id = p_id;
            global_index = g_id;
        }
    };

    explicit CustomKeyHashFunction(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    IndexResult
    operator () (const Key& k,
                 const size_t& num_frames,
                 const size_t& num_buckets_per_frame,
                 const size_t& num_buckets_per_table,
                 const size_t& offset) const {

        (void)num_frames;
        (void)num_buckets_per_frame;
        (void)num_buckets_per_table;
        (void)offset;

        return IndexResult(0, k / 2);
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
            std::vector<int> writer1;
            EmitterFunction emit = ([&writer1](const int value) {
                                        writer1.push_back(value);
                                    });

            CustomKeyHashFunction<int> cust_hash;
            core::PostReduceFlush<int, int, decltype(red_fn)> flush_func(red_fn);
            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                         core::PostReduceFlush<int, int, decltype(red_fn)>, CustomKeyHashFunction<int> >
            table(ctx, key_ex, red_fn, emit, cust_hash, flush_func, common::Range(0, 0), int(), 0, 1024 * 32);

            ASSERT_EQ(0u, writer1.size());
            ASSERT_EQ(0u, table.num_items());

            for (int i = 0; i < 16; i++) {
                table.Insert(std::move(pair(i)));
            }

            ASSERT_EQ(0u, writer1.size());
            ASSERT_EQ(16u, table.num_items());

            table.Flush(true);

            // ASSERT_EQ(16u, writer1.size());
            ASSERT_EQ(0u, table.num_items());
        };

    api::RunLocalSameThread(start_func);
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
            std::vector<int> writer1;
            EmitterFunction emit = ([&writer1](const int value) {
                                        writer1.push_back(value);
                                    });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emit);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(3u, table.num_items());

            table.Insert(pair(2));

            ASSERT_EQ(3u, table.num_items());

            table.Flush();

            ASSERT_EQ(3u, writer1.size());
        };

    api::RunLocalSameThread(start_func);
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
            std::vector<int> writer1;
            EmitterFunction emit = ([&writer1](const int value) {
                                        writer1.push_back(value);
                                    });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emit);

            ASSERT_EQ(0u, table.num_items());
        };

    api::RunLocalSameThread(start_func);
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
            std::vector<int> writer1;
            EmitterFunction emit = ([&writer1](const int value) {
                                        writer1.push_back(value);
                                    });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false>
            table(ctx, key_ex, red_fn, emit);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(3u, table.num_items());

            table.Flush(true);

            ASSERT_EQ(3u, writer1.size());
            ASSERT_EQ(0u, table.num_items());

            table.Insert(pair(1));

            ASSERT_EQ(1u, table.num_items());
        };

    api::RunLocalSameThread(start_func);
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
            std::vector<int> writer1;
            EmitterFunction emit = ([&writer1](const int value) {
                                        writer1.push_back(value);
                                    });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false>
            table(ctx, key_ex, red_fn, emit);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(3u, table.num_items());

            table.Flush(true);

            ASSERT_EQ(3u, writer1.size());
            ASSERT_EQ(0u, table.num_items());

            table.Insert(pair(1));

            ASSERT_EQ(1u, table.num_items());
        };

    api::RunLocalSameThread(start_func);
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
            std::vector<int> writer1;
            std::vector<int> writer2;
            EmitterFunction emit = ([&writer1, &writer2](const int value) {
                                        writer1.push_back(value);
                                        writer2.push_back(value);
                                    });

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false>
            table(ctx, key_ex, red_fn, emit);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(3u, table.num_items());

            table.Flush(true);

            ASSERT_EQ(0u, table.num_items());
            ASSERT_EQ(3u, writer1.size());
            ASSERT_EQ(3u, writer2.size());

            table.Insert(pair(1));

            ASSERT_EQ(1u, table.num_items());
        };

    api::RunLocalSameThread(start_func);
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
            std::vector<StringPair> writer1;
            EmitterFunction emit = ([&writer1](const StringPair& value) {
                                        writer1.push_back(value);
                                    });

            core::ReducePostProbingTable<StringPair, std::string, StringPair, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emit);

            table.Insert(std::make_pair("hallo", std::make_pair("hallo", 1)));
            table.Insert(std::make_pair("hello", std::make_pair("hello", 2)));
            table.Insert(std::make_pair("bonjour", std::make_pair("bonjour", 3)));

            ASSERT_EQ(3u, table.num_items());

            table.Insert(std::make_pair("hello", std::make_pair("hello", 5)));

            ASSERT_EQ(3u, table.num_items());

            table.Insert(std::make_pair("baguette", std::make_pair("baguette", 42)));

            ASSERT_EQ(4u, table.num_items());
        };

    api::RunLocalSameThread(start_func);
}

TEST_F(PostTable, DISABLED_WithinTableItemsLimit) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };
            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<int> writer1;
            EmitterFunction emit = ([&writer1](const int value) {
                                        writer1.push_back(value);
                                    });

            size_t byte_size = 1024 * 1024;
            size_t total_items = 32 * 1024;
            double fill_rate = 0.5;

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                         core::PostReduceFlush<int, int, decltype(red_fn)>,
                                         core::ReduceByHashKey<int>, std::equal_to<int> >
            table(ctx, key_ex, red_fn, emit, core::ReduceByHashKey<int>(),
                  core::PostReduceFlush<int, int, decltype(red_fn)>(red_fn), common::Range(0, 0), -1, 0, byte_size, fill_rate,
                  1);

            ASSERT_EQ(0u, table.num_items());

            size_t num_items = (size_t)(static_cast<double>(total_items) * fill_rate);

            for (size_t i = 0; i < num_items; ++i) {
                table.Insert(pair(static_cast<int>(i)));
            }
            ASSERT_EQ(num_items, table.num_items());

            ASSERT_EQ(0u, writer1.size());

            table.Flush(true);

            ASSERT_EQ(0u, table.num_items());
            ASSERT_EQ(num_items, writer1.size());
        };

    api::RunLocalSameThread(start_func);
}

TEST_F(PostTable, DISABLED_WithinTableItemsLimit2) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto key_ex = [](int in) {
                              return in;
                          };
            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<int> writer1;
            EmitterFunction emit = ([&writer1](const int value) {
                                        writer1.push_back(value);
                                    });

            size_t byte_size = 16 * 32 * 1024;
            size_t total_items = 16 * 1024;
            double fill_rate = 0.5;

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                         core::PostReduceFlush<int, int, decltype(red_fn)>,
                                         core::ReduceByHashKey<int>, std::equal_to<int> >
            table(ctx, key_ex, red_fn, emit, core::ReduceByHashKey<int>(),
                  core::PostReduceFlush<int, int, decltype(red_fn)>(red_fn), common::Range(0, 0), -1, 0, byte_size, fill_rate,
                  1);

            ASSERT_EQ(0u, table.num_items());

            size_t num_items = (size_t)(static_cast<double>(total_items) * fill_rate);

            for (size_t i = 0; i < num_items; ++i) {
                table.Insert(pair(static_cast<int>(i)));
            }

            ASSERT_EQ(num_items, table.num_items());

            ASSERT_EQ(0u, writer1.size());

            table.Flush(true);

            ASSERT_EQ(0u, table.num_items());
            ASSERT_EQ(num_items, writer1.size());
        };

    api::RunLocalSameThread(start_func);
}

TEST_F(PostTable, DISABLED_AboveTableItemsLimit) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            auto key_ex = [](int in) {
                              return in;
                          };
            auto red_fn = [](int in1, int in2) {
                              return in1 + in2;
                          };

            using EmitterFunction = std::function<void(const int&)>;
            std::vector<int> writer1;
            EmitterFunction emit = ([&writer1](const int value) {
                                        writer1.push_back(value);
                                    });

            size_t byte_size = 8 * 8 * 1024;
            size_t total_items = 1 * 4 * 1024;
            size_t on_top = 10;
            double fill_rate = 0.5;

            core::ReducePostProbingTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                         core::PostReduceFlush<int, int, decltype(red_fn)>,
                                         core::ReduceByHashKey<int>, std::equal_to<int> >
            table(ctx, key_ex, red_fn, emit,
                  core::ReduceByHashKey<int>(),
                  core::PostReduceFlush<int, int, decltype(red_fn)>(red_fn),
                  common::Range(0, 0), -1, 0, byte_size, fill_rate, 1);

            size_t num_items = (size_t)(static_cast<double>(total_items) * fill_rate);

            ASSERT_EQ(0u, table.num_items());

            for (size_t i = 0; i < num_items; ++i) {
                table.Insert(pair(static_cast<int>(i)));
            }

            ASSERT_TRUE(table.num_items() <= num_items);

            for (size_t i = num_items; i < num_items + on_top; ++i) {
                table.Insert(pair(static_cast<int>(i)));
            }

            ASSERT_TRUE(table.num_items() <= num_items + on_top);

            ASSERT_EQ(0u, writer1.size());

            table.Flush(true);

            ASSERT_EQ(num_items + on_top, writer1.size());
            ASSERT_EQ(0u, table.num_items());
        };

    api::RunLocalSameThread(start_func);
}

/******************************************************************************/
