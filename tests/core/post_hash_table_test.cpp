/*******************************************************************************
 * tests/core/post_hash_table_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/context.hpp>
#include <thrill/core/reduce_post_table.hpp>
#include <thrill/net/manager.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

struct PostTable : public::testing::Test { };

std::pair<int, int> pair(int ele) {
    return std::make_pair(ele, ele);
}

template <typename Key, typename HashFunction = std::hash<Key> >
class CustomKeyHashFunction
    : public core::PostReduceByHashKey<int>
{
public:
    explicit CustomKeyHashFunction(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePostTable>
    size_t
    operator () (const Key& v, ReducePostTable* ht, const size_t& size) const {

        (void)v;
        (void)ht;
        (void)size;

        return 0;
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
            core::PostReduceFlushToDefault<int, decltype(red_fn)> flush_func;
            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<int, decltype(red_fn)>, CustomKeyHashFunction<int> >
            table(ctx, key_ex, red_fn, emitters, cust_hash, flush_func);

            ASSERT_EQ(0u, writer1.size());

            for (int i = 0; i < 16; i++) {
                table.Insert(pair(i));
            }

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(16u, writer1.size());
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

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false>
            table(ctx, key_ex, red_fn, emitters);

            ASSERT_EQ(0u, table.NumBlocksPerTable());

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(0u, writer1.size());

            table.Flush(true);

            ASSERT_EQ(0u, table.NumBlocksPerTable());
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

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emitters);

            ASSERT_EQ(0u, table.NumBlocksPerTable());
            ASSERT_EQ(0u, table.NumItemsPerTable());
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

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emitters);

            ASSERT_EQ(0u, writer1.size());

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(3u, writer1.size());

            table.Insert(pair(1));
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

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false>
            table(ctx, key_ex, red_fn, emitters);

            ASSERT_EQ(0u, writer1.size());

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(0u, writer1.size());

            table.Flush(true);

            ASSERT_EQ(3u, writer1.size());

            table.Insert(pair(4));
            table.Insert(pair(5));
            table.Insert(pair(6));

            ASSERT_EQ(3u, writer1.size());

            table.Flush(true);

            ASSERT_EQ(6u, writer1.size());
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

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn)>
            table(ctx, key_ex, red_fn, emitters);

            table.Insert(pair(1));
            table.Insert(pair(2));
            table.Insert(pair(3));

            ASSERT_EQ(0u, writer1.size());
            ASSERT_EQ(0u, writer2.size());

            table.Flush();

            ASSERT_EQ(3u, writer1.size());
            ASSERT_EQ(3u, writer2.size());
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

            const size_t TargetBlockSize = 24 * 8;
            StringPair sp;

            core::ReducePostTable<StringPair, std::string, StringPair, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<std::string, decltype(red_fn)>,
                                  core::PostReduceByHashKey<std::string>, std::equal_to<std::string>, TargetBlockSize>
            table(ctx, key_ex, red_fn, emitters, core::PostReduceByHashKey<std::string>(),
                  core::PostReduceFlushToDefault<std::string, decltype(red_fn)>(), 0, 0, sp, 1024 * 24, 1.0, 0.5, 1.0,
                  std::equal_to<std::string>());

            table.Insert(std::make_pair("hallo", std::make_pair("hallo", 1)));
            table.Insert(std::make_pair("hello", std::make_pair("hello", 2)));
            table.Insert(std::make_pair("bonjour", std::make_pair("bonjour", 3)));

            ASSERT_EQ(3u, table.NumBlocksPerTable());

            table.Insert(std::make_pair("hello", std::make_pair("hello", 5)));

            ASSERT_EQ(3u, table.NumBlocksPerTable());

            table.Insert(std::make_pair("baguette", std::make_pair("baguette", 42)));

            ASSERT_EQ(4u, table.NumBlocksPerTable());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, OneBucketOneBlockTestFillRate) {

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

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePostTable<int, int, int,
                                                                          decltype(key_ex), decltype(red_fn), false,
                                                                          core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                                                          core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);
            using KeyValuePair = std::pair<int, int>;

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                  core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, key_ex, red_fn, emitters, core::PostReduceByHashKey<int>(),
                  core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0, bucket_block_size * 5, 0.2, 1.0, 1.0,
                  std::equal_to<int>());

            size_t block_size = common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));
            ASSERT_EQ(8u, block_size);

            ASSERT_EQ(0u, table.NumBlocksPerTable());
            ASSERT_EQ(0u, writer1.size());

            for (size_t i = 0; i < block_size; ++i) {
                table.Insert(i);
            }
            ASSERT_EQ(1u, table.NumBlocksPerTable());
            ASSERT_EQ(block_size, table.NumItemsPerTable());

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(block_size, writer1.size());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, OneBucketOneBlockTestFillRate2) {

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

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePostTable<int, int, int,
                                                                          decltype(key_ex), decltype(red_fn), false,
                                                                          core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                                                          core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);
            using KeyValuePair = std::pair<int, int>;

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                  core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, key_ex, red_fn, emitters, core::PostReduceByHashKey<int>(),
                  core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0, bucket_block_size * 5, 0.2, 0.5, 1.0,
                  std::equal_to<int>());

            size_t block_size = common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));
            ASSERT_EQ(8u, block_size);

            ASSERT_EQ(0u, table.NumBlocksPerTable());
            ASSERT_EQ(0u, writer1.size());

            for (size_t i = 0; i < block_size; ++i) {
                table.Insert(i);
            }
            ASSERT_EQ(1u, table.NumBlocksPerTable());
            ASSERT_EQ(block_size, table.NumItemsPerTable());

            for (size_t i = block_size; i < block_size * 2; ++i) {
                table.Insert(i);
            }
            ASSERT_EQ(2u, table.NumBlocksPerTable());
            ASSERT_EQ(block_size * 2, table.NumItemsPerTable());

            ASSERT_EQ(0u, writer1.size());
            table.Flush(true);
            ASSERT_EQ(0u, table.NumItemsPerTable());
            ASSERT_EQ(2 * block_size, writer1.size());
        };

    api::RunSameThread(start_func);
}

TEST_F(PostTable, OneBucketTwoBlocksTestFillRate) {

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

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePostTable<int, int, int,
                                                                          decltype(key_ex), decltype(red_fn), false,
                                                                          core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                                                          core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);

            using KeyValuePair = std::pair<int, int>;

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                  core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, key_ex, red_fn, emitters, core::PostReduceByHashKey<int>(),
                  core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0, bucket_block_size * 5, 0.2, 1.0, 1.0,
                  std::equal_to<int>());

            size_t block_size = common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));
            ASSERT_EQ(8u, block_size);

            ASSERT_EQ(0u, table.NumBlocksPerTable());

            for (size_t i = 0; i < block_size; ++i) {
                table.Insert(pair(i));
            }
            ASSERT_EQ(1u, table.NumBlocksPerTable());
            ASSERT_EQ(block_size, table.NumItemsPerTable());

            for (size_t i = block_size; i < block_size * 2; ++i) {
                table.Insert(pair(i));
            }
            ASSERT_EQ(2u, table.NumBlocksPerTable());
            ASSERT_EQ(block_size * 2, table.NumItemsPerTable());

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(block_size * 2, writer1.size());
        };
    api::RunSameThread(start_func);
}

TEST_F(PostTable, OneBucketTwoBlocksTestFillRate2) {

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

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePostTable<int, int, int,
                                                                          decltype(key_ex), decltype(red_fn), false,
                                                                          core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                                                          core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);
            using KeyValuePair = std::pair<int, int>;

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                  core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, key_ex, red_fn, emitters, core::PostReduceByHashKey<int>(),
                  core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0,
                  bucket_block_size * 5, 0.2, 0.5, 1.0,
                  std::equal_to<int>());

            size_t block_size = common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));
            ASSERT_EQ(8u, block_size);

            ASSERT_EQ(0u, table.NumBlocksPerTable());

            for (size_t i = 0; i < block_size; ++i) {
                table.Insert(pair(i));
            }
            ASSERT_EQ(1u, table.NumBlocksPerTable());
            ASSERT_EQ(block_size, table.NumItemsPerTable());

            for (size_t i = block_size; i < block_size * 2; ++i) {
                table.Insert(pair(i));
            }
            ASSERT_EQ(2u, table.NumBlocksPerTable());
            ASSERT_EQ(block_size * 2, table.NumItemsPerTable());

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(block_size * 2, writer1.size());
        };
    api::RunSameThread(start_func);
}

TEST_F(PostTable, TwoBucketsTwoBlocksTestFillRate) {

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

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePostTable<int, int, int,
                                                                          decltype(key_ex), decltype(red_fn), false,
                                                                          core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                                                          core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);
            using KeyValuePair = std::pair<int, int>;

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                  core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, key_ex, red_fn, emitters, core::PostReduceByHashKey<int>(),
                  core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0,
                  bucket_block_size * 5, 0.5, 1.0, 1.0,
                  std::equal_to<int>());

            size_t block_size = common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));
            ASSERT_EQ(8u, block_size);

            ASSERT_EQ(0u, table.NumBlocksPerTable());

            for (size_t i = 0; i < block_size; ++i) {
                table.Insert(pair(i));
            }
            ASSERT_EQ(2u, table.NumBlocksPerTable());

            for (size_t i = block_size; i < block_size * 2; ++i) {
                table.Insert(pair(i));
            }
            ASSERT_EQ(2u, table.NumBlocksPerTable());

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(block_size * 2, writer1.size());
        };
    api::RunSameThread(start_func);
}

TEST_F(PostTable, TwoBucketsTwoBlocksTestFillRate2) {

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

            const size_t TargetBlockSize = 8 * 8;
            const size_t bucket_block_size = sizeof(core::ReducePostTable<int, int, int,
                                                                          decltype(key_ex), decltype(red_fn), false,
                                                                          core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                                                          core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);
            using KeyValuePair = std::pair<int, int>;

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                  core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, key_ex, red_fn, emitters, core::PostReduceByHashKey<int>(),
                  core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0,
                  bucket_block_size * 5, 0.5, 0.5, 1.0,
                  std::equal_to<int>());

            size_t block_size = common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));
            ASSERT_EQ(8u, block_size);

            ASSERT_EQ(0u, table.NumBlocksPerTable());

            for (size_t i = 0; i < block_size; ++i) {
                table.Insert(pair(i));
            }
            ASSERT_EQ(2u, table.NumBlocksPerTable());

            for (size_t i = block_size; i < block_size * 2; ++i) {
                table.Insert(pair(i));
            }
            ASSERT_EQ(2u, table.NumBlocksPerTable());

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(block_size * 2, writer1.size());
        };
    api::RunSameThread(start_func);
}

TEST_F(PostTable, MaxTableBlocks) {

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

            const size_t TargetBlockSize = 8 * 1024;
            const size_t bucket_block_size = sizeof(core::ReducePostTable<int, int, int,
                    decltype(key_ex), decltype(red_fn), false,
                    core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                    core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>::BucketBlock);

            using KeyValuePair = std::pair<int, int>;
            size_t max_blocks = 8;

            core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                                  core::PostReduceFlushToDefault<int, decltype(red_fn)>,
                                  core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(ctx, key_ex, red_fn, emitters, core::PostReduceByHashKey<int>(),
                  core::PostReduceFlushToDefault<int, decltype(red_fn)>(),
                  0, 0, 0, bucket_block_size * max_blocks * 2, 0.5, 1.0, 0.1,
                  std::equal_to<int>());

            size_t block_size = common::max<size_t>(8, TargetBlockSize /
                                                    sizeof(KeyValuePair));

            size_t num_items = block_size * max_blocks;

            ASSERT_EQ(0u, table.NumBlocksPerTable());

            for (size_t i = 0; i < num_items; ++i) {
                table.Insert(pair(i));
                ASSERT_TRUE(table.NumBlocksPerTable() <= max_blocks * 2);
            }

            ASSERT_EQ(0u, writer1.size());

            table.Flush();

            ASSERT_EQ(num_items, writer1.size());
        };
    api::RunSameThread(start_func);
}

/******************************************************************************/
