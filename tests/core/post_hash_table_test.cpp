/*******************************************************************************
 * tests/core/post_hash_table_test.cpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_post_table.hpp>
#include <gtest/gtest.h>

#include <c7a/net/manager.hpp>
#include <string>
#include <utility>
#include <vector>

using namespace c7a::data;
using namespace c7a::net;

struct PostTable : public::testing::Test { };

std::pair<int, int> pair(int ele) {
    return std::make_pair(ele, ele);
}

template <typename Key, typename HashFunction = std::hash<Key> >
class CustomKeyHashFunction
    : public c7a::core::PostReduceByHashKey<int>
{
public:
    CustomKeyHashFunction(const HashFunction& hash_function = HashFunction())
        : hash_function_(hash_function)
    { }

    template <typename ReducePostTable>
    size_t
    operator () (const Key& v, ReducePostTable* ht, const size_t& size) const {

        (*ht).NumBlocks();
        size_t i = size+1;

        return v / 2;
    }

private:
    HashFunction hash_function_;
};

TEST_F(PostTable, CustomHashFunction) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    CustomKeyHashFunction<int> cust_hash;
    c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)> flush_func;

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
                               c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>, CustomKeyHashFunction<int> >
    table(key_ex, red_fn, emitters, cust_hash, flush_func);

    ASSERT_EQ(0u, writer1.size());

    for (int i = 0; i < 16; i++) {
        table.Insert(std::move(pair(i)));
    }

    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(16u, writer1.size());
}

TEST_F(PostTable, AddIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn)>
    table(key_ex, red_fn, emitters);

    ASSERT_EQ(0u, table.NumBlocks());

    table.Insert(pair(1));
    table.Insert(pair(2));
    table.Insert(pair(3));

    ASSERT_EQ(3u, table.NumBlocks());
    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(0u, table.NumBlocks());
    ASSERT_EQ(3u, writer1.size());
}

TEST_F(PostTable, CreateEmptyTable) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn)>
    table(key_ex, red_fn, emitters);

    ASSERT_EQ(0u, table.NumBlocks());
}

TEST_F(PostTable, FlushIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn)>
    table(key_ex, red_fn, emitters);

    ASSERT_EQ(0u, table.NumBlocks());
    ASSERT_EQ(0u, writer1.size());

    table.Insert(pair(1));
    table.Insert(pair(2));
    table.Insert(pair(3));

    ASSERT_EQ(3u, table.NumBlocks());
    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(0u, table.NumBlocks());
    ASSERT_EQ(3u, writer1.size());

    table.Insert(pair(1));

    ASSERT_EQ(1u, table.NumBlocks());
}

TEST_F(PostTable, FlushIntegersInSequence) {
    auto key_ex = [](int in) {
                      return in;
                  };
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn)>
    table(key_ex, red_fn, emitters);

    ASSERT_EQ(0u, table.NumBlocks());
    ASSERT_EQ(0u, writer1.size());

    table.Insert(pair(1));
    table.Insert(pair(2));
    table.Insert(pair(3));

    ASSERT_EQ(3u, table.NumBlocks());
    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(0u, table.NumBlocks());
    ASSERT_EQ(3u, writer1.size());

    table.Insert(pair(4));
    table.Insert(pair(5));
    table.Insert(pair(6));

    ASSERT_EQ(3u, table.NumBlocks());
    ASSERT_EQ(3u, writer1.size());

    table.Flush();

    ASSERT_EQ(0u, table.NumBlocks());
    ASSERT_EQ(6u, writer1.size());
}

TEST_F(PostTable, MultipleEmitters) {
    std::vector<int> vec1;

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    std::vector<int> writer2;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });
    emitters.push_back([&writer2](const int value) { writer2.push_back(value); });

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn)>
    table(key_ex, red_fn, emitters);

    table.Insert(pair(1));
    table.Insert(pair(2));
    table.Insert(pair(3));

    ASSERT_EQ(0u, writer1.size());
    ASSERT_EQ(0u, writer2.size());

    table.Flush();

    ASSERT_EQ(3u, writer1.size());
    ASSERT_EQ(3u, writer2.size());
}

TEST_F(PostTable, ComplexType) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    typedef std::function<void (const StringPair&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<StringPair> writer1;
    emitters.push_back([&writer1](const StringPair value) { writer1.push_back(value); });

    c7a::core::ReducePostTable<StringPair, std::string, StringPair, decltype(key_ex), decltype(red_fn)>
    table(key_ex, red_fn, emitters);

    table.Insert(std::make_pair("hallo", std::make_pair("hallo", 1)));
    table.Insert(std::make_pair("hello", std::make_pair("hello", 2)));
    table.Insert(std::make_pair("bonjour", std::make_pair("bonjour", 3)));

    ASSERT_EQ(3u, table.NumBlocks());

    table.Insert(std::make_pair("hello", std::make_pair("hello", 5)));

    ASSERT_EQ(3u, table.NumBlocks());

    table.Insert(std::make_pair("baguette", std::make_pair("baguette", 42)));

    ASSERT_EQ(4u, table.NumBlocks());
}

TEST_F(PostTable, OneBucketOneBlock) {
    auto key_ex = [](int in) {
        return in;
    };
    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    const size_t TargetBlockSize = 16*1024;
    typedef std::pair<int, int> KeyValuePair;

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
            c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>,
            c7a::core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
    table(key_ex, red_fn, emitters, c7a::core::PostReduceByHashKey<int>(),
          c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0, 1, 0.5, 1, 1,
          std::equal_to<int>());

    size_t block_size = c7a::common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));

    ASSERT_EQ(0u, table.NumBlocks());

    for (size_t i = 0; i < block_size; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(1u, table.NumBlocks());
    }

    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(block_size, writer1.size());
}

TEST_F(PostTable, OneBucketOneBlockOverflow) {
    auto key_ex = [](int in) {
        return in;
    };
    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    const size_t TargetBlockSize = 16*1024;
    typedef std::pair<int, int> KeyValuePair;

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
            c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>,
            c7a::core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(key_ex, red_fn, emitters, c7a::core::PostReduceByHashKey<int>(),
                  c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0, 1, 0.5, 2, 1,
                  std::equal_to<int>());

    size_t block_size = c7a::common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));

    ASSERT_EQ(0u, table.NumBlocks());
    ASSERT_EQ(0u, writer1.size());

    for (size_t i = 0; i < block_size; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(1u, table.NumBlocks());
    }
    ASSERT_EQ(1u, table.NumBlocks());
    for (size_t i = block_size; i < block_size*2; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(1u, table.NumBlocks());
    }
    ASSERT_EQ(1u, table.NumBlocks());

    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(block_size*2, writer1.size());
}

TEST_F(PostTable, OneBucketTwoBlocks) {
    auto key_ex = [](int in) {
        return in;
    };
    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    const size_t TargetBlockSize = 16*1024;
    typedef std::pair<int, int> KeyValuePair;

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
            c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>,
            c7a::core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(key_ex, red_fn, emitters, c7a::core::PostReduceByHashKey<int>(),
                  c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0, 1, 0.5, 3, 1,
                  std::equal_to<int>());

    size_t block_size = c7a::common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));

    ASSERT_EQ(0u, table.NumBlocks());

    for (size_t i = 0; i < block_size; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(1u, table.NumBlocks());
    }
    ASSERT_EQ(1u, table.NumBlocks());

    for (size_t i = 0; i < block_size; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(1u, table.NumBlocks());
    }
    ASSERT_EQ(1u, table.NumBlocks());

    for (size_t i = block_size; i < block_size*2; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(2u, table.NumBlocks());
    }
    ASSERT_EQ(2u, table.NumBlocks());

    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(block_size*2, writer1.size());
}

TEST_F(PostTable, OneBucketTwoBlocksOverflow) {
    auto key_ex = [](int in) {
        return in;
    };
    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    const size_t TargetBlockSize = 16*1024;
    typedef std::pair<int, int> KeyValuePair;

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
            c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>,
            c7a::core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(key_ex, red_fn, emitters, c7a::core::PostReduceByHashKey<int>(),
                  c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>(), 0, 0, 0, 1, 0.5, 3, 1,
                  std::equal_to<int>());

    size_t block_size = c7a::common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));

    ASSERT_EQ(0u, table.NumBlocks());

    for (size_t i = 0; i < block_size; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(1u, table.NumBlocks());
    }
    ASSERT_EQ(1u, table.NumBlocks());

    for (size_t i = 0; i < block_size; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(1u, table.NumBlocks());
    }
    ASSERT_EQ(1u, table.NumBlocks());

    for (size_t i = block_size; i < block_size*2; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(2u, table.NumBlocks());
    }
    ASSERT_EQ(2u, table.NumBlocks());

    for (size_t i = block_size*2; i < block_size*3; ++i) {
        table.Insert(pair(i));
        ASSERT_EQ(2u, table.NumBlocks());
    }
    ASSERT_EQ(2u, table.NumBlocks());

    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(block_size*3, writer1.size());
}

TEST_F(PostTable, DISABLED_MaxTableBlocks) {
    auto key_ex = [](int in) {
        return in;
    };
    auto red_fn = [](int in1, int in2) {
        return in1 + in2;
    };

    typedef std::function<void (const int&)> EmitterFunction;
    std::vector<EmitterFunction> emitters;
    std::vector<int> writer1;
    emitters.push_back([&writer1](const int value) { writer1.push_back(value); });

    const size_t TargetBlockSize = 16*1024;
    typedef std::pair<int, int> KeyValuePair;
    size_t num_buckets = 64;
    size_t max_blocks = 128;

    c7a::core::ReducePostTable<int, int, int, decltype(key_ex), decltype(red_fn), false,
            c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>,
            c7a::core::PostReduceByHashKey<int>, std::equal_to<int>, TargetBlockSize>
            table(key_ex, red_fn, emitters, c7a::core::PostReduceByHashKey<int>(),
                  c7a::core::PostReduceFlushToDefault<int, decltype(red_fn)>(),
                  0, 0, 0, num_buckets, 0.5, max_blocks, 16,
                  std::equal_to<int>());

    size_t block_size = c7a::common::max<size_t>(8, TargetBlockSize / sizeof(KeyValuePair));

    size_t num_items = block_size * max_blocks + 1;

    ASSERT_EQ(0u, table.NumBlocks());

    for (size_t i = 0; i < num_items*2; ++i) {
        table.Insert(pair(i));
    }
    ASSERT_EQ(max_blocks, table.NumBlocks());

    ASSERT_EQ(0u, writer1.size());

    table.Flush();

    ASSERT_EQ(num_items*2, writer1.size());
}

/******************************************************************************/
