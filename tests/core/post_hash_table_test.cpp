/*******************************************************************************
 * tests/core/post_hash_table_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <c7a/core/reduce_post_table.hpp>

#include "gtest/gtest.h"

using namespace c7a::data;
using namespace c7a::net;

struct PostTable : public::testing::Test {
    PostTable()
        : dispatcher("dispatcher"),
          manager(dispatcher),
          id(manager.AllocateDIA()),
          iterator(manager.GetIterator<int>(id)),
          pair_emit(manager.GetLocalEmitter<std::pair<std::string, int> >(id)) {
        emitters.emplace_back(manager.GetLocalEmitter<int>(id));
    }

    DispatcherThread                      dispatcher;
    Manager                               manager;
    ChainId                               id = manager.AllocateDIA();
    Iterator<int>                         iterator;
    Emitter<std::pair<std::string, int> > pair_emit; //both emitters access the same dia id, which is bad if you use them both
    std::vector<Emitter<int> >            emitters;

    size_t CountIteratorElements() {
        size_t result = 0;
        while (iterator.HasNext()) {
            result++;
            iterator.Next();
        }
        return result;
    }
};

std::pair<int, int> pair(int ele) {
    return std::make_pair(ele, ele);
}

TEST_F(PostTable, CustomHashFunction) {

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    using HashTable = typename c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn),
                                                          Emitter<int> >;

    auto hash_function = [](int key, HashTable*) {
                             return key / 2;
                         };

    HashTable table(8, 2, 20, 100, key_ex, red_fn, emitters, hash_function);

    for (int i = 0; i < 16; i++) {
        table.Insert(std::move(pair(i)));
    }

    table.Flush();

    //TODO:enable this assertion as soon as CountIteratorElements() counts iterator elements. -> the output size is LOG-tested though.
    //ASSERT_EQ(CountIteratorElements(), 16u);
}

TEST_F(PostTable, AddIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(key_ex, red_fn, emitters);

    table.Insert(pair(1));
    table.Insert(pair(2));
    table.Insert(pair(3));

    table.Print();

    ASSERT_EQ(3u, table.Size());

    table.Insert(pair(2));

    table.Print();

    ASSERT_EQ(3u, table.Size());
}

TEST_F(PostTable, CreateEmptyTable) {
    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(key_ex, red_fn, emitters);

    ASSERT_EQ(0u, table.Size());
}

TEST_F(PostTable, FlushIntegers) {
    auto key_ex = [](int in) {
                      return in;
                  };
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(key_ex, red_fn, emitters);

    table.Insert(pair(1));
    table.Insert(pair(2));
    table.Insert(pair(3));

    ASSERT_EQ(3u, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    table.Insert(pair(1));

    ASSERT_EQ(1u, table.Size());
}

TEST_F(PostTable, FlushIntegersInSequence) {
    auto key_ex = [](int in) {
                      return in;
                  };
    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), Emitter<int>, true>
    table(key_ex, red_fn, emitters);

    table.Insert(pair(1));
    table.Insert(pair(2));
    table.Insert(pair(3));

    ASSERT_EQ(3u, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    table.Insert(pair(1));

    ASSERT_EQ(1u, table.Size());
}

TEST_F(PostTable, DISABLED_MultipleEmitters) { //TODO(ts) enable when hash table flushes emitters
    std::vector<int> vec1;

    auto key_ex = [](int in) {
                      return in;
                  };

    auto red_fn = [](int in1, int in2) {
                      return in1 + in2;
                  };

    std::vector<Emitter<int> > emitters;
    emitters.emplace_back(manager.GetLocalEmitter<int>(id));
    emitters.emplace_back(manager.GetLocalEmitter<int>(id));
    emitters.emplace_back(manager.GetLocalEmitter<int>(id));

    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), Emitter<int> >
    table(key_ex, red_fn, emitters);

    table.Insert(pair(1));
    table.Insert(pair(2));
    table.Insert(pair(3));

    ASSERT_EQ(3u, table.Size());

    table.Flush();

    ASSERT_EQ(0u, table.Size());

    table.Insert(pair(1));

    ASSERT_EQ(1u, table.Size());

    ASSERT_EQ(9u, CountIteratorElements());
}

TEST_F(PostTable, ComplexType) {
    using StringPair = std::pair<std::string, int>;

    auto key_ex = [](StringPair in) {
                      return in.first;
                  };

    auto red_fn = [](StringPair in1, StringPair in2) {
                      return std::make_pair(in1.first, in1.second + in2.second);
                  };

    std::vector<Emitter<StringPair> > emitters;
    emitters.emplace_back(manager.GetLocalEmitter<StringPair>(id));
    c7a::core::ReducePostTable<decltype(key_ex), decltype(red_fn), Emitter<StringPair> >
    table(key_ex, red_fn, emitters);

    table.Insert(std::make_pair("hallo", std::make_pair("hallo", 1)));
    table.Insert(std::make_pair("hello", std::make_pair("hello", 2)));
    table.Insert(std::make_pair("bonjour", std::make_pair("bonjour", 3)));

    ASSERT_EQ(3u, table.Size());

    table.Insert(std::make_pair("hello", std::make_pair("hello", 5)));

    ASSERT_EQ(3u, table.Size());

    table.Insert(std::make_pair("baguette", std::make_pair("baguette", 42)));

    ASSERT_EQ(4u, table.Size());
}

// TODO(ms): add one test with a for loop inserting 10000 items. -> trigger

/******************************************************************************/
