/*******************************************************************************
 * tests/core/multiway_merge_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>

#include <thrill/core/iterator_wrapper.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/data/file.hpp>
#include <thrill/common/function_traits.hpp>

#include <thrill/common/logger.hpp>

#include <algorithm>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

using namespace thrill; // NOLINT


struct MultiwayMerge : public::testing::Test {
    data::BlockPool block_pool_ { nullptr };
};

TEST_F(MultiwayMerge, Basic) {
    // static const bool debug = false;
    std::mt19937 gen(0);
    std::size_t a = 2;
    std::size_t b = 5;
    std::size_t total = 2 * 5;

    using iterator = std::vector<int>::iterator;
    std::vector<std::vector<int> > in;
    std::vector<int> ref;
    std::vector<int> output;
    std::vector<std::pair<iterator, iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (std::size_t i = 0; i < a; ++i) {
        std::vector<int> tmp;
        tmp.reserve(b);
        for (std::size_t j = 0; j < b; ++j) {
            auto elem = gen() % 10;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(std::begin(tmp), std::end(tmp));
        in.push_back(tmp);
    }

    for (auto& vec : in) {
        seq.push_back(std::make_pair(std::begin(vec), std::end(vec)));
    }

    std::sort(std::begin(ref), std::end(ref));
    core::sequential_multiway_merge<true, false>(std::begin(seq),
                                                        std::end(seq),
                                                        std::begin(output),
                                                        total,
                                                        std::less<int>());
    for (std::size_t i = 0; i < total; ++i) {
        ASSERT_EQ(ref[i], output[i]);
    }
}

TEST_F(MultiwayMerge, VectorWrapper) {
    // static const bool debug = false;
    std::mt19937 gen(0);
    std::size_t a = 200;
    std::size_t b = 50;
    std::size_t total = 2 * 5;

    using iterator = thrill::core::VectorIteratorWrapper<int>;
    std::vector<std::vector<int> > in;
    std::vector<int> ref;
    std::vector<int> output;
    std::vector<std::pair<iterator, iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (std::size_t i = 0; i < a; ++i) {
        std::vector<int> tmp;
        tmp.reserve(b);
        for (std::size_t j = 0; j < b; ++j) {
            auto elem = gen();
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(std::begin(tmp), std::end(tmp));
        in.push_back(tmp);
    }

    for (auto& vec : in) {
        seq.push_back(std::make_pair(thrill::core::VectorIteratorWrapper<int>(&vec, 0),
                                     thrill::core::VectorIteratorWrapper<int>(&vec, b)));
    }

    std::sort(std::begin(ref), std::end(ref));
    core::sequential_multiway_merge<true, false>(std::begin(seq),
                                                        std::end(seq),
                                                        std::begin(output),
                                                        total,
                                                        std::less<int>());
    for (std::size_t i = 0; i < total; ++i) {
        ASSERT_EQ(ref[i], output[i]);
    }
}

TEST_F(MultiwayMerge, File_Wrapper_with_many_Runs) {
    static const bool debug = false;
    std::mt19937 gen(0);
    // get _a_ different runs with _a_ number of elements
    std::size_t a = 400;
    std::vector<std::size_t> b;
    b.reserve(a);

    for (std::size_t t = 0; t < a; ++t) {
        b.push_back(400 + gen() % 100);
    }

    std::size_t total = 0;
    for (auto t : b) {
        total += t;
    }

    using Iterator = thrill::core::FileIteratorWrapper<int>;
    using OIterator = thrill::core::FileOutputIteratorWrapper<int>;
    using File = data::File;
    using Reader = File::Reader;
    using Writer = File::Writer;
    std::vector<File> in;
    std::vector<int> ref;
    std::vector<int> output;
    std::vector<std::pair<Iterator, Iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (std::size_t i = 0; i < a; ++i) {
        std::vector<int> tmp;
        tmp.reserve(b[i]);
        for (std::size_t j = 0; j < b[i]; ++j) {
            auto elem = rand() % 100;
            sLOG << "FILE" << i << "with elem" << elem;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(std::begin(tmp), std::end(tmp));

        data::File f;
        {
            auto w = f.GetWriter();
            for (auto& t : tmp) {
                w(t);
            }
        }
        in.push_back(f);
    }

    for (std::size_t t = 0; t < in.size(); ++t) {
        auto reader = std::make_shared<Reader>(in[t].GetReader(true));
        Iterator s(&in[t], reader, 0, true);
        Iterator e(&in[t], reader, in[t].NumItems(), false);
        seq.push_back(std::make_pair(s, e));
    }

    data::File output_file;

    {
        OIterator oiter(std::make_shared<Writer>(output_file.GetWriter()));

        std::sort(std::begin(ref), std::end(ref));
        core::sequential_file_multiway_merge<true, false>(std::begin(seq),
                                                                 std::end(seq),
                                                                 oiter,
                                                                 total,
                                                                 std::less<int>());
    }

    auto r = output_file.GetReader(true);
    for (std::size_t i = 0; i < total; ++i) {
        auto e = r.Next<int>();
        sLOG << std::setw(3) << ref[i] << std::setw(3) << e;
        ASSERT_EQ(ref[i], e);
    }
}

TEST_F(MultiwayMerge, File_Wrapper_with_1_Runs) {
    static const bool debug = false;
    std::mt19937 gen(0);
    std::size_t a = 1;
    std::size_t b = 100;
    std::size_t total = a * b;

    using Iterator = thrill::core::FileIteratorWrapper<int>;
    using OIterator = thrill::core::FileOutputIteratorWrapper<int>;
    using File = data::File;
    using Reader = File::Reader;
    using Writer = File::Writer;
    std::vector<File> in;
    std::vector<int> ref;
    std::vector<int> output;
    std::vector<std::pair<Iterator, Iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (std::size_t i = 0; i < a; ++i) {
        std::vector<int> tmp;
        tmp.reserve(b);
        for (std::size_t j = 0; j < b; ++j) {
            auto elem = gen() % 100;
            sLOG << "FILE" << i << "with elem" << elem;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(std::begin(tmp), std::end(tmp));

        data::File f;
        {
            auto w = f.GetWriter();
            for (auto& t : tmp) {
                w(t);
            }
        }
        in.push_back(f);
    }

    for (std::size_t t = 0; t < in.size(); ++t) {
        auto reader = std::make_shared<Reader>(in[t].GetReader(true));
        Iterator s(&in[t], reader, 0, true);
        Iterator e(&in[t], reader, in[t].NumItems(), false);
        seq.push_back(std::make_pair(s, e));
    }

    data::File output_file;
    {
        OIterator oiter(std::make_shared<Writer>(output_file.GetWriter()));

        std::sort(std::begin(ref), std::end(ref));
        core::sequential_file_multiway_merge<true, false>(std::begin(seq),
                                                                 std::end(seq),
                                                                 oiter,
                                                                 total,
                                                                 std::less<int>());
    }

    auto r = output_file.GetReader(true);
    for (std::size_t i = 0; i < total; ++i) {
        auto e = r.Next<int>();
        sLOG << std::setw(3) << ref[i] << std::setw(3) << e;
        ASSERT_EQ(ref[i], e);
    }
}

TEST_F(MultiwayMerge, GetMultiwayMergePuller) {
    static const bool debug = false;
    std::mt19937 gen(0);
    std::size_t a = 4;
    std::size_t b = 3;
    std::size_t total = a * b;

    using Iterator = thrill::core::FileIteratorWrapper<int>;
    using File = data::File;
    using Reader = File::Reader;
    // using Writer = File::Writer;
    std::vector<File> in;
    std::vector<int> ref;
    std::vector<int> output;
    std::vector<std::pair<Iterator, Iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (std::size_t i = 0; i < a; ++i) {
        std::vector<int> tmp;
        tmp.reserve(b);
        for (std::size_t j = 0; j < b; ++j) {
            auto elem = gen() % 100;
            sLOG << "FILE" << i << "with elem" << elem;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(std::begin(tmp), std::end(tmp));

        data::File f(block_pool_);
        {
            auto w = f.GetWriter();
            for (auto& t : tmp) {
                w(t);
            }
        }
        in.push_back(f);
    }

    for (std::size_t t = 0; t < in.size(); ++t) {
        auto reader = std::make_shared<Reader>(in[t].GetReader(true));
        Iterator s(&in[t], reader, 0, true);
        Iterator e(&in[t], reader, in[t].num_items(), false);
        seq.push_back(std::make_pair(s, e));
    }

    auto puller = core::get_sequential_file_multiway_merge_tree<true, false>(
        std::begin(seq),
        std::end(seq),
        total,
        std::less<int>());

    std::sort(std::begin(ref), std::end(ref));

    for (std::size_t i = 0; i < total; ++i) {
        auto e = puller.Next();
        sLOG << std::setw(3) << ref[i] << std::setw(3) << e;
        ASSERT_EQ(ref[i], e);
    }
}

/******************************************************************************/
