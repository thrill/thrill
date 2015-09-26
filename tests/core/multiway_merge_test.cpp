/*******************************************************************************
 * tests/core/multiway_merge_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>

#include <thrill/common/function_traits.hpp>
#include <thrill/core/iterator_wrapper.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/data/file.hpp>

#include <thrill/common/logger.hpp>

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace thrill; // NOLINT

struct MultiwayMerge : public::testing::Test {
    data::BlockPool block_pool_ { nullptr };
};

TEST_F(MultiwayMerge, Basic) {
    // static const bool debug = false;
    std::mt19937 gen(0);
    size_t a = 2;
    size_t b = 5;
    size_t total = 2 * 5;

    using iterator = std::vector<size_t>::iterator;
    std::vector<std::vector<size_t> > in;
    std::vector<size_t> ref;
    std::vector<size_t> output;
    std::vector<std::pair<iterator, iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (size_t i = 0; i < a; ++i) {
        std::vector<size_t> tmp;
        tmp.reserve(b);
        for (size_t j = 0; j < b; ++j) {
            auto elem = gen() % 10;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(tmp.begin(), tmp.end());
        in.push_back(tmp);
    }

    for (auto& vec : in) {
        seq.push_back(std::make_pair(vec.begin(), vec.end()));
    }

    std::sort(ref.begin(), ref.end());
    core::sequential_multiway_merge<true, false>(seq.begin(),
                                                 seq.end(),
                                                 output.begin(),
                                                 total,
                                                 std::less<size_t>());
    for (size_t i = 0; i < total; ++i) {
        ASSERT_EQ(ref[i], output[i]);
    }
}

TEST_F(MultiwayMerge, VectorWrapper) {
    // static const bool debug = false;
    std::mt19937 gen(0);
    size_t a = 200;
    size_t b = 50;
    size_t total = 2 * 5;

    using iterator = thrill::core::VectorIteratorWrapper<size_t>;
    std::vector<std::vector<size_t> > in;
    std::vector<size_t> ref;
    std::vector<size_t> output;
    std::vector<std::pair<iterator, iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (size_t i = 0; i < a; ++i) {
        std::vector<size_t> tmp;
        tmp.reserve(b);
        for (size_t j = 0; j < b; ++j) {
            auto elem = gen();
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(tmp.begin(), tmp.end());
        in.push_back(tmp);
    }

    for (auto& vec : in) {
        seq.push_back(std::make_pair(thrill::core::VectorIteratorWrapper<size_t>(&vec, 0),
                                     thrill::core::VectorIteratorWrapper<size_t>(&vec, b)));
    }

    std::sort(ref.begin(), ref.end());
    core::sequential_multiway_merge<true, false>(seq.begin(),
                                                 seq.end(),
                                                 output.begin(),
                                                 total,
                                                 std::less<size_t>());
    for (size_t i = 0; i < total; ++i) {
        ASSERT_EQ(ref[i], output[i]);
    }
}

TEST_F(MultiwayMerge, File_Wrapper_with_many_Runs) {
    static const bool debug = false;
    std::mt19937 gen(0);
    // get _a_ different runs with _a_ number of elements
    size_t a = 400;
    std::vector<size_t> b;
    b.reserve(a);

    for (size_t t = 0; t < a; ++t) {
        b.push_back(400 + gen() % 100);
    }

    size_t total = 0;
    for (auto t : b) {
        total += t;
    }

    using Iterator = thrill::core::FileIteratorWrapper<size_t>;
    using OIterator = thrill::core::FileOutputIteratorWrapper<size_t>;
    using File = data::File;
    using Reader = File::Reader;
    using Writer = File::Writer;
    std::vector<File> in;
    std::vector<size_t> ref;
    std::vector<size_t> output;
    std::vector<std::pair<Iterator, Iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (size_t i = 0; i < a; ++i) {
        std::vector<size_t> tmp;
        tmp.reserve(b[i]);
        for (size_t j = 0; j < b[i]; ++j) {
            auto elem = rand() % 100;
            sLOG << "FILE" << i << "with elem" << elem;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(tmp.begin(), tmp.end());

        data::File f(block_pool_);
        {
            auto w = f.GetWriter();
            for (auto& t : tmp) {
                w(t);
            }
        }
        in.emplace_back(std::move(f));
    }

    for (size_t t = 0; t < in.size(); ++t) {
        auto reader = std::make_shared<Reader>(in[t].GetReader(true));
        Iterator s(&in[t], reader, 0, true);
        Iterator e(&in[t], reader, in[t].num_items(), false);
        seq.push_back(std::make_pair(s, e));
    }

    data::File output_file(block_pool_);

    {
        OIterator oiter(std::make_shared<Writer>(output_file.GetWriter()));

        std::sort(ref.begin(), ref.end());
        core::sequential_file_multiway_merge<true, false>(seq.begin(),
                                                          seq.end(),
                                                          oiter,
                                                          total,
                                                          std::less<size_t>());
    }

    auto r = output_file.GetReader(true);
    for (size_t i = 0; i < total; ++i) {
        auto e = r.Next<size_t>();
        sLOG << std::setw(3) << ref[i] << std::setw(3) << e;
        ASSERT_EQ(ref[i], e);
    }
}

TEST_F(MultiwayMerge, File_Wrapper_with_1_Runs) {
    static const bool debug = false;
    std::mt19937 gen(0);
    size_t a = 1;
    size_t b = 100;
    size_t total = a * b;

    using Iterator = thrill::core::FileIteratorWrapper<size_t>;
    using OIterator = thrill::core::FileOutputIteratorWrapper<size_t>;
    using File = data::File;
    using Reader = File::Reader;
    using Writer = File::Writer;
    std::vector<File> in;
    std::vector<size_t> ref;
    std::vector<size_t> output;
    std::vector<std::pair<Iterator, Iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (size_t i = 0; i < a; ++i) {
        std::vector<size_t> tmp;
        tmp.reserve(b);
        for (size_t j = 0; j < b; ++j) {
            auto elem = gen() % 100;
            sLOG << "FILE" << i << "with elem" << elem;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(tmp.begin(), tmp.end());

        data::File f(block_pool_);
        {
            auto w = f.GetWriter();
            for (auto& t : tmp) {
                w(t);
            }
        }
        in.emplace_back(std::move(f));
    }

    for (size_t t = 0; t < in.size(); ++t) {
        auto reader = std::make_shared<Reader>(in[t].GetReader(true));
        Iterator s(&in[t], reader, 0, true);
        Iterator e(&in[t], reader, in[t].num_items(), false);
        seq.push_back(std::make_pair(s, e));
    }

    data::File output_file(block_pool_);
    {
        OIterator oiter(std::make_shared<Writer>(output_file.GetWriter()));

        std::sort(ref.begin(), ref.end());
        core::sequential_file_multiway_merge<true, false>(seq.begin(),
                                                          seq.end(),
                                                          oiter,
                                                          total,
                                                          std::less<size_t>());
    }

    auto r = output_file.GetReader(true);
    for (size_t i = 0; i < total; ++i) {
        auto e = r.Next<size_t>();
        sLOG << std::setw(3) << ref[i] << std::setw(3) << e;
        ASSERT_EQ(ref[i], e);
    }
}

TEST_F(MultiwayMerge, GetMultiwayMergePuller) {
    static const bool debug = false;
    std::mt19937 gen(0);
    size_t a = 4;
    size_t b = 3;
    size_t total = a * b;

    using Iterator = thrill::core::FileIteratorWrapper<size_t>;
    using File = data::File;
    using Reader = File::Reader;
    // using Writer = File::Writer;
    std::vector<File> in;
    std::vector<size_t> ref;
    std::vector<size_t> output;
    std::vector<std::pair<Iterator, Iterator> > seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (size_t i = 0; i < a; ++i) {
        std::vector<size_t> tmp;
        tmp.reserve(b);
        for (size_t j = 0; j < b; ++j) {
            auto elem = gen() % 100;
            sLOG << "FILE" << i << "with elem" << elem;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(tmp.begin(), tmp.end());

        data::File f(block_pool_);
        {
            auto w = f.GetWriter();
            for (auto& t : tmp) {
                w(t);
            }
        }
        in.emplace_back(std::move(f));
    }

    for (size_t t = 0; t < in.size(); ++t) {
        auto reader = std::make_shared<Reader>(in[t].GetReader(true));
        Iterator s(&in[t], reader, 0, true);
        Iterator e(&in[t], reader, in[t].num_items(), false);
        seq.push_back(std::make_pair(s, e));
    }

    auto puller = core::get_sequential_file_multiway_merge_tree<true, false>(
        seq.begin(),
        seq.end(),
        total,
        std::less<size_t>());

    std::sort(ref.begin(), ref.end());

    for (size_t i = 0; i < total; ++i) {
        auto e = puller.Next();
        sLOG << std::setw(3) << ref[i] << std::setw(3) << e;
        ASSERT_EQ(ref[i], e);
    }
}

/******************************************************************************/
