/*******************************************************************************
 * tests/core/multiway_merge_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Huyen Chau Nguyen <hello@chau-nguyen.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>

#include <thrill/common/function_traits.hpp>
#include <thrill/core/multiway_merge.hpp>
#include <thrill/core/multiway_merge_attic.hpp>
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

struct MultiwayMerge : public ::testing::Test {
    data::BlockPool block_pool_;
};

TEST_F(MultiwayMerge, Basic) {
    // static constexpr bool debug = false;
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

    for (auto & vec : in) {
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

TEST_F(MultiwayMerge, GetMultiwayMergePuller) {
    static constexpr bool debug = false;
    std::mt19937 gen(0);
    size_t a = 4;
    size_t b = 3;
    size_t total = a * b;

    using File = data::File;
    std::vector<File> in;
    std::vector<size_t> ref;
    std::vector<size_t> output;

    in.reserve(a);
    ref.reserve(total);
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

        data::File f(block_pool_, 0, /* dia_id */ 0);
        {
            auto w = f.GetWriter();
            for (auto & t : tmp) {
                w.Put(t);
            }
        }
        in.emplace_back(std::move(f));
    }

    std::vector<data::File::ConsumeReader> seq;
    seq.reserve(a);

    for (size_t t = 0; t < in.size(); ++t)
        seq.emplace_back(in[t].GetConsumeReader());

    auto puller = core::make_multiway_merge_tree<size_t>(
        seq.begin(), seq.end(), std::less<size_t>());

    std::sort(ref.begin(), ref.end());

    for (size_t i = 0; i < total; ++i) {
        ASSERT_TRUE(puller.HasNext());
        auto e = puller.Next();
        sLOG << std::setw(3) << ref[i] << std::setw(3) << e;
        ASSERT_EQ(ref[i], e);
    }
    ASSERT_FALSE(puller.HasNext());
}

/******************************************************************************/
