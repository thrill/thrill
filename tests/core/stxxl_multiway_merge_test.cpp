/*******************************************************************************
 * tests/core/stage_builder_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/thrill.hpp>

#include <thrill/core/stxxl_multiway_merge.hpp>
#include <thrill/core/iterator_wrapper.hpp>
#include <thrill/data/file.hpp>

 #include <thrill/common/logger.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <vector>
#include <cstdlib>

using namespace thrill; // NOLINT

static const bool debug = false;

TEST(MultiwayMerge, Basic) {
    std::size_t a = 2;
    std::size_t b = 5;
    std::size_t total = 2*5;

    using iterator = typename std::vector<int>::iterator;
    std::vector<std::vector<int>> in;
    std::vector<int> ref;
    std::vector<int> output;
    std::vector<std::pair<iterator, iterator>> seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (std::size_t i = 0; i < a; ++i) {
        std::vector<int> tmp;
        tmp.reserve(b);
        for (std::size_t j = 0; j < b; ++j) {
            auto elem = rand() % 10;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(std::begin(tmp), std::end(tmp));
        in.push_back(tmp);
    }

    for (auto & vec : in) {
        seq.push_back(std::make_pair(std::begin(vec), std::end(vec)));
    }

    std::sort(std::begin(ref), std::end(ref));
    stxxl::parallel::sequential_multiway_merge<true, false>(std::begin(seq),
                                                            std::end(seq),
                                                            std::begin(output),
                                                            total,
                                                            std::less<int>());
    for (std::size_t i = 0; i < total; ++i) {
        ASSERT_EQ(ref[i], output[i]);
    }
}


TEST(MultiwayMerge, Vector_Wrapper) {
    std::size_t a = 200;
    std::size_t b = 50;
    std::size_t total = 2*5;

    using iterator = thrill::core::StxxlVectorWrapper<int>;
    std::vector<std::vector<int>> in;
    std::vector<int> ref;
    std::vector<int> output;
    std::vector<std::pair<iterator, iterator>> seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (std::size_t i = 0; i < a; ++i) {
        std::vector<int> tmp;
        tmp.reserve(b);
        for (std::size_t j = 0; j < b; ++j) {
            auto elem = rand();
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(std::begin(tmp), std::end(tmp));
        in.push_back(tmp);
    }

    for (auto & vec : in) {
        seq.push_back(std::make_pair(thrill::core::StxxlVectorWrapper<int>(&vec, 0),
                                     thrill::core::StxxlVectorWrapper<int>(&vec, b)));
    }

    std::sort(std::begin(ref), std::end(ref));
    stxxl::parallel::sequential_multiway_merge<true, false>(std::begin(seq),
                                                            std::end(seq),
                                                            std::begin(output),
                                                            total,
                                                            std::less<int>());
    for (std::size_t i = 0; i < total; ++i) {
        ASSERT_EQ(ref[i], output[i]);
    }
}

TEST(MultiwayMerge, File_Wrapper) {
    std::size_t a = 150;
    std::size_t b = 200;
    std::size_t total = a*b;

    using Iterator = thrill::core::StxxlFileWrapper<int>;
    using File = data::File;
    using Reader = File::Reader;
    std::vector<File> in;
    std::vector<int> ref;
    std::vector<int> output;
    std::vector<std::pair<Iterator, Iterator>> seq;

    in.reserve(a);
    ref.reserve(total);
    seq.reserve(a);
    output.resize(total);

    for (std::size_t i = 0; i < a; ++i) {
        std::vector<int> tmp;
        tmp.reserve(b);
        for (std::size_t j = 0; j < b; ++j) {
            auto elem = rand();
            sLOG << "FILE" << i << "with elem" << elem;
            tmp.push_back(elem);
            ref.push_back(elem);
        }
        std::sort(std::begin(tmp), std::end(tmp));

        data::File f;
        {
            auto w = f.GetWriter();
            for (auto & t : tmp) {
                w(t);
            }
        }
        in.push_back(f);
    }

    for (std::size_t t = 0; t < in.size(); ++t) {
        auto reader = std::make_shared<Reader>(in[t].GetReader());
        Iterator s = Iterator(&in[t], reader, 0, true);
        Iterator e = Iterator(&in[t], reader, in[t].NumItems(), false);
        seq.push_back(std::make_pair(s, e));
    }

    std::sort(std::begin(ref), std::end(ref));
    stxxl::parallel::sequential_multiway_merge<true, false>(std::begin(seq),
                                                            std::end(seq),
                                                            std::begin(output),
                                                            total,
                                                            std::less<int>());

    for (std::size_t i = 0; i < total; ++i) {
        sLOG << std::setw(3) << ref[i] << std::setw(3) << output[i];
        ASSERT_EQ(ref[i], output[i]);
    }



}

/******************************************************************************/
