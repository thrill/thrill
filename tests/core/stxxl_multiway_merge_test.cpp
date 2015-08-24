/*******************************************************************************
 * tests/core/stage_builder_test.cpp
 *
 * Part of Project Thrill.
 *
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/thrill.hpp>

#include <stxxl/bits/parallel/losertree.h>
#include <stxxl/bits/parallel/multiway_merge.h>
#include <thrill/core/iterator_wrapper.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <vector>
#include <cstdlib>

using namespace thrill; // NOLINT

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
    std::size_t a = 2;
    std::size_t b = 5;
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
            auto elem = rand() % 10;
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
    std::size_t a = 2;
    std::size_t b = 5;
    std::size_t total = 2*5;

    using iterator = thrill::core::StxxlFileWrapper<int>;
    std::vector<data::File> in;
    std::vector<data::File::Reader> in_reader;
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

        // REVIEW(cn): this File only lives for one iteration of the loop.
        data::File f;
        auto w = f.GetWriter();
        for (auto & t : tmp) {
            w(t);
        }
        w.Close();
        in.push_back(f);
        in_reader.push_back(f.GetReader());
    }

    for (std::size_t t = 0; t < in.size(); ++t) {
        seq.push_back(std::make_pair(thrill::core::StxxlFileWrapper<int>(&in[t], &in_reader[t], 0),
                                     thrill::core::StxxlFileWrapper<int>(&in[t], &in_reader[t], in[t].NumItems())));
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

/******************************************************************************/
