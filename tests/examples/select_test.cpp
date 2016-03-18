/*******************************************************************************
 * tests/examples/select_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/select/select.hpp>

#include <thrill/api/distribute.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;
using namespace examples::select;

/******************************************************************************/
// Zipf generated graph

TEST(Select, SelectRandom) {
    static constexpr bool debug = false;

    static constexpr size_t input_size = 10000;
    static constexpr size_t select_rank = 3000;

    // generate some random integers
    std::vector<size_t> input(input_size);

    std::minstd_rand rng(123456);
    for (size_t i = 0; i < input_size; ++i) {
        input[i] = rng();
    }

    // calculate correct result
    size_t correct;
    {
        std::vector<size_t> input_copy = input;
        std::nth_element(input_copy.begin(), input_copy.begin() + select_rank,
                         input_copy.end());

        LOG << "selected element: " << input_copy[select_rank];
        correct = input_copy[select_rank];
    }

    auto start_func =
        [&input, &correct](Context& ctx) {
            ctx.enable_consume();

            auto input_dia = Distribute(ctx, input);

            size_t element = Select(input_dia, select_rank);

            LOG << "selected element: " << element;
            ASSERT_EQ(correct, element);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
