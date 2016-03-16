/*******************************************************************************
 * tests/api/sampling_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <huebschle@kit.edu>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/allgather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/sample.hpp>
#include <thrill/api/size.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

static const bool debug = false;

TEST(SamplingNode, CompileAndExecute) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {
            size_t n = 1024;

            auto sizets = Generate(ctx, n);

            // sample
            auto reduced1 = sizets.Sample(0.25);
            auto reduced2 = sizets.Sample(0.05);
            auto out_vec1 = reduced1.AllGather();
            auto out_vec2 = reduced2.AllGather();

            LOG << "result size 0.25: " << out_vec1.size() << " / " << sizets.Size();
            LOG << "result size 0.05: " << out_vec2.size() << " / " << sizets.Size();
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
