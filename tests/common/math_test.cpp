/*******************************************************************************
 * tests/common/math_test.cpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/common/math.hpp>

#include <sstream>
#include <vector>

using namespace thrill;

TEST(Math, OneFactor) {
    static const bool debug = false;

    for (size_t n = 1; n < 20; ++n) {
        // print header
        std::ostringstream os1;
        os1 << "n" << n;
        for (size_t p = 0; p < n; ++p) {
            os1 << "  " << p;
        }
        LOG << os1.str();

        // check communication matrix (that every pair communicated)
        std::vector<std::vector<size_t> > matrix;
        for (size_t i = 0; i < n; ++i) {
            matrix.emplace_back(n);
        }

        for (size_t r = 0; r < common::CalcOneFactorSize(n); ++r) {
            std::ostringstream os2;
            os2 << "r" << r;

            std::vector<size_t> peer(n);

            for (size_t p = 0; p < n; ++p) {
                size_t x = common::CalcOneFactorPeer(r, p, n);
                os2 << "  " << x;
                peer[p] = x;
            }
            LOG << os2.str();

            // test that peers communicate with each other
            for (size_t i = 0; i < peer.size(); ++i) {
                die_unless(peer[i] == i || peer[peer[i]] == i);

                die_unless(matrix[i][peer[i]] == 0);
                matrix[i][peer[i]] = 1;
            }
        }
        LOG << "";

        // test that all peers communicated
        for (size_t i = 0; i < matrix.size(); ++i) {
            for (size_t j = 0; j < matrix.size(); ++j) {
                die_unless(matrix[i][j] || i == j);
            }
        }
    }
}

/******************************************************************************/
