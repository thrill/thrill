/*******************************************************************************
 * tests/common/matrix_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/common/logger.hpp>
#include <thrill/common/matrix.hpp>

#include <vector>

using namespace thrill;

TEST(Matrix, Simple) {
    using DMatrix = common::Matrix<double>;
    DMatrix matrix1(4);

    matrix1(0, 0) = 1;
    matrix1(1, 1) = 1;
    matrix1(2, 2) = 1;
    matrix1(3, 3) = 1;

    DMatrix matrix2 = matrix1;
    matrix2 = matrix1 + matrix2;

    matrix1 *= 2;

    ASSERT_EQ(matrix2, matrix1);
}

/******************************************************************************/
