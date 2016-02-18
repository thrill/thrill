/*******************************************************************************
 * tests/common/json_logger_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/json_logger.hpp>

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace thrill;

// common::JsonLine SubFunction() {
//     common::JsonLine out;
//     out << "answer" << 42;
//     return out;
// }

TEST(JsonLogger, Test1) {

    common::JsonLogger logger;

    logger << "Node" << "Sort\nNode"
           << "bool" << true
           << "int" << 5
           << "double" << 1.5
           << "string" << std::string("abc")
           << "vector" << std::vector<int>({ 6, 9, 42 })
           << "string vector" << std::vector<const char*>({ "abc", "def" });
//           << "sub" << SubFunction()
//           << "sub2" << common::JsonLine("answer", 42);
}

/******************************************************************************/
