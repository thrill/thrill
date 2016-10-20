/*******************************************************************************
 * tests/common/zip_stream_example.cpp
 *
 * Simple test for zip_istream on-the-fly decompression filter.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/common/die.hpp>
#include <thrill/common/zip_stream.hpp>

#include <fstream>
#include <iostream>

using namespace thrill;

int main(int argc, char* argv[]) {
    if (argc == 1) {
        common::zip_istream zipper(std::cin);
        std::cout << zipper.rdbuf();
    }
    else {
        std::ifstream in(argv[1]);
        common::zip_istream zipper(in);
        std::cout << zipper.rdbuf();
    }

    return 0;
}

/******************************************************************************/
