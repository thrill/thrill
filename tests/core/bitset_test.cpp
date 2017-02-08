/*******************************************************************************
 * tests/core/bitset_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>

#include <thrill/common/logger.hpp>
#include <thrill/core/bit_stream.hpp>
#include <thrill/core/dynamic_bitset.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <cmath>
#include <utility>
#include <vector>

using namespace thrill;

#define PRINT(x) LOG1 << x;

TEST(DynamicBitset, DISABLED_ABC) {
    std::vector<uint64_t> hashes;
    hashes.reserve(100);

    uint64_t B = 6399;
    uint64_t m = 100;
    uint64_t b = 44;
    uint64_t upper_space_bound = m * (2 + common::IntegerLog2Ceil((B / m) + 1));
    core::DynamicBitset<uint64_t> golomb_code(upper_space_bound, false, b);
    PRINT("golomb parameters:");
    PRINT("B = " << B);
    PRINT("m = " << m);
    PRINT("b = " << b);
    PRINT("w.c. size = " << upper_space_bound << " bits "
          "[" << upper_space_bound / 8.0 << " Bytes]");
    PRINT("exp. size = " << m * (1.5 + common::IntegerLog2Ceil((B / m) + 1)) << "\tbits [" << (m * (1.5 + common::IntegerLog2Ceil((B / m) + 1))) / 8.0 << " Bytes]");

    uint64_t val;
    golomb_code.seek(0);
    hashes.clear();
    for (int i = 1; i < 100; ++i) {
        golomb_code.golomb_in((uint64_t)21);
        hashes.push_back(21);
    }
    golomb_code.golomb_in((uint64_t)4320);
    hashes.push_back(4320);

    PRINT("RAW GOLOMB DATA:");
    uint64_t* data = golomb_code.data();
    for (int i = 0; i < golomb_code.size(); ++i) {
        PRINT(data[i]);
    }

    uint64_t golomb_size = golomb_code.byte_size();

    PRINT("Result:");
    PRINT(hashes.size() * 8.0 << " Bytes compressed to " << golomb_size << " Bytes - compression factor: " << (hashes.size() * 8.0) / golomb_size);
    PRINT("min. delta: " << 21);
    PRINT("max. delta: " << 4320);
    PRINT("Code size [bits] (experimental, byte-aligned): " << (golomb_size * 8.0));
    PRINT("Code size [bits] (experimental, raw)         : " << golomb_code.pos() * 64 + golomb_code.bits());
    PRINT("Code size [bits] (optimal lower bound)       : " << (100 * log2(B / m)));
    PRINT("bits/hash (experimental, byte-aligned): " << (golomb_size * 8.0) / 100);
    PRINT("bits/hash (experimental, raw)         : " << (golomb_code.pos() * 64.0 + golomb_code.bits()) / 100);
    PRINT("bits/hash (optimal lower bound)       : " << (100.0 * log2(B / m)) / 100);

    std::vector<uint64_t> decoded_hashes;
    val = 0;
    golomb_code.seek(0);
    for (std::vector<uint64_t>::size_type i = 0; i < hashes.size(); ++i) {
        val = golomb_code.golomb_out();
        decoded_hashes.push_back(val);
    }

    // PRINT("maxpos" << golomb_code.GetMaxPos());
    // PRINT("pos" << golomb_code.pos());
    // PRINT("bits" << golomb_code.bits());
    ASSERT_EQ(golomb_code.pos() * 64 + golomb_code.bits(), 797 // , "FAILED: wrong golomb code size"
              );
    ASSERT_EQ(hashes, decoded_hashes                           // , "FAILED: encoded values != decoded values"
              );
    PRINT("--------------------> SUCCESS!");
}

TEST(DynamicBitset, KnownData) {
    size_t elements = 1000;
    double fpr_parameter = 8;
    size_t space_bound = elements * (2 + std::log2(fpr_parameter));
    size_t b = (size_t)(std::log(2) * fpr_parameter);

    core::DynamicBitset<size_t> golomb_coder(space_bound, false, b);

    golomb_coder.golomb_in(0);
    for (size_t i = 0; i < elements; ++i) {
        golomb_coder.golomb_in((i % 20) + 1);
    }

    golomb_coder.seek();

    ASSERT_EQ(0, golomb_coder.golomb_out());
    for (size_t i = 0; i < elements; ++i) {
        ASSERT_EQ((i % 20) + 1, golomb_coder.golomb_out());
    }
}

TEST(DynamicBitset, KnownRawData) {
    size_t elements = 1000;
    double fpr_parameter = 8;
    size_t space_bound = elements * (2 + std::log2(fpr_parameter));
    size_t b = (size_t)(std::log(2) * fpr_parameter);

    core::DynamicBitset<size_t> golomb_coder(space_bound, false, b);

    golomb_coder.golomb_in(0);
    for (size_t i = 0; i < elements; ++i) {
        golomb_coder.golomb_in((i % 20) + 1);
    }

    golomb_coder.seek();

    core::DynamicBitset<size_t> out_coder(
        golomb_coder.data(), golomb_coder.size(), b, elements);

    for (size_t i = 0; i < golomb_coder.size(); i++) {
        ASSERT_EQ(golomb_coder.data()[i], out_coder.data()[i]);
    }

    out_coder.seek();

    ASSERT_EQ(out_coder.buffer(), golomb_coder.buffer());

    ASSERT_EQ(0, out_coder.golomb_out());
    for (size_t i = 0; i < elements; ++i) {
        size_t out = out_coder.golomb_out();
        ASSERT_EQ((i % 20) + 1, out);
    }
}

TEST(DynamicBitset, RandomData) {
    size_t elements = 10000;
    size_t mod = 100000;
    double fpr_parameter = 8;
    size_t space_bound = elements * (2 + std::log2(fpr_parameter));
    size_t b = (size_t)(std::log(2) * fpr_parameter);

    core::DynamicBitset<size_t> golomb_coder(space_bound, false, b);

    std::vector<size_t> elements_vec;
    std::default_random_engine generator(std::random_device { } ());
    std::uniform_int_distribution<int> distribution(1, mod);

    for (size_t i = 0; i < elements; ++i) {
        elements_vec.push_back(distribution(generator));
    }

    std::sort(elements_vec.begin(), elements_vec.end());

    size_t last = 0;
    size_t uniques = 0;
    for (size_t i = 0; i < elements; ++i) {
        if (elements_vec[i] > last) {
            uniques++;
            golomb_coder.golomb_in(elements_vec[i] - last);
            last = elements_vec[i];
        }
    }

    // golomb_coder.seek();

    core::DynamicBitset<size_t> out_coder(
        golomb_coder.data(), golomb_coder.size(), b, uniques);

    out_coder.seek();

    last = 0;
    for (size_t i = 0; i < elements; ++i) {
        if (elements_vec[i] > last) {
            size_t out = out_coder.golomb_out() + last;
            last = out;
            ASSERT_EQ(elements_vec[i], out);
        }
    }
}

/******************************************************************************/
