/*******************************************************************************
 * tests/api/hyperloglog_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Moritz Kiefer <moritz.kiefer@purelyfunctional.org>
 * Copyright (C) 2017 Tino Fuhrmann <tino-fuhrmann@web.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/generate.hpp>
#include <thrill/api/hyperloglog.hpp>

#include <gtest/gtest.h>

#include <algorithm>
#include <functional>
#include <random>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

double relativeError(double trueVal, double estimate) {
    return estimate / trueVal - 1;
}

TEST(Operations, HyperLogLog) {
    auto start_func =
        [](Context& ctx) {
            static constexpr bool debug = false;
            size_t n = 100000;

            auto indices = Generate(ctx, n);

            double estimate = indices.HyperLogLog<4>();
            LOG << "hyperloglog with p=" << 4 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.360370);

            estimate = indices.HyperLogLog<5>();
            LOG << "hyperloglog with p=" << 5 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.558514);

            estimate = indices.HyperLogLog<6>();
            LOG << "hyperloglog with p=" << 6 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.142211);

            estimate = indices.HyperLogLog<7>();
            LOG << "hyperloglog with p=" << 7 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.0219474);

            estimate = indices.HyperLogLog<8>();
            LOG << "hyperloglog with p=" << 8 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.0186043);

            estimate = indices.HyperLogLog<9>();
            LOG << "hyperloglog with p=" << 9 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.0676228);

            estimate = indices.HyperLogLog<10>();
            LOG << "hyperloglog with p=" << 10 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.0192143);

            estimate = indices.HyperLogLog<11>();
            LOG << "hyperloglog with p=" << 11 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.0113029);

            estimate = indices.HyperLogLog<12>();
            LOG << "hyperloglog with p=" << 12 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.0110096);

            estimate = indices.HyperLogLog<13>();
            LOG << "hyperloglog with p=" << 13 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.0140553);

            estimate = indices.HyperLogLog<14>();
            LOG << "hyperloglog with p=" << 14 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.0137586);

            estimate = indices.HyperLogLog<15>();
            LOG << "hyperloglog with p=" << 15 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.00135144);

            estimate = indices.HyperLogLog<16>();
            LOG << "hyperloglog with p=" << 16 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            ASSERT_LE(std::abs(relativeError(n, estimate)), 0.00438102);

            LOG << "###################################################";
            LOG << "hyperloglog for small counts";
            LOG << "";

            n = 1000;

            indices = Generate(ctx, n);

            estimate = indices.HyperLogLog<4>();
            LOG << "hyperloglog with p=" << 4 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            estimate = indices.HyperLogLog<6>();
            LOG << "hyperloglog with p=" << 6 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            estimate = indices.HyperLogLog<8>();
            LOG << "hyperloglog with p=" << 8 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            estimate = indices.HyperLogLog<10>();
            LOG << "hyperloglog with p=" << 10 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            estimate = indices.HyperLogLog<12>();
            LOG << "hyperloglog with p=" << 12 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            estimate = indices.HyperLogLog<14>();
            LOG << "hyperloglog with p=" << 14 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
            estimate = indices.HyperLogLog<14>();
            LOG << "hyperloglog with p=" << 14 << ": " << estimate
                << ", relative error: " << relativeError(n, estimate);
        };

    thrill::Run([&](thrill::Context& ctx) { start_func(ctx); });
}

TEST(Operations, encodeHash) {
    // decidingBits = 0 => 1 as last Bit, vale (aka number of leading zeroes) should be 5
    uint64_t random = 0b0000100000000000000000000000100000000000000000000000000000000000;
    uint32_t encoded = core::hyperloglog::encodeHash<25, 16>(random);
    uint32_t manualEncoded = 0b00001000000000000000000000001001;
    ASSERT_EQ(manualEncoded, encoded);

    // decidingBits = 1 => 0 as last Bit, dont care about value
    random = 0b0000100000000000000010000000100000000000000000000000000000000000;
    encoded = core::hyperloglog::encodeHash<25, 16>(random);
    manualEncoded = 0b00001000000000000000100000000000;
    ASSERT_EQ(manualEncoded, encoded);
}

TEST(Operations, decodeHash) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint64_t> dis(0, 18446744073709551615u);
    uint32_t densePrecision = 4;
    for (int n = 0; n < 1000; ++n) {
        uint64_t random = dis(gen);
        uint32_t index = random >> (64 - densePrecision);
        uint64_t valueBits = random << densePrecision;
        uint8_t value =
            valueBits == 0 ? (64 - densePrecision) : __builtin_clzll(valueBits);
        value++;
        uint32_t encoded = core::hyperloglog::encodeHash<25, 4>(random);
        auto decoded = core::hyperloglog::decodeHash<25, 4>(encoded);
        ASSERT_EQ(index, decoded.first);
        ASSERT_EQ(value, decoded.second);
    }
    densePrecision = 12;
    for (int n = 0; n < 1000; ++n) {
        uint64_t random = dis(gen);
        uint32_t index = random >> (64 - densePrecision);
        uint64_t valueBits = random << densePrecision;
        uint8_t value =
            valueBits == 0 ? (64 - densePrecision) : __builtin_clzll(valueBits);
        value++;
        uint32_t encoded = core::hyperloglog::encodeHash<25, 12>(random);
        auto decoded = core::hyperloglog::decodeHash<25, 12>(encoded);
        ASSERT_EQ(index, decoded.first);
        ASSERT_EQ(value, decoded.second);
    }
}

TEST(Operations, sparseListEncoding) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32_t> dis(
        0, std::numeric_limits<uint32_t>::max());
    std::uniform_int_distribution<size_t> lengthDis(0, 10000);
    for (size_t i = 0; i < 10; ++i) {
        size_t length = lengthDis(gen);
        std::vector<uint32_t> input;
        input.reserve(length);
        for (size_t j = 0; j < length; ++j) {
            input.emplace_back(dis(gen));
        }
        std::sort(input.begin(), input.end());
        std::vector<uint8_t> encoded = core::hyperloglog::encodeSparseList(input);

        // check decoding
        std::vector<uint32_t> decoded = core::hyperloglog::decodeSparseList(encoded);
        ASSERT_EQ(input, decoded);
    }
}

/******************************************************************************/
