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
#include <thrill/core/distributed_bitset.hpp>
#include <thrill/core/dynamic_bitset.hpp>

#include <cmath>

TEST(DynamicBitset, KnownData) {
	size_t elements = 1000;
	double fpr_parameter = 8;
	size_t space_bound = elements * (2 + std::log2(fpr_parameter));
	size_t b = (size_t)(std::log(2) * fpr_parameter);

	thrill::core::DynamicBitset<size_t> golomb_coder(space_bound, false, b);

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

	thrill::core::DynamicBitset<size_t> golomb_coder(space_bound, false, b);

	golomb_coder.golomb_in(0);
	for (size_t i = 0; i < elements; ++i) {
		golomb_coder.golomb_in((i % 20) + 1);
	}

	golomb_coder.seek();

	thrill::core::DynamicBitset<size_t> out_coder(golomb_coder.GetGolombData(),
												  golomb_coder.size(),
												  b);

	for (size_t i = 0; i < golomb_coder.size(); i++) {
		ASSERT_EQ(golomb_coder.GetGolombData()[i], out_coder.GetGolombData()[i]);
	}

	out_coder.seek();

	ASSERT_EQ(out_coder.GetBuffer(), golomb_coder.GetBuffer());

	ASSERT_EQ(0, out_coder.golomb_out());
	for (size_t i = 0; i < elements; ++i) {
		size_t out = out_coder.golomb_out();
		ASSERT_EQ((i % 20) + 1, out);
	}
	
}

