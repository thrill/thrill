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

TEST(DynamicBitset, GolombCoding) {

	const size_t cluster_size = 1;
	const size_t bitset_size = 1024 * 1024;
	const size_t num_elements = 1024 * 64;

	std::default_random_engine generator(std::random_device { } ());
	std::uniform_int_distribution<size_t> distribution(0, 1024 * 1024 * 1024);

	thrill::core::DistributedBitset<bitset_size / cluster_size, cluster_size>
			dbs(0, bitset_size);
	dbs.Add(0);
	for (size_t i = 1; i < num_elements; ++i) {		
		dbs.Add(distribution(generator));
	}

	thrill::core::DynamicBitset<size_t> bitset = dbs.Golombify(0);

	std::bitset<bitset_size> degolombed = dbs.Degolombify(bitset);

	std::bitset<bitset_size> non_golombed = dbs.Get(0);

	for (size_t i = 0; i < bitset_size; ++i) {
		ASSERT_EQ(degolombed[i], non_golombed[i]);
	}
	
}

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
