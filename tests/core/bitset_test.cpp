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

#include <thrill/core/distributed_bitset.hpp>

TEST(DistributedBitset, Construct) {

	const size_t cluster_size = 32;
	const size_t bitset_size = (1024 * 1024);

	size_t last_end;

	for (size_t i = 0; i < cluster_size; ++i) {

		thrill::core::DistributedBitset<bitset_size / cluster_size, cluster_size>
			dbs_i(i, bitset_size);
		if (i == 0) {
			ASSERT_EQ(dbs_i.MyStart(), (size_t) 0);
		} else {
			ASSERT_EQ(dbs_i.MyStart(), last_end + 1);
		}
		if (i == cluster_size - 1) {
			ASSERT_EQ(dbs_i.MyEnd(), bitset_size - 1);
		}
		last_end = dbs_i.MyEnd();
	}
}
