/*******************************************************************************
 * tests/core/duplicate_detection_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/core/duplicate_detection.hpp>

using namespace thrill; //NOLINT

TEST(DuplicateDetection, IntegersMod10000) {

	auto start_func = [](Context& ctx) {
		size_t elements = 2000;
		
		
		std::vector<size_t> duplicates;
		std::vector<size_t> hashes;
		
		for (size_t i = 0; i < elements; ++i) {
			hashes.push_back((i * 317) % 9721);
		}

		core::DuplicateDetection duplicate_detection;

		duplicate_detection.FindDuplicates(duplicates, 
										   hashes,
										   ctx,
										   elements,
										   0);
	};

	api::RunLocalTests(start_func);

}

