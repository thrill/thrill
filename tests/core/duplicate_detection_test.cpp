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
#include <thrill/common/math.hpp>
#include <thrill/core/duplicate_detection.hpp>

using namespace thrill; //NOLINT

TEST(DuplicateDetection, AllDuplicatedList) {

    auto start_func = [](Context& ctx) {
                          size_t elements = 10000;

                          std::vector<size_t> duplicates;
                          std::vector<size_t> hashes;

                          for (size_t i = 0; i < elements; ++i) {
                              hashes.push_back(i);
                          }

                          core::DuplicateDetection duplicate_detection;

                          duplicate_detection.FindDuplicates(duplicates,
                                                             hashes,
                                                             ctx,
                                                             elements,
                                                             0);

                          if (ctx.num_workers() > 1) {
                              ASSERT_EQ(hashes, duplicates);
                          }
                          else {
                              ASSERT_EQ(duplicates, std::vector<size_t>());
                          }
                      };

    api::RunLocalTests(start_func);
}

TEST(DuplicateDetection, SomeDuplicatedElements) {

    auto start_func = [](Context& ctx) {

                          // Local elements are [myrange.begin, myrange.end + delta)
                          // delta elements are duplicates with next worker

                          size_t elements = 10000;
                          size_t delta = 50;

                          // save unneeded test complexity, lower delta if this fails
                          // test will remain pretty much the same
                          assert(elements > delta * ctx.num_workers());

                          std::vector<size_t> splitters;

                          for (size_t i = 0; i < ctx.num_workers() - 1; ++i) {
                              double per_pe = static_cast<double>(elements) / static_cast<double>(ctx.num_workers());
                              splitters.push_back(
                                  static_cast<size_t>(std::ceil(static_cast<double>(i + 1) * per_pe)));
                          }

                          std::vector<size_t> duplicates;
                          std::vector<size_t> hashes;

                          common::Range my_range = common::CalculateLocalRange(
                              elements, ctx.num_workers(), ctx.my_rank());

                          for (size_t i = my_range.begin; i < my_range.end + delta; ++i) {
                              hashes.push_back(i);
                          }

                          core::DuplicateDetection duplicate_detection;

                          size_t unique_per = elements / ctx.num_workers() + delta;

                          duplicate_detection.FindDuplicates(duplicates,
                                                             hashes,
                                                             ctx,
                                                             unique_per,
                                                             0);

                          for (size_t i = 0; i < splitters.size(); ++i) {
                              for (size_t j = 0; j < delta; ++j) {
                                  ASSERT_EQ(duplicates[i * delta + j], splitters[i] + j);
                              }
                          }
                      };

    api::RunLocalTests(start_func);
}

TEST(DuplicateDetection, SomeDuplicatedElementsNonConsec) {

    auto start_func = [](Context& ctx) {

                          // Local elements are [myrange.begin, myrange.end + delta)
                          // delta elements are duplicates with next worker

                          size_t elements = 10000;
                          size_t delta = 50;
                          size_t multiplier = 7;

                          // save unneeded test complexity, lower delta if this fails
                          // test will remain pretty much the same
                          assert(elements > delta * ctx.num_workers());

                          std::vector<size_t> splitters;

                          for (size_t i = 0; i < ctx.num_workers() - 1; ++i) {
                              double per_pe = static_cast<double>(elements) / static_cast<double>(ctx.num_workers());
                              splitters.push_back(
                                  static_cast<size_t>(std::ceil(static_cast<double>((i + 1) * per_pe))));
                          }

                          std::vector<size_t> duplicates;
                          std::vector<size_t> hashes;

                          common::Range my_range = common::CalculateLocalRange(
                              elements, ctx.num_workers(), ctx.my_rank());

                          for (size_t i = my_range.begin; i < my_range.end + delta; ++i) {
                              hashes.push_back(i * multiplier);
                          }

                          core::DuplicateDetection duplicate_detection;

                          size_t unique_per = elements / ctx.num_workers() + delta;

                          duplicate_detection.FindDuplicates(duplicates,
                                                             hashes,
                                                             ctx,
                                                             unique_per,
                                                             0);

                          for (size_t i = 0; i < splitters.size(); ++i) {
                              for (size_t j = 0; j < delta; ++j) {
                                  ASSERT_EQ(duplicates[i * delta + j], (splitters[i] + j) * multiplier);
                              }
                          }
                      };

    api::RunLocalTests(start_func);
}

TEST(DuplicateDetection, AllDuplicatedHash) {

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

                          if (ctx.num_workers() > 1) {
                              ASSERT_EQ(hashes, duplicates);
                          }
                          else {
                              ASSERT_EQ(duplicates, std::vector<size_t>());
                          }
                      };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
