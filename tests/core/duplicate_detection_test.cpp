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
                          size_t elements = 10;

                          std::vector<size_t> duplicates;
                          std::vector<size_t> hashes;

                          for (size_t i = 0; i < elements; ++i) {
                              hashes.push_back(i);
                          }

                          core::DuplicateDetection duplicate_detection;

                          duplicate_detection.FindNonDuplicates(duplicates,
                                                                hashes,
                                                                ctx,
                                                                0);

                          if (ctx.num_workers() > 1) {
                              ASSERT_EQ(duplicates, std::vector<size_t>());
                          }
                          else {
                              ASSERT_EQ(duplicates, hashes);
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
                          assert(elements > 2 * delta * ctx.num_workers());

                          std::vector<size_t> splitters;

                          for (size_t i = 0; i < ctx.num_workers() - 1; ++i) {
                              double per_pe = static_cast<double>(elements) / static_cast<double>(ctx.num_workers());
                              splitters.push_back(
                                  static_cast<size_t>(std::ceil(static_cast<double>(i + 1) * per_pe)));
                          }

                          std::vector<size_t> non_duplicates;
                          std::vector<size_t> hashes;

                          common::Range my_range = common::CalculateLocalRange(
                              elements, ctx.num_workers(), ctx.my_rank());

                          for (size_t i = my_range.begin; i < my_range.end + delta; ++i) {
                              hashes.push_back(i);
                          }

                          core::DuplicateDetection duplicate_detection;

                          duplicate_detection.FindNonDuplicates(non_duplicates,
                                                                hashes,
                                                                ctx,
                                                                0);

                          if (ctx.num_workers() > 1) {
                              size_t my_range_size =
                                  my_range.end - my_range.begin;
                              if (ctx.my_rank() == 0 ||
                                  ctx.my_rank() == ctx.num_workers() - 1) {
                                  ASSERT_EQ(my_range_size,
                                            non_duplicates.size());
                              } else {
                                  ASSERT_EQ(my_range_size - delta,
                                            non_duplicates.size());
                              }
                          } else {
                              ASSERT_EQ(elements + delta, non_duplicates.size());
                          }

                          size_t start_uniques, end_uniques;

                          if (ctx.my_rank() == 0) {
                              start_uniques = 0;
                          } else {
                              start_uniques = my_range.begin + delta;
                          }

                          if (ctx.my_rank() == ctx.num_workers() - 1) {
                              end_uniques = elements + delta;
                          } else {
                              end_uniques = my_range.end;
                          }

                          for (size_t j = 0;
                               j < (end_uniques - start_uniques); ++j) {
                              ASSERT_EQ(non_duplicates[j],
                                        j + start_uniques);
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

                          std::vector<size_t> non_duplicates;
                          std::vector<size_t> hashes;

                          common::Range my_range = common::CalculateLocalRange(
                              elements, ctx.num_workers(), ctx.my_rank());

                          for (size_t i = my_range.begin; i < my_range.end + delta; ++i) {
                              hashes.push_back(i * multiplier);
                          }

                          core::DuplicateDetection duplicate_detection;

                          duplicate_detection.FindNonDuplicates(non_duplicates,
                                                                hashes,
                                                                ctx,
                                                                0);

                           if (ctx.num_workers() > 1) {
                              size_t my_range_size =
                                  my_range.end - my_range.begin;
                              if (ctx.my_rank() == 0 ||
                                  ctx.my_rank() == ctx.num_workers() - 1) {
                                  ASSERT_EQ(my_range_size,
                                            non_duplicates.size());
                              } else {
                                  ASSERT_EQ(my_range_size - delta,
                                            non_duplicates.size());
                              }
                          } else {
                              ASSERT_EQ(elements + delta, non_duplicates.size());
                          }

                          size_t start_uniques, end_uniques;

                          if (ctx.my_rank() == 0) {
                              start_uniques = 0;
                          } else {
                              start_uniques = my_range.begin + delta;
                          }

                          if (ctx.my_rank() == ctx.num_workers() - 1) {
                              end_uniques = elements + delta;
                          } else {
                              end_uniques = my_range.end;
                          }

                          for (size_t j = 0;
                               j < (end_uniques - start_uniques); ++j) {
                              ASSERT_EQ(non_duplicates[j],
                                        multiplier * (j + start_uniques));
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

                          duplicate_detection.FindNonDuplicates(duplicates,
                                                                hashes,
                                                                ctx,
                                                                0);

                          if (ctx.num_workers() > 1) {
                              ASSERT_EQ(duplicates, std::vector<size_t>());
                          }
                          else {
                              ASSERT_EQ(duplicates, hashes);
                          }
                      };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
