/*******************************************************************************
 * tests/api/stage_builder_test.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <gtest/gtest.h>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/reduce_by_key.hpp>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

using namespace thrill; // NOLINT

TEST(Stage, CountReferencesSimple) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx, 16,
                [](const size_t& index) {
                    return static_cast<int>(index) + 1;
                });

            auto duplicate_elements = [](int in, auto emit) {
                                          emit(in);
                                          emit(in);
                                      };

            auto modulo_two = [](int in) {
                                  return (in % 2);
                              };

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            // Create two new DIA references to Generate
            auto doubles = integers.FlatMap(duplicate_elements);
            auto quadruples = integers.FlatMap(duplicate_elements);

            // Create new child reference to Generate
            auto reduced = quadruples.ReduceByKey(
                VolatileKeyTag, modulo_two, add_function);

            // Trigger execution
            std::vector<int> out_vec = reduced.AllGather();

            // 3x DIA reference + 1x child reference
            ASSERT_EQ(3u, integers.node_refcount());
            ASSERT_EQ(3u, doubles.node_refcount());
            ASSERT_EQ(3u, quadruples.node_refcount());
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(1u, reduced.node_refcount());
        };

    api::RunLocalTests(start_func);
}

TEST(Stage, CountReferencesLOpNode) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx, 16,
                [](const size_t& index) {
                    return static_cast<int>(index) + 1;
                });

            auto duplicate_elements = [](int in, auto emit) {
                                          emit(in);
                                          emit(in);
                                      };

            auto modulo_two = [](int in) {
                                  return (in % 2);
                              };

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            // Create a new DIA references to Generate
            auto doubles = integers.FlatMap(duplicate_elements);

            // Create a child references to Generate
            // Create a new DIA reference to LOpNode
            DIA<int> quadruples = integers.FlatMap(duplicate_elements).Cache();

            // Create new child reference to LOpNode
            auto reduced = quadruples.ReduceByKey(
                VolatileKeyTag, modulo_two, add_function);

            // Trigger execution
            std::vector<int> out_vec = reduced.AllGather();

            // 2x DIA reference + 1x child reference
            ASSERT_EQ(2u, integers.node_refcount());
            ASSERT_EQ(2u, doubles.node_refcount());
            // 1x DIA reference + 1x child reference
            ASSERT_EQ(1u, quadruples.node_refcount());
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(1u, reduced.node_refcount());
        };

    api::RunLocalTests(start_func);
}

TEST(Stage, OverwriteReferenceLOpNode) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx, 16,
                [](const size_t& index) {
                    return static_cast<int>(index) + 1;
                });

            auto duplicate_elements = [](int in, auto emit) {
                                          emit(in);
                                          emit(in);
                                      };

            auto modulo_two = [](int in) {
                                  return (in % 2);
                              };

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            // Create a new DIA references to Generate
            auto doubles = integers.FlatMap(duplicate_elements);

            // Create a child references to Generate
            // Create a new DIA reference to LOpNode
            DIA<int> quadruples = integers.FlatMap(duplicate_elements).Cache();

            // Overwrite reference to LOpNode
            quadruples = quadruples.ReduceByKey(
                VolatileKeyTag, modulo_two, add_function).Cache();

            // Trigger execution
            std::vector<int> out_vec = quadruples.AllGather();

            // 2x DIA reference + 1x child reference
            ASSERT_EQ(2u, integers.node_refcount());
            ASSERT_EQ(2u, doubles.node_refcount());
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(1u, quadruples.node_refcount());
        };

    api::RunLocalTests(start_func);
}

TEST(Stage, AdditionalChildReferences) {

    auto start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx, 16,
                [](const size_t& index) {
                    return static_cast<int>(index) + 1;
                });

            auto duplicate_elements = [](int in, auto emit) {
                                          emit(in);
                                          emit(in);
                                      };

            auto modulo_two = [](int in) {
                                  return (in % 2);
                              };

            auto add_function = [](int in1, int in2) {
                                    return in1 + in2;
                                };

            // Create a new DIA references to Generate
            auto doubles = integers.FlatMap(duplicate_elements);

            // Create a child references to Generate
            // Create a new DIA reference to LOpNode
            DIA<int> quadruples = integers.FlatMap(duplicate_elements).Cache();

            // Create a child reference to LOpNode
            DIA<int> octuples = quadruples.ReduceByKey(
                VolatileKeyTag, modulo_two, add_function).Cache();
            // Create a child reference to LOpNode
            DIA<int> octuples_second = quadruples.ReduceByKey(
                VolatileKeyTag, modulo_two, add_function).Cache();

            // Trigger execution
            std::vector<int> out_vec = octuples.AllGather();

            // 2x DIA reference + 1x child reference
            ASSERT_EQ(2u, integers.node_refcount());
            ASSERT_EQ(2u, doubles.node_refcount());
            // 1x DIA reference + 2x child reference
            ASSERT_EQ(1u, quadruples.node_refcount());
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(1u, octuples.node_refcount());
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(1u, octuples_second.node_refcount());
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
