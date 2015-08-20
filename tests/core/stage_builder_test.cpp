/*******************************************************************************
 * tests/core/stage_builder_test.cpp
 *
 * Part of Project c7a.
 *
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#include <thrill/c7a.hpp>
#include <gtest/gtest.h>

#include <algorithm>
#include <random>
#include <string>
#include <vector>

using namespace c7a;
using c7a::DIARef;
using c7a::Context;

TEST(Stage, CountReferencesSimple) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return static_cast<int>(index) + 1;
                },
                16);

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
            auto reduced = quadruples.ReduceBy(modulo_two, add_function);

            // Trigger execution
            std::vector<int> out_vec = reduced.AllGather();

            // 3x DIA reference + 1x child reference
            ASSERT_EQ(integers.node_refcount(), 4u);
            ASSERT_EQ(doubles.node_refcount(), 4u);
            ASSERT_EQ(quadruples.node_refcount(), 4u);
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(reduced.node_refcount(), 1u);
        };

    api::RunLocalTests(start_func);
}

TEST(Stage, CountReferencesLOpNode) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return static_cast<int>(index) + 1;
                },
                16);

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
            DIARef<int> quadruples = integers.FlatMap(duplicate_elements).Cache();

            // Create new child reference to LOpNode
            auto reduced = quadruples.ReduceBy(modulo_two, add_function);

            // Trigger execution
            std::vector<int> out_vec = reduced.AllGather();

            // 2x DIA reference + 1x child reference
            ASSERT_EQ(integers.node_refcount(), 3u);
            ASSERT_EQ(doubles.node_refcount(), 3u);
            // 1x DIA reference + 1x child reference
            ASSERT_EQ(quadruples.node_refcount(), 2u);
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(reduced.node_refcount(), 1u);
        };

    api::RunLocalTests(start_func);
}

TEST(Stage, OverwriteReferenceLOpNode) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return static_cast<int>(index) + 1;
                },
                16);

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
            DIARef<int> quadruples = integers.FlatMap(duplicate_elements).Cache();

            // Overwrite reference to LOpNode
            quadruples = quadruples.ReduceBy(modulo_two, add_function).Cache();

            // Trigger execution
            std::vector<int> out_vec = quadruples.AllGather();

            // 2x DIA reference + 1x child reference
            ASSERT_EQ(integers.node_refcount(), 3u);
            ASSERT_EQ(doubles.node_refcount(), 3u);
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(quadruples.node_refcount(), 1u);
        };

    api::RunLocalTests(start_func);
}

TEST(Stage, AdditionalChildReferences) {

    std::function<void(Context&)> start_func =
        [](Context& ctx) {

            auto integers = Generate(
                ctx,
                [](const size_t& index) {
                    return static_cast<int>(index) + 1;
                },
                16);

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
            DIARef<int> quadruples = integers.FlatMap(duplicate_elements).Cache();

            // Create a child reference to LOpNode
            DIARef<int> octuples = quadruples.ReduceBy(modulo_two, add_function).Cache();
            // Create a child reference to LOpNode
            DIARef<int> octuples_second = quadruples.ReduceBy(modulo_two, add_function).Cache();

            // Trigger execution
            std::vector<int> out_vec = octuples.AllGather();

            // 2x DIA reference + 1x child reference
            ASSERT_EQ(integers.node_refcount(), 3u);
            ASSERT_EQ(doubles.node_refcount(), 3u);
            // 1x DIA reference + 2x child reference
            ASSERT_EQ(quadruples.node_refcount(), 3u);
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(octuples.node_refcount(), 1u);
            // 1x DIA reference + 0x child reference
            ASSERT_EQ(octuples_second.node_refcount(), 1u);
        };

    api::RunLocalTests(start_func);
}

/******************************************************************************/
