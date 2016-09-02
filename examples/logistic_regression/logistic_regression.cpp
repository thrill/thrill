/*******************************************************************************
 * examples/logistic_regression/logistic_regression.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <array>
#include <random>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "logistic_regression.hpp"

using namespace thrill;                        // NOLINT
using namespace examples::logistic_regression; // NOLINT

// Dimensions of the data
constexpr size_t dim = 3;
using T = double;

using Element = std::array<T, dim>;
using DataObject = std::pair<bool, Element>;

#define LOGM LOGC(debug && ctx.my_rank() == 0)

template <typename Input>
static auto ReadInputFile(api::Context & ctx, const Input &input_path) {
    return ReadLines(ctx, input_path)
           .Map([](const std::string& line) {
                    // parse "value,dim_1,dim_2,...,dim_n" lines
                    char* endptr;
                    DataObject obj;
                    // yikes C stuff, TODO template
                    obj.first = common::from_cstr<T>(line.c_str(), &endptr);
                    die_unless(endptr && *endptr == ',' &&
                               "Could not parse input line");

                    for (size_t i = 0; i < dim; ++i) {
                        T value = common::from_cstr<T>(endptr + 1, &endptr);
                        die_unless(endptr &&
                                   ((i + 1 <= dim && *endptr == ',') ||
                                    (i + 1 == dim && *endptr == 0)) &&
                                   "Could not parse input line");
                        obj.second[i] = value;
                    }
                    return obj;
                })
           .Cache();
}

static auto GenerateInput(api::Context & ctx, size_t size) {

    std::default_random_engine rng(std::random_device { } ());
    std::normal_distribution<double> norm_dist(0.0, 1.0);
    std::lognormal_distribution<double> lognorm_dist(0.0, 1.0);

    return Generate(
        ctx,
        [&](size_t index) {
            bool c = (2 * index < size);

            // add noise to features
            Element p;
            p[0] = index * 0.1 + size / 100.0 * norm_dist(rng);
            p[1] = index * index * 0.1 + size / 100.0 * norm_dist(rng);
            p[2] = size - index * 0.1 + size / 100.0 * norm_dist(rng);

            return DataObject(c, p);
        }, size)
           // cache generated data, otherwise random generators are used again.
           .Cache();
}

static auto GenerateTestData(api::Context & ctx, size_t size) {

    std::default_random_engine rng(std::random_device { } ());
    std::normal_distribution<double> norm_dist(0.0, 1.0);
    std::lognormal_distribution<double> lognorm_dist(0.0, 1.0);

    return Generate(
        ctx,
        [size](size_t index) {
            bool c = (2 * index < size);

            // do not add noise to features
            Element p;
            p[0] = index * 0.1;
            p[1] = index * index * 0.1;
            p[2] = size - index * 0.1;

            return DataObject(c, p);
        }, size);
}

template <typename InputDIA>
auto TrainLogit(api::Context & ctx,
                const InputDIA &input_dia,
                size_t max_iterations, double gamma, double epsilon) {

    Element weights;
    double norm;
    size_t iterations;
    std::tie(weights, norm, iterations) =
        logit_train<T, dim>(input_dia, max_iterations, gamma, epsilon);

    LOGM << "Iterations: " << iterations;
    LOGM << "Norm: " << norm;
    LOGM << "Final weights (model):";
    for (size_t i = 0; i < dim; ++i) {
        LOGM << "Model[" << i << "] = " << weights[i];
    }
    return weights;
}

template <typename InputDIA>
void TestLogit(api::Context& ctx, const std::string& test_file,
               const InputDIA& input_dia, const Element& weights) {
    size_t num_trues, true_trues, num_falses, true_falses;
    std::tie(num_trues, true_trues, num_falses, true_falses)
        = logit_test<T, dim>(input_dia, weights);
    LOGM << "Evaluation result for " << test_file << ":";
    LOGM << "\tTrue:  " << true_trues << " of " << num_trues << " correct, "
         << num_trues - true_trues << " incorrect, "
         << static_cast<double>(true_trues) / num_trues * 100.0 << "% matched";
    LOGM << "\tFalse: " << true_falses << " of " << num_falses
         << " correct, " << num_falses - true_falses << " incorrect, "
         << static_cast<double>(true_falses) / num_falses * 100.0 << "% matched";
}

int main(int argc, char* argv[]) {
    common::CmdlineParser clp;

    std::string training_path;
    std::vector<std::string> test_path;
    clp.AddParamString("input", training_path, "training file pattern(s)");
    clp.AddParamStringlist("test", test_path, "test file pattern(s)");

    size_t max_iterations = 1000;
    clp.AddSizeT('n', "iterations", max_iterations,
                 "Maximum number of iterations, default: 1000");

    double gamma = 0.002, epsilon = 0.0001;
    clp.AddDouble('g', "gamma", gamma, "Gamma, default: 0.002");
    clp.AddDouble('e', "epsilon", epsilon, "Epsilon, default: 0.0001");

    bool generate = false;
    clp.AddFlag('G', "generate", generate,
                "Generate some random data to train and classify");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            // ctx.enable_consume();

            Element weights;

            if (generate) {
                size_t size = common::from_cstr<size_t>(training_path.c_str());
                weights = TrainLogit(ctx, GenerateInput(ctx, size),
                                     max_iterations, gamma, epsilon);

                TestLogit(ctx, "generated",
                          GenerateTestData(ctx, size / 10), weights);
            }
            else {
                weights = TrainLogit(ctx, ReadInputFile(ctx, training_path),
                                     max_iterations, gamma, epsilon);

                for (const auto& test_file : test_path) {
                    auto data = ReadInputFile(ctx, test_file);
                    TestLogit(ctx, test_file, data, weights);
                }
            }
        });
}

/******************************************************************************/
