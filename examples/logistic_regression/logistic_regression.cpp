/*******************************************************************************
 * examples/logistic_regression/logreg.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Lorenz Hübschle-Schneider <lorenz@4z2.de>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <array>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "logistic_regression.hpp"
#include "strtonumber.hpp"

using namespace thrill;               // NOLINT
using namespace examples::logistic_regression;  // NOLINT

// Dimensions of the data
constexpr size_t dim = 3;
using T = double;

using Element = std::array<T, dim>;
using DataObject = std::pair<T, Element>;

#define LOGM LOGC(debug && ctx.my_rank() == 0)

template <typename Input>
static auto ReadFile(api::Context &ctx, const Input &input_path) {
    return ReadLines(ctx, input_path)
        .Map([](const std::string& line) {
                // parse "value,dim_1,dim_2,...,dim_n" lines
                char* endptr;
                DataObject obj;
                // yikes C stuff, TODO template
                obj.first = strtonumber<T>(line.c_str(), &endptr);
                die_unless(endptr && *endptr == ',' &&
                           "Could not parse input line");

                for (size_t i = 0; i < dim; ++i) {
                    T value = strtonumber<T>(endptr + 1, &endptr);
                    die_unless(endptr &&
                               ((i+1 <=dim && *endptr == ',') ||
                                (i+1 ==dim && *endptr == 0)) &&
                               "Could not parse input line");
                    obj.second[i] = value;
                }
                return obj;
            })
        .Cache();
}


static void RunLogReg(api::Context& ctx,
                      const std::string &training_path,
                      const std::vector<std::string> &test_path,
                      size_t max_iterations, double gamma, double epsilon) {
    auto input = ReadFile(ctx, training_path);

    Element weights;
    double norm;
    size_t iterations;
    std::tie(weights, norm, iterations) =
        train_logreg<T, dim>(input, max_iterations, gamma, epsilon);

    LOGM << "Iterations: " << iterations;
    LOGM << "Norm: " << norm;
    LOGM << "Final weights (model):";
    for (size_t i = 0; i < dim; ++i) {
        LOGM << "Model[" << i << "] = " << weights[i];
    }

    for (const auto &test_file : test_path) {
        auto data = ReadFile(ctx, test_file);
        size_t num_trues, true_trues, num_falses, true_falses;
        std::tie(num_trues, true_trues, num_falses, true_falses)
            = test_logreg<T, dim>(data, weights);
        LOGM << "Evaluation result for " << test_file << ":";
        LOGM << "\tTrue:  " << true_trues << " of " << num_trues << " correct, "
             << num_trues - true_trues  << " incorrect ("
             << static_cast<double>(true_trues)/num_trues * 100.0 << "%)";
        LOGM << "\tFalse: " << true_falses << " of " << num_falses
             << " correct, " << num_falses - true_falses << " incorrect ("
             << static_cast<double>(true_falses)/num_falses * 100.0 << "%)";
    }
}

int main(int argc, char* argv[]) {
    common::CmdlineParser clp;

    std::string training_path;
    std::vector<std::string> test_path;
    clp.AddParamString("input", training_path, "training file pattern(s)");
    clp.AddParamStringlist("test", test_path, "test file pattern(s)");

    size_t max_iterations(1000);
    clp.AddSizeT('n', "iterations", max_iterations, "Maximum number of iterations, default: 1000");

    double gamma(0.002), epsilon(0.0001);
    clp.AddDouble('g', "gamma", gamma, "Gamma, default: 0.002");
    clp.AddDouble('e', "epsilon", epsilon, "Epsilon, default: 0.0001");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    return api::Run(
        [&](api::Context& ctx) {
            RunLogReg(ctx, training_path, test_path,
                      max_iterations, gamma, epsilon);
        });
}

/******************************************************************************/
