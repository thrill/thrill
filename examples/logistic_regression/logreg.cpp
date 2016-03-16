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
#include <thrill/api/generate.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

#include <algorithm>
#include <string>
#include <utility>

#include "logreg.hpp"
#include "strtonumber.hpp"

using namespace thrill;               // NOLINT
using namespace examples::logistic_regression;  // NOLINT

// Dimensions of the data
constexpr size_t dim = 3;
using T = double;

#define LOGM LOGC(debug && ctx.my_rank() == 0)


static void RunLogReg(api::Context& ctx,
                      const std::vector<std::string> &input_path,
                      size_t max_iterations, double gamma, double epsilon) {
    using Element = std::array<T, dim>;
    using DataObject = std::pair<T, Element>;

    auto input =
        ReadLines(ctx, input_path)
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
                               ((i+1 < dim && *endptr == ',') ||
                                (i+1 ==dim && *endptr == 0)) &&
                               "Could not parse input line");
                    obj.second[i] = value;
                }
                return obj;
            });

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
}

int main(int argc, char* argv[]) {
    common::CmdlineParser clp;

    std::vector<std::string> input_path;
    clp.AddParamStringlist("input", input_path,
                           "input file pattern(s)");

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
            RunLogReg(ctx, input_path, max_iterations, gamma, epsilon);
        });
}

/******************************************************************************/
