/*******************************************************************************
 * examples/stochastic_gradient_descent/stochastic_gradient_descent_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2017 Alina Saalfeld <alina.saalfeld@ymail.com>
 * Copyright (C) 2017 Clemens Wallrath <clemens.wallrath@nerven.gift>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <thrill/api/cache.hpp>
#include <thrill/api/context.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_lines.hpp>

#include <tlx/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>

#include <algorithm>
#include <iomanip>
#include <string>
#include <vector>

#include "stochastic_gradient_descent.hpp"

using namespace examples::stochastic_gradient_descent; // NOLINT

//! Output the points and the fitted linear function as a 2-D SVG drawing
template <typename Vector>
void OutputSVG(const std::string& svg_path, double svg_scale,
               const DIA<DataPoint<Vector> >& point_dia,
               const Vector& model) {
    double width = 0, height = 0, min_vert = 0, max_vert = 0, min_hor = 0, max_hor = 0;

    std::vector<DataPoint<Vector> > list = point_dia.Gather();

    for (const DataPoint<Vector>& p : list) {
        min_hor = std::min(min_hor, p.data.x[0]);
        max_hor = std::max(max_hor, p.data.x[0]);
        min_vert = std::min(min_vert, p.label);
        max_vert = std::max(max_vert, p.label);
    }

    double weight = model.x[0];
    double y1 = min_hor * weight;
    double y2 = max_hor * weight;
    min_vert = std::min(min_vert, y1);
    min_vert = std::min(min_vert, y2);
    max_vert = std::max(max_vert, y1);
    max_vert = std::max(max_vert, y2);

    width = max_hor - min_hor;
    height = max_vert - min_vert;

    std::ofstream os(svg_path);

    os << "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n";
    os << "<svg\n";
    os << "   xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n";
    os << "   xmlns:cc=\"http://creativecommons.org/ns#\"\n";
    os << "   xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n";
    os << "   xmlns:svg=\"http://www.w3.org/2000/svg\"\n";
    os << "   xmlns=\"http://www.w3.org/2000/svg\"\n";
    os << "   version=\"1.1\" id=\"svg2\" width=\"" << width * svg_scale
       << "\" height=\"" << height * svg_scale << "\">\n";
    os << "  <g id=\"layer1\">\n";

    // Draw grid
    os << "    <line x1=\"0\" y1=\"" << (height + min_vert) * svg_scale
       << "\" x2=\"" << width * svg_scale << "\" y2=\"" << (height + min_vert) * svg_scale
       << "\" stroke-width=\"1\" stroke=\"#777777\" style=\"stroke-opacity:0.3\" />\n";
    os << "    <line x1=\"" << -min_hor * svg_scale << "\" y1=\"0\""
       << " x2=\"" << -min_hor * svg_scale << "\" y2=\"" << height * svg_scale
       << "\" stroke-width=\"1\" stroke=\"#777777\" style=\"stroke-opacity:0.3\" />\n";

    // Draw points
    for (const DataPoint<Vector>& p : list) {
        os << "    <circle r=\"1\" cx=\"" << (p.data.x[0] - min_hor) * svg_scale
           << "\" cy=\"" << (height - p.label + min_vert) * svg_scale
           << "\" style=\"stroke:none;stroke-opacity:1;fill:#45a2d1;fill-opacity:1\" />\n";
    }

    // Draw line
    os << "    <line x1=\"0\" y1=\"" << (height - y1 + min_vert) * svg_scale
       << "\" x2=\"" << width * svg_scale << "\" y2=\"" << (height - y2 + min_vert) * svg_scale
       << "\" stroke-width=\"1\" stroke=\"#ff9900\" />\n";

    os << " </g>\n";
    os << "</svg>\n";
}

template <typename Vector>
static void RunStochasticGradGenerated(
    thrill::Context& ctx, size_t dimensions, size_t iterations,
    size_t num_points, double mini_batch_fraction,
    double step_size, double tolerance,
    const std::string& svg_path, double svg_scale, size_t repetitions) {

    std::default_random_engine rng(2342);
    std::uniform_real_distribution<double> uni_dist(-100.0, 100.0);
    std::normal_distribution<double> norm_dist(1.0, 0.1);
    std::normal_distribution<double> weight_dist(1.0, 5);

    Vector weights = Vector::Random(dimensions, weight_dist, rng);
    if (ctx.my_rank() == 0)
        LOG1 << "Generated weights: " << weights;

    auto points =
        Generate(
            ctx, num_points,
            [&](const size_t& /* index */) {
                auto x = Vector::Random(dimensions, uni_dist, rng);
                auto y = weights.dot(x) * norm_dist(rng);
                return DataPoint<Vector>({ x, y });
            })
        .Cache().KeepForever().Execute();

    auto start = std::chrono::high_resolution_clock::now();

    Vector result;

    for (size_t r = 0; r < repetitions; r++) {
        auto grad_descent = StochasticGradientDescent<Vector>(
            iterations, mini_batch_fraction, step_size, tolerance);

        auto initial_weights = Vector::Make(dimensions).fill(1.0);
        result = grad_descent.optimize(points, initial_weights);
    }

    auto end = std::chrono::high_resolution_clock::now();
    if (ctx.my_rank() == 0) {
        LOG1 << "Estimated weights: " << result;
        LOG1 << "Computation time: " << (std::chrono::duration_cast<std::chrono::duration<double> >(end - start)).count() / repetitions << "s";
    }

    if (svg_path.size() && dimensions == 1) {
        OutputSVG(svg_path, svg_scale, points.Collapse(), result);
    }
}

template <typename Vector>
static void RunStochasticGradFile(
    thrill::Context& ctx, size_t dimensions, size_t iterations,
    double mini_batch_fraction, double step_size, double tolerance,
    const std::string& svg_path, double svg_scale,
    const std::string& input_path, size_t repetitions) {

    auto points =
        ReadLines(ctx, input_path)
        .Filter(
            [](const std::string& input) {
                // filter empty lines and comments
                return (!input.empty() && input.at(0) != '#');
            })
        .Map(
            [dimensions](const std::string& input) {
                // parse "<pt> <pt> <pt> ... <lbl>" lines
                Vector v = Vector::Make(dimensions);
                double l;
                char* endptr = const_cast<char*>(input.c_str());
                for (size_t i = 0; i < dimensions; ++i) {
                    while (*endptr == ' ') ++endptr;
                    v.x[i] = std::strtod(endptr, &endptr);
                    if (!endptr || *endptr != ' ') {
                        die("Could not parse point coordinates: " << input);
                    }
                }
                while (*endptr == ' ') ++endptr;
                l = std::strtod(endptr, &endptr);
                if (!endptr) {
                    die("Could not parse point coordinates: " << input);
                }
                while (*endptr == ' ') ++endptr;
                if (!endptr || *endptr != 0) {
                    die("Could not parse point coordinates: " << input);
                }
                return DataPoint<Vector>({ v, l });
            })
        .Cache().KeepForever().Execute();

    auto start = std::chrono::high_resolution_clock::now();

    Vector result;

    for (size_t r = 0; r < repetitions; r++) {
        auto grad_descent = StochasticGradientDescent<Vector>(
            iterations, mini_batch_fraction, step_size, tolerance);

        auto initial_weights = Vector::Make(dimensions).fill(1.0);
        result = grad_descent.optimize(points, initial_weights);
    }

    auto end = std::chrono::high_resolution_clock::now();

    if (ctx.my_rank() == 0) {
        LOG1 << "Estimated weights: " << result;
        LOG1 << "Computation time: " << (std::chrono::duration_cast<std::chrono::duration<double> >(end - start)).count() / repetitions << "s";
    }

    if (svg_path.size() && dimensions == 1) {
        OutputSVG(svg_path, svg_scale, points.Collapse(), result);
    }
}

int main(int argc, char* argv[]) {

    tlx::CmdlineParser cp;

    bool generate = false;
    cp.add_flag('g', "generate", generate,
                "generate random data, set num = #points");

    size_t num = 100;
    cp.add_size_t('n', "num", num,
                 "number of points to generate");

    size_t dimensions = 1;
    cp.add_size_t('d', "dim", dimensions,
                 "dimensions of weights 1-10, default: 1");

    size_t iterations = 100;
    cp.add_size_t('i', "iterations", iterations,
                 "iterations, default: 100");

    size_t repetitions = 1;
    cp.add_size_t('r', "repetitions", repetitions,
                 "repetitions, for timing purpose only.");

    double mini_batch_fraction = 1;
    cp.add_double('f', "frac", mini_batch_fraction,
                  "mini_batch_fraction, default: 1");

    double step_size = 0.001;
    cp.add_double('s', "step", step_size,
                  "stepsize, default: 0.001");

    double tolerance = 0.01;
    cp.add_double('t', "tolerance", tolerance,
                  "tolerance, default: 0.01");

    std::string input_path = "";
    cp.add_string('p', "paths", input_path,
                  "input file");

    std::string svg_path = "";
    cp.add_string('o', "output", svg_path,
                  "output path for svg drawing (only for dim = 2)");

    double svg_scale = 1;
    cp.add_double('S', "svg-scale", svg_scale,
                  "scale coordinates for svg output, default: 1");

    if (!cp.process(argc, argv)) {
        return -1;
    }

    cp.print_result();

    if (!generate && input_path == "") {
        die("Please use -g to generate input data or -p to load files");
    }

    auto start_func =
        [&](thrill::Context& ctx) {
            ctx.enable_consume();
            if (generate) {
                switch (dimensions) {
                case 0:
                    die("Zero dimensional gradient descent doesn't seem very useful.");
                    break;
                case 1:
                    RunStochasticGradGenerated<Vector<1> >(ctx, dimensions, iterations, num, mini_batch_fraction, step_size, tolerance, svg_path, svg_scale, repetitions);
                    break;
                case 2:
                    RunStochasticGradGenerated<Vector<2> >(ctx, dimensions, iterations, num, mini_batch_fraction, step_size, tolerance, svg_path, svg_scale, repetitions);
                    break;
                default:
                    RunStochasticGradGenerated<VVector>(ctx, dimensions, iterations, num, mini_batch_fraction, step_size, tolerance, svg_path, svg_scale, repetitions);
                    break;
                }
            }
            else {
                switch (dimensions) {
                case 0:
                    die("Zero dimensional gradient descent doesn't seem very useful.");
                    break;
                case 1:
                    RunStochasticGradFile<Vector<1> >(ctx, dimensions, iterations, mini_batch_fraction, step_size, tolerance, svg_path, svg_scale, input_path, repetitions);
                    break;
                case 2:
                    RunStochasticGradFile<Vector<2> >(ctx, dimensions, iterations, mini_batch_fraction, step_size, tolerance, svg_path, svg_scale, input_path, repetitions);
                    break;
                default:
                    RunStochasticGradFile<VVector>(ctx, dimensions, iterations, mini_batch_fraction, step_size, tolerance, svg_path, svg_scale, input_path, repetitions);
                    break;
                }
            }
        };
    return thrill::Run(start_func);
}

/******************************************************************************/
