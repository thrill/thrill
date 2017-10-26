/*******************************************************************************
 * examples/k-means/k-means_run.cpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Matthias Stumpp <mstumpp@gmail.com>
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#include <examples/k-means/k-means.hpp>

#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>
#include <tlx/cmdline_parser.hpp>

#include <algorithm>
#include <iomanip>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace examples::k_means; // NOLINT

//! Output a #rrggbb color for each cluster index
class SVGColor
{
public:
    explicit SVGColor(size_t cluster) : cluster_(cluster) { }
    size_t cluster_;
};

std::ostream& operator << (std::ostream& os, const SVGColor& c) {
    os << "#" << std::hex << std::setfill('0') << std::setw(2)
       << unsigned(static_cast<double>(3 * (c.cluster_ + 1) % 11) / 11.0 * 256)
       << unsigned(static_cast<double>(7 * (c.cluster_ + 1) % 11) / 11.0 * 256)
       << unsigned(static_cast<double>(9 * (c.cluster_ + 1) % 11) / 11.0 * 256);
    return os;
}

//! Output the points and centroids as a SVG drawing.
template <typename Point>
void OutputSVG(const std::string& svg_path, double svg_scale,
               const DIA<Point>& list,
               const KMeansModel<Point>& model) {
    tlx::unused(svg_path);
    tlx::unused(svg_scale);
    tlx::unused(list);
    tlx::unused(model);
}

//! Output the points and centroids as a 2-D SVG drawing
template <>
void OutputSVG(const std::string& svg_path, double svg_scale,
               const DIA<Point<2> >& point_dia,
               const KMeansModel<Point<2> >& model) {
    double width = 0, height = 0;

    using Point2D = Point<2>;

    const std::vector<Point2D>& centroids = model.centroids();
    std::vector<PointClusterId<Point2D> > list =
        model.ClassifyPairs(point_dia).Gather();

    for (const PointClusterId<Point2D>& p : list) {
        width = std::max(width, p.first.x[0]);
        height = std::max(height, p.first.x[1]);
    }

    if (point_dia.context().my_rank() != 0) return;

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

    for (const PointClusterId<Point2D>& p : list) {
        os << "    <circle r=\"1\" cx=\"" << p.first.x[0] * svg_scale
           << "\" cy=\"" << p.first.x[1] * svg_scale
           << "\" style=\"stroke:none;stroke-opacity:1;fill:"
           << SVGColor(p.second) << ";fill-opacity:1\" />\n";
    }
    for (size_t i = 0; i < centroids.size(); ++i) {
        const Point2D& p = centroids[i];
        os << "    <circle r=\"4\" cx=\"" << p.x[0] * svg_scale
           << "\" cy=\"" << p.x[1] * svg_scale
           << "\" style=\"stroke:black;stroke-opacity:1;fill:"
           << SVGColor(i) << ";fill-opacity:1\" />\n";
    }
    os << " </g>\n";
    os << "</svg>\n";
}

template <typename Point>
static void RunKMeansGenerated(
    thrill::Context& ctx, bool bisecting,
    size_t dimensions, size_t num_clusters, size_t iterations, double eps,
    const std::string& svg_path, double svg_scale,
    const std::vector<std::string>& input_paths) {

    std::default_random_engine rng(123456);
    std::uniform_real_distribution<float> dist(0.0, 1000.0);

    size_t num_points;
    if (input_paths.size() != 1 ||
        !thrill::common::from_str<size_t>(input_paths[0], num_points))
        die("For generated data, set input_path to the number of points.");

    auto points =
        Generate(
            ctx, num_points,
            [&](const size_t& /* index */) {
                return Point::Random(dimensions, dist, rng);
            })
        .Cache().KeepForever();

    auto result = bisecting ?
                  BisecKMeans(points.Keep(), dimensions, num_clusters, iterations, eps) :
                  KMeans(points.Keep(), dimensions, num_clusters, iterations, eps);

    double cost = result.ComputeCost(points);
    if (ctx.my_rank() == 0)
        LOG1 << "k-means cost: " << cost;

    if (svg_path.size() && dimensions == 2) {
        OutputSVG(svg_path, svg_scale, points, result);
    }
}

template <typename Point>
static void RunKMeansFile(
    thrill::Context& ctx, bool bisecting,
    size_t dimensions, size_t num_clusters, size_t iterations, double eps,
    const std::string& svg_path, double svg_scale,
    const std::vector<std::string>& input_paths) {

    auto points =
        ReadLines(ctx, input_paths).Map(
            [dimensions](const std::string& input) {
                // parse "<pt> <pt> <pt> ..." lines
                Point p = Point::Make(dimensions);
                char* endptr = const_cast<char*>(input.c_str());
                for (size_t i = 0; i < dimensions; ++i) {
                    while (*endptr == ' ') ++endptr;
                    p.x[i] = std::strtod(endptr, &endptr);
                    if (!endptr || (*endptr != ' ' && i != dimensions - 1)) {
                        die("Could not parse point coordinates: " << input);
                    }
                }
                while (*endptr == ' ') ++endptr;
                if (!endptr || *endptr != 0) {
                    die("Could not parse point coordinates: " << input);
                }
                return p;
            });

    auto result = bisecting ?
                  BisecKMeans(points.Keep(), dimensions, num_clusters, iterations, eps) :
                  KMeans(points.Keep(), dimensions, num_clusters, iterations, eps);

    double cost = result.ComputeCost(points.Keep());
    if (ctx.my_rank() == 0)
        LOG1 << "k-means cost: " << cost;

    if (svg_path.size() && dimensions == 2) {
        OutputSVG(svg_path, svg_scale, points.Collapse(), result);
    }
}

int main(int argc, char* argv[]) {

    tlx::CmdlineParser clp;

    bool generate = false;
    clp.add_bool('g', "generate", generate,
                 "generate random data, set input = #points");

    bool bisecting = false;
    clp.add_bool('b', "bisecting", bisecting,
                 "enable bisecting k-Means");

    size_t iterations = 10;
    clp.add_size_t('n', "iterations", iterations,
                   "iterations, default: 10");

    size_t dimensions = 2;
    clp.add_param_size_t("dim", dimensions,
                         "dimensions of points 2-10, default: 2");

    size_t num_clusters;
    clp.add_param_size_t("clusters", num_clusters, "Number of clusters");

    double epsilon = 0;
    clp.add_double('e', "epsilon", epsilon,
                   "centroid position delta for break condition, default: 0");

    std::string svg_path;
    clp.add_string('s', "svg", svg_path,
                   "output path for svg drawing (only for dim = 2)");

    double svg_scale = 1;
    clp.add_double('S', "svg-scale", svg_scale,
                   "scale coordinates for svg output, default: 1");

    std::vector<std::string> input_paths;
    clp.add_param_stringlist("input", input_paths,
                             "input file pattern(s)");

    if (!clp.process(argc, argv)) {
        return -1;
    }

    clp.print_result();

    auto start_func =
        [&](thrill::Context& ctx) {
            ctx.enable_consume();

            if (generate) {
                switch (dimensions) {
                case 0:
                    die("Zero dimensional clustering is easy.");
                case 2:
                    RunKMeansGenerated<Point<2> >(
                        ctx, bisecting, dimensions, num_clusters, iterations,
                        epsilon, svg_path, svg_scale, input_paths);
                    break;
                case 3:
                    RunKMeansGenerated<Point<3> >(
                        ctx, bisecting, dimensions, num_clusters, iterations,
                        epsilon, svg_path, svg_scale, input_paths);
                    break;
                default:
                    RunKMeansGenerated<VPoint>(
                        ctx, bisecting, dimensions, num_clusters, iterations,
                        epsilon, svg_path, svg_scale, input_paths);
                }
            }
            else {
                switch (dimensions) {
                case 0:
                    die("Zero dimensional clustering is easy.");
                case 2:
                    RunKMeansFile<Point<2> >(
                        ctx, bisecting, dimensions, num_clusters, iterations,
                        epsilon, svg_path, svg_scale, input_paths);
                    break;
                case 3:
                    RunKMeansFile<Point<3> >(
                        ctx, bisecting, dimensions, num_clusters, iterations,
                        epsilon, svg_path, svg_scale, input_paths);
                    break;
                default:
                    RunKMeansFile<VPoint>(
                        ctx, bisecting, dimensions, num_clusters, iterations,
                        epsilon, svg_path, svg_scale, input_paths);
                }
            }
        };

    return thrill::Run(start_func);
}

/******************************************************************************/
