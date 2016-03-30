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
#include <thrill/common/cmdline_parser.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>

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
void OutputSVG(std::ostream& os,
               const std::vector<PointClusterId<Point> >& list,
               const std::vector<Point>& centroids) {
    thrill::common::THRILL_UNUSED(os);
    thrill::common::THRILL_UNUSED(list);
    thrill::common::THRILL_UNUSED(centroids);
}

//! Output the points and centroids as a 2-D SVG drawing
template <>
void OutputSVG(std::ostream& os,
               const std::vector<PointClusterId<Point<2> > >& list,
               const std::vector<Point<2> >& centroids) {
    double width = 0, height = 0;
    double shrink = 200;

    using Point2D = Point<2>;

    for (const PointClusterId<Point2D>& p : list) {
        width = std::max(width, p.first.x[0]);
        height = std::max(height, p.first.x[1]);
    }

    os << "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n";
    os << "<svg\n";
    os << "   xmlns:dc=\"http://purl.org/dc/elements/1.1/\"\n";
    os << "   xmlns:cc=\"http://creativecommons.org/ns#\"\n";
    os << "   xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n";
    os << "   xmlns:svg=\"http://www.w3.org/2000/svg\"\n";
    os << "   xmlns=\"http://www.w3.org/2000/svg\"\n";
    os << "   version=\"1.1\" id=\"svg2\" width=\"" << width / shrink
       << "\" height=\"" << height / shrink << "\">\n";
    os << "  <g id=\"layer1\">\n";

    for (const PointClusterId<Point2D>& p : list) {
        os << "    <circle r=\"1\" cx=\"" << p.first.x[0] / shrink
           << "\" cy=\"" << p.first.x[1] / shrink
           << "\" style=\"stroke:none;stroke-opacity:1;fill:"
           << SVGColor(p.second) << ";fill-opacity:1\" />\n";
    }
    for (size_t i = 0; i < centroids.size(); ++i) {
        const Point2D& p = centroids[i];
        os << "    <circle r=\"4\" cx=\"" << p.x[0] / shrink
           << "\" cy=\"" << p.x[1] / shrink
           << "\" style=\"stroke:black;stroke-opacity:1;fill:"
           << SVGColor(i) << ";fill-opacity:1\" />\n";
    }
    os << " </g>\n";
    os << "</svg>\n";
}

template <typename Point>
static void RunKMeansGenerated(
    thrill::Context& ctx,
    size_t dimensions,
    size_t num_clusters, size_t iterations, const std::string& svg_path,
    const std::vector<std::string>& input_paths) {

    std::default_random_engine rng(std::random_device { } ());
    std::uniform_real_distribution<float> dist(0.0, 100000.0);

    size_t num_points;
    if (input_paths.size() != 1 ||
        !thrill::common::from_str<size_t>(input_paths[0], num_points))
        die("For generated data, set input_path to the number of points.");

    auto points = Generate(
        ctx, [&](const size_t& /* index */) {
            return Point::Random(dimensions, dist, rng);
        }, num_points);

    DIA<Point> centroids_dia = Generate(
        ctx, [&](const size_t& /* index */) {
            return Point::Random(dimensions, dist, rng);
        }, num_clusters);

    auto result = KMeans(points, centroids_dia, iterations);

    std::vector<PointClusterId<Point> > plist = result.Gather();
    std::vector<Point> centroids = centroids_dia.Gather();

    if (ctx.my_rank() == 0 && svg_path.size() && dimensions == 2) {
        std::ofstream os(svg_path);
        OutputSVG(os, plist, centroids);
    }
}

int main(int argc, char* argv[]) {

    thrill::common::CmdlineParser clp;

    bool generate = false;
    clp.AddFlag('g', "generate", generate,
                "generate random data, set input = #points");

    size_t iterations = 10;
    clp.AddSizeT('n', "iterations", iterations,
                 "iterations, default: 10");

    size_t dimensions = 2;
    clp.AddParamSizeT("dim", dimensions,
                      "dimensions of points 2-10, default: 2");

    size_t num_clusters;
    clp.AddParamSizeT("clusters", num_clusters, "Number of clusters");

    std::string svg_path;
    clp.AddString('s', "svg", svg_path,
                  "output path for svg drawing (only for dim = 2)");

    std::vector<std::string> input_paths;
    clp.AddParamStringlist("input", input_paths,
                           "input file pattern(s)");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func =
        [&](thrill::Context& ctx) {
            ctx.enable_consume();

            if (generate) {
#define RunKMeans(D, P)                                                    \
case D:                                                                    \
    RunKMeansGenerated<P>(                                                 \
        ctx, dimensions, num_clusters, iterations, svg_path, input_paths); \
    break;
                switch (dimensions) {
                    RunKMeans(2, Point<2>);
                    RunKMeans(3, Point<3>);
                    RunKMeans(4, Point<4>);
                case 0:
                default:
                    RunKMeansGenerated<VPoint>(
                        ctx, dimensions, num_clusters,
                        iterations, svg_path, input_paths);
                }
            }
            else { }
        };

    return thrill::Run(start_func);
}

/******************************************************************************/
