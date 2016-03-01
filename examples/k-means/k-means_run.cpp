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

#include <algorithm>
#include <random>
#include <string>
#include <utility>
#include <vector>

using namespace thrill;            // NOLINT
using namespace examples::k_means; // NOLINT

using Point2D = Point<2>;

//! Output a #rrggbb color for each cluster index
class SVGColor
{
public:
    explicit SVGColor(size_t cluster) : cluster_(cluster) { }

    friend std::ostream& operator << (std::ostream& os, const SVGColor& c) {
        return os << "#" << std::hex << std::setfill('0') << std::setw(2)
                  << unsigned((3 * (c.cluster_ + 1) % 11) / 11.0 * 256.0)
                  << unsigned((7 * (c.cluster_ + 1) % 11) / 11.0 * 256.0)
                  << unsigned((9 * (c.cluster_ + 1) % 11) / 11.0 * 256.0);
    }
    size_t cluster_;
};

//! Output the points and centroids as a SVG drawing
void OutputSVG(std::ostream& os,
               const std::vector<PointClusterId<2> >& list,
               const std::vector<Point2D>& centroids) {
    double width = 0, height = 0;
    double shrink = 200;

    for (const PointClusterId<2>& p : list) {
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

    for (const PointClusterId<2>& p : list) {
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

int main(int argc, char* argv[]) {

    common::CmdlineParser clp;

    bool generate = false;
    clp.AddFlag('g', "generate", generate,
                "generate random data, set input = #points");

    size_t iter = 10;
    clp.AddSizeT('n', "iterations", iter, "PageRank iterations, default: 10");

    int num_points;
    clp.AddParamInt("points", num_points, "number of points");

    int num_clusters;
    clp.AddParamInt("clusters", num_clusters, "Number of clusters");

    std::string svg_path;
    clp.AddString('s', "svg", svg_path, "output svg drawing");

    if (!clp.Process(argc, argv)) {
        return -1;
    }

    clp.PrintResult();

    auto start_func =
        [&](api::Context& ctx) {
            ctx.enable_consume();

            std::default_random_engine rng(std::random_device { } ());
            std::uniform_real_distribution<float> dist(0.0, 100000.0);

            auto points = Generate(
                ctx, [&](const size_t& /* index */) {
                    return Point2D {
                        { dist(rng), dist(rng) }
                    };
                }, num_points);

            DIA<Point2D> centroids_dia = Generate(
                ctx, [&](const size_t& /* index */) {
                    return Point2D {
                        { dist(rng), dist(rng) }
                    };
                }, num_clusters);

            auto result = KMeans(points, centroids_dia, iter);

            std::vector<PointClusterId<2> > plist = result.Gather();
            std::vector<Point2D> centroids = centroids_dia.Gather();

            if (ctx.my_rank() == 0 && svg_path.size()) {
                std::ofstream os(svg_path);
                OutputSVG(os, plist, centroids);
            }
        };

    return api::Run(start_func);
}

/******************************************************************************/
