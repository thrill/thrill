/*******************************************************************************
 * examples/triangles/triangles.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Alexander Noe <aleexnoe@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_EXAMPLES_TRIANGLES_TRIANGLES_HEADER
#define THRILL_EXAMPLES_TRIANGLES_TRIANGLES_HEADER

#include <thrill/api/join.hpp>
#include <thrill/api/size.hpp>

using Node = size_t;
using Edge = std::pair<Node, Node>;

using namespace thrill; // NOLINT

namespace std { //i am sorry.

	template <> struct hash<Edge> {
		size_t operator()(const Edge& e) const {
			return hash<Node>()(e.first) ^ hash<Node>()(e.second);
		}
	};

} //namespace std

namespace examples {
namespace triangles {

template <typename Stack>
size_t CountTriangles(const DIA<Edge, Stack>& edges) {

	auto edges_length_2 = edges.InnerJoinWith(edges, [](const Edge& e) {
			return e.second;
		}, [](const Edge& e) {
			return e.first;
		}, [](const Edge& e1, const Edge& e2) {
			assert(e1.second == e2.first);
			return std::make_pair(e1.first, e2.second);
		});

	auto triangles = edges_length_2.InnerJoinWith(edges, [](const Edge& e) {
			return e;
		}, [](const Edge& e) {
			return e;
		}, [](const Edge& /*e1*/, const Edge& /*e2*/) {
			return (size_t)1;
		});

	return triangles.Size();
}

} // namespace triangles
} // namespace examples

#endif // !THRILL_EXAMPLES_TRIANGLES_TRIANGLES_HEADER

/******************************************************************************/
