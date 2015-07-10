/*******************************************************************************
 * c7a/c7a.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chuck Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_C7A_HEADER
#define C7A_C7A_HEADER

#include <c7a/api/dia.hpp>

#include <c7a/api/allgather_node.hpp>
#include <c7a/api/generate_file_node.hpp>
#include <c7a/api/generate_node.hpp>
#include <c7a/api/prefixsum_node.hpp>
#include <c7a/api/read_node.hpp>
#include <c7a/api/reduce_node.hpp>
#include <c7a/api/reduce_to_index_node.hpp>
#include <c7a/api/sum_node.hpp>
#include <c7a/api/write_node.hpp>
#include <c7a/api/zip_node.hpp>

namespace c7a {

// our public interface classes and methods. all others should be in a
// sub-namespace.

using api::DIARef;
using api::Context;

} // namespace c7a

#endif // !C7A_C7A_HEADER

/******************************************************************************/
