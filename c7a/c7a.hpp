/*******************************************************************************
 * c7a/c7a.hpp
 *
 * Part of Project c7a.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef C7A_C7A_HEADER
#define C7A_C7A_HEADER

#include <c7a/api/dia.hpp>
#include <c7a/api/bootstrap.hpp>

#include <c7a/api/allgather.hpp>
#include <c7a/api/generate_from_file.hpp>
#include <c7a/api/generate.hpp>
#include <c7a/api/prefixsum.hpp>
#include <c7a/api/read.hpp>
#include <c7a/api/reduce.hpp>
#include <c7a/api/reduce_to_index.hpp>
#include <c7a/api/sum.hpp>
#include <c7a/api/sort.hpp>
#include <c7a/api/write.hpp>
#include <c7a/api/zip.hpp>
#include <c7a/api/size.hpp>

namespace c7a {

// our public interface classes and methods. all others should be in a
// sub-namespace.

using api::DIARef;
using api::Context;

} // namespace c7a

#endif // !C7A_C7A_HEADER

/******************************************************************************/
