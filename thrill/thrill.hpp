/*******************************************************************************
 * thrill/thrill.hpp
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Alexander Noe <aleexnoe@gmail.com>
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_THRILL_HEADER
#define THRILL_THRILL_HEADER

/*[[[perl
print "#include <$_>\n" foreach (sort <thrill/api/*.hpp>);
]]]*/
#include <thrill/api/action_node.hpp>
#include <thrill/api/allgather.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/dia_base.hpp>
#include <thrill/api/dia_node.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/distribute_from.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/api/function_stack.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/generate_from_file.hpp>
#include <thrill/api/groupby.hpp>
#include <thrill/api/groupby_index.hpp>
#include <thrill/api/groupby_iterator.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/reduce.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/api/stats_graph.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/write_lines_many.hpp>
#include <thrill/api/zip.hpp>
// [[[end]]]

namespace thrill {

// our public interface classes and methods go into this namespace. there are
// aliased in their respective header file. all others should be in a
// sub-namespace.

} // namespace thrill

#endif // !THRILL_THRILL_HEADER

/******************************************************************************/
