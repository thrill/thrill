/*******************************************************************************
 * thrill/thrill.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
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
print "#include <$_>\n" foreach sort glob("thrill/api/"."*.hpp");
]]]*/
#include <thrill/api/action_node.hpp>
#include <thrill/api/all_gather.hpp>
#include <thrill/api/all_reduce.hpp>
#include <thrill/api/bernoulli_sample.hpp>
#include <thrill/api/cache.hpp>
#include <thrill/api/collapse.hpp>
#include <thrill/api/concat.hpp>
#include <thrill/api/concat_to_dia.hpp>
#include <thrill/api/context.hpp>
#include <thrill/api/dia.hpp>
#include <thrill/api/dia_base.hpp>
#include <thrill/api/dia_node.hpp>
#include <thrill/api/distribute.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/api/equal_to_dia.hpp>
#include <thrill/api/gather.hpp>
#include <thrill/api/generate.hpp>
#include <thrill/api/group_by_iterator.hpp>
#include <thrill/api/group_by_key.hpp>
#include <thrill/api/group_to_index.hpp>
#include <thrill/api/inner_join.hpp>
#include <thrill/api/max.hpp>
#include <thrill/api/merge.hpp>
#include <thrill/api/min.hpp>
#include <thrill/api/prefixsum.hpp>
#include <thrill/api/print.hpp>
#include <thrill/api/read_binary.hpp>
#include <thrill/api/read_lines.hpp>
#include <thrill/api/rebalance.hpp>
#include <thrill/api/reduce_by_key.hpp>
#include <thrill/api/reduce_to_index.hpp>
#include <thrill/api/sample.hpp>
#include <thrill/api/size.hpp>
#include <thrill/api/sort.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/api/sum.hpp>
#include <thrill/api/union.hpp>
#include <thrill/api/window.hpp>
#include <thrill/api/write_binary.hpp>
#include <thrill/api/write_lines.hpp>
#include <thrill/api/write_lines_one.hpp>
#include <thrill/api/zip.hpp>
#include <thrill/api/zip_window.hpp>
#include <thrill/api/zip_with_index.hpp>
// [[[end]]]

namespace thrill {

// our public interface classes and methods go into this namespace. there are
// aliased in their respective header file. all others should be in a
// sub-namespace.

} // namespace thrill

#endif // !THRILL_THRILL_HEADER

/******************************************************************************/
