/*******************************************************************************
 * thrill/api/reduce_config.hpp
 *
 * Common declarations for ReduceNode and ReduceToIndexNode.
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_REDUCE_CONFIG_HEADER
#define THRILL_API_REDUCE_CONFIG_HEADER

#include <thrill/core/reduce_table.hpp>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

//! template to take a ReduceConfig and split the memory in half between the pre
//! and post stages.
template <typename ReduceConfig>
class ReduceConfigEqualSplit
{
public:
    //! derive config class from the base, but cut memory limit in half.
    class EqualSplit : public ReduceConfig
    {
    public:
        size_t limit_memory_bytes = ReduceConfig::limit_memory_bytes / 2;
    };

    EqualSplit pre_table;
    EqualSplit post_table;
};

//! default configuration for ReduceNode and ReduceToIndexNode
class DefaultReduceConfig
    : public ReduceConfigEqualSplit<core::DefaultReduceTableConfig>
{ };

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_REDUCE_CONFIG_HEADER

/******************************************************************************/
