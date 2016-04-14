/*******************************************************************************
 * thrill/api/source_node.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_SOURCE_NODE_HEADER
#define THRILL_API_SOURCE_NODE_HEADER

#include <thrill/api/dia_node.hpp>

namespace thrill {
namespace api {

//! \ingroup api_layer
//! \{

template <typename ValueType>
class SourceNode : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;

    SourceNode(Context& ctx, const char* label)
        : Super(ctx, label, { /* parent_ids */ }, { /* parents */ })
    { }

    //! SourceNodes generally do not Execute, they only PushData.
    void Execute() override { }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SOURCE_NODE_HEADER

/******************************************************************************/
