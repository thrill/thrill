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
        : Super(ctx, label, { /* parent_ids */ }, { /* parents */ }) {
        // SourceNode are kept by default: they usually read files or databases
        // on PushData(), which should not be consumed.
        Super::consume_counter_ = Super::never_consume_;
    }

    //! SourceNodes generally do not Execute, they only PushData.
    void Execute() override { }

    //! SourceNodes generally do not do anything on Dispose, they only PushData.
    void Dispose() override { }

    //! Print error when trying to set consume to true.
    void IncConsumeCounter(size_t /* counter */) final {
        die("You cannot set a SourceNode to .Keep() or consume its data.");
    }
};

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_SOURCE_NODE_HEADER

/******************************************************************************/
