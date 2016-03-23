/*******************************************************************************
 * thrill/api/concat_to_dia.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_CONCAT_TO_DIA_HEADER
#define THRILL_API_CONCAT_TO_DIA_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/logger.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class ConcatToDIANode final : public SourceNode<ValueType>
{
public:
    using Super = SourceNode<ValueType>;
    using Super::context_;

    ConcatToDIANode(Context& ctx,
                    const std::vector<ValueType>& in_vector)
        : SourceNode<ValueType>(ctx, "ConcatToDIA"),
          in_vector_(in_vector)
    { }

    ConcatToDIANode(Context& ctx,
                    std::vector<ValueType>&& in_vector)
        : SourceNode<ValueType>(ctx, "ConcatToDIA"),
          in_vector_(std::move(in_vector))
    { }

    void PushData(bool /* consume */) final {
        for (size_t i = 0; i < in_vector_.size(); ++i) {
            this->PushItem(in_vector_[i]);
        }
    }

private:
    //! Vector pointer to read elements from.
    std::vector<ValueType> in_vector_;
};

/*!
 * ConcatToDIA is a Source-DOp, which takes a vector of data on all workers, and
 * CONCATENATES them into a DIA. Use Distribute to actually distribute data from
 * a single worker, ConcatToDIA is a wrapper if the data is already distributed.
 *
 * \param ctx Reference to the Context object
 *
 * \param in_vector Vector to concatenate into a DIA, the contents is COPIED
 * into the DIANode.
 *
 * \ingroup dia_sources
 */
template <typename ValueType>
auto ConcatToDIA(Context & ctx,
                 const std::vector<ValueType>&in_vector) {

    using ConcatToDIANode = api::ConcatToDIANode<ValueType>;

    return DIA<ValueType>(std::make_shared<ConcatToDIANode>(ctx, in_vector));
}

/*!
 * ConcatToDIA is a Source-DOp, which takes a vector of data on all workers, and
 * CONCATENATES them into a DIA. Use Distribute to actually distribute data from
 * a single worker, ConcatToDIA is a wrapper if the data is already distributed.
 *
 * \param ctx Reference to the Context object
 *
 * \param in_vector Vector to concatenate into a DIA, the contents is MOVED into
 * the DIANode.
 */
template <typename ValueType>
auto ConcatToDIA(Context & ctx,
                 std::vector<ValueType>&& in_vector) {

    using ConcatToDIANode = api::ConcatToDIANode<ValueType>;

    return DIA<ValueType>(
        std::make_shared<ConcatToDIANode>(ctx, std::move(in_vector)));
}

} // namespace api

//! imported from api namespace
using api::ConcatToDIA;

} // namespace thrill

#endif // !THRILL_API_CONCAT_TO_DIA_HEADER

/******************************************************************************/
