/*******************************************************************************
 * thrill/api/equal_to_dia.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_EQUAL_TO_DIA_HEADER
#define THRILL_API_EQUAL_TO_DIA_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/source_node.hpp>
#include <thrill/common/logger.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

template <typename ValueType>
class EqualToDIANode final : public SourceNode<ValueType>
{
public:
    using Super = SourceNode<ValueType>;
    using Super::context_;

    EqualToDIANode(Context& ctx,
                   const std::vector<ValueType>& in_vector)
        : SourceNode<ValueType>(ctx, "EqualToDIA"),
          in_vector_(in_vector)
    { }

    EqualToDIANode(Context& ctx,
                   std::vector<ValueType>&& in_vector)
        : SourceNode<ValueType>(ctx, "EqualToDIA"),
          in_vector_(std::move(in_vector))
    { }

    void PushData(bool /* consume */) final {
        common::Range local = context_.CalculateLocalRange(in_vector_.size());

        for (size_t i = local.begin; i < local.end; ++i) {
            this->PushItem(in_vector_[i]);
        }
    }

private:
    //! Vector pointer to read elements from.
    std::vector<ValueType> in_vector_;
};

/*!
 * EqualToDIA is a Source-DOp, which takes a vector of data EQUAL on all
 * workers, and returns the data in a DIA. Use Distribute to actually distribute
 * data from a single worker, EqualToDIA is a wrapper if the data is already
 * distributed.
 *
 * \param ctx Reference to the Context object
 *
 * \param in_vector Vector to convert to a DIA, the contents is COPIED into the
 * DIANode.
 */
template <typename ValueType>
auto EqualToDIA(Context & ctx,
                const std::vector<ValueType>&in_vector) {

    using EqualToDIANode = api::EqualToDIANode<ValueType>;

    return DIA<ValueType>(std::make_shared<EqualToDIANode>(ctx, in_vector));
}

/*!
 * EqualToDIA is an Source-DOp, which takes a vector of data EQUAL on all
 * workers, and returns the data in a DIA. Use Distribute to actually distribute
 * data from a single worker, EqualToDIA is a wrapper if the data is already
 * distributed.
 *
 * \param ctx Reference to the Context object
 *
 * \param in_vector Vector to convert to a DIA, the contents is MOVED into the
 * DIANode.
 */
template <typename ValueType>
auto EqualToDIA(Context & ctx,
                std::vector<ValueType>&& in_vector) {

    using EqualToDIANode = api::EqualToDIANode<ValueType>;

    return DIA<ValueType>(
        std::make_shared<EqualToDIANode>(ctx, std::move(in_vector)));
}

//! \}

} // namespace api

//! imported from api namespace
using api::EqualToDIA;

} // namespace thrill

#endif // !THRILL_API_EQUAL_TO_DIA_HEADER

/******************************************************************************/
