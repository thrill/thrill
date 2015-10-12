/*******************************************************************************
 * thrill/api/distribute.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_DISTRIBUTE_HEADER
#define THRILL_API_DISTRIBUTE_HEADER

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
class DistributeNode final : public SourceNode<ValueType>
{
public:
    using Super = SourceNode<ValueType>;
    using Super::context_;

    DistributeNode(Context& ctx,
                   const std::vector<ValueType>& in_vector,
                   StatsNode* stats_node)
        : SourceNode<ValueType>(ctx, { }, stats_node),
          in_vector_(in_vector)
    { }

    DistributeNode(Context& ctx,
                   std::vector<ValueType>&& in_vector,
                   StatsNode* stats_node)
        : SourceNode<ValueType>(ctx, { }, stats_node),
          in_vector_(std::move(in_vector))
    { }

    void PushData(bool /* consume */) final {
        size_t local_begin, local_end;
        std::tie(local_begin, local_end) =
            context_.CalculateLocalRange(in_vector_.size());

        for (size_t i = local_begin; i < local_end; ++i) {
            this->PushItem(in_vector_[i]);
        }
    }

    void Dispose() final { }

private:
    //! Vector pointer to read elements from.
    std::vector<ValueType> in_vector_;
};

/*!
 * Distribute is a Source-DOp, which takes a vector of data EQUAL on all
 * workers, and returns the data in a DIA. Use DistributeFrom to actually
 * distribute data from a single worker, Distribute is more a ToDIA wrapper if
 * the data is already distributed.
 *
 * \param ctx Reference to the Context object
 *
 * \param in_vector Vector to convert to a DIA, the contents is COPIED into the
 * DIANode.
 */
template <typename ValueType>
auto Distribute(Context & ctx,
                const std::vector<ValueType>&in_vector) {

    using DistributeNode = api::DistributeNode<ValueType>;

    StatsNode* stats_node = ctx.stats_graph().AddNode(
        "Distribute", DIANodeType::DOP);

    auto shared_node =
        std::make_shared<DistributeNode>(ctx, in_vector, stats_node);

    return DIA<ValueType>(shared_node, { stats_node });
}

/*!
 * Distribute is an Initial-DOp, which takes a vector of data EQUAL on all
 * workers, and returns the data in a DIA. Use DistributeFrom to actually
 * distribute data from a single worker, Distribute is more a ToDIA wrapper if
 * the data is already distributed.
 *
 * \param ctx Reference to the Context object
 *
 * \param in_vector Vector to convert to a DIA, the contents is MOVED into the
 * DIANode.
 */
template <typename ValueType>
auto Distribute(Context & ctx,
                std::vector<ValueType>&& in_vector) {

    using DistributeNode = api::DistributeNode<ValueType>;

    StatsNode* stats_node = ctx.stats_graph().AddNode(
        "Distribute", DIANodeType::DOP);

    auto shared_node =
        std::make_shared<DistributeNode>(ctx, std::move(in_vector), stats_node);

    return DIA<ValueType>(shared_node, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_DISTRIBUTE_HEADER

/******************************************************************************/
