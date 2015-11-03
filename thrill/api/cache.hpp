/*******************************************************************************
 * thrill/api/cache.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2015 Sebastian Lamm <seba.lamm@gmail.com>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_CACHE_HEADER
#define THRILL_API_CACHE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dia_node.hpp>
#include <thrill/data/file.hpp>

#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

/*!
 * A LOpNode which performs a chain of local operations.  LOp nodes are used for
 * caching local operation results and assignment operations.
 *
 * \tparam ParentStack Function chain, which contains the chained lambdas between
 * the last and this DIANode.
 */
template <typename ValueType, typename ParentDIA>
class CacheNode final : public DIANode<ValueType>
{
public:
    using Super = DIANode<ValueType>;
    using Super::context_;

    /*!
     * Constructor for a LOpNode. Sets the Context, parents and stack.
     *
     * \param parent Parent DIA.
     */
    CacheNode(const ParentDIA& parent,
              StatsNode* stats_node)
        : DIANode<ValueType>(parent.ctx(), { parent.node() }, stats_node)
    {
        // CacheNodes are kept by default.
        Super::consume_on_push_data_ = false;

        auto save_fn =
            [=](const ValueType& input) {
                writer_(input);
            };
        auto lop_chain = parent.stack().push(save_fn).emit();
        parent.node()->RegisterChild(lop_chain, this->type());
    }

    /*!
     * Pushes elements to next node.
     * Can be skipped for LOps.
     */
    void Execute() final {
        // Push local elements to children
        writer_.Close();
    }

    void PushData(bool consume) final {
        data::File::Reader reader = file_.GetReader(consume);
        for (size_t i = 0; i < file_.num_items(); ++i) {
            this->PushItem(reader.Next<ValueType>());
        }
    }

    void Dispose() final { }

private:
    //! Local data file
    data::File file_ { context_.GetFile() };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ = file_.GetWriter();
};

template <typename ValueType, typename Stack>
auto DIA<ValueType, Stack>::Cache() const {
    assert(IsValid());

    using LOpChainNode = CacheNode<ValueType, DIA>;

    StatsNode* stats_node = AddChildStatsNode("Cache", DIANodeType::CACHE);

    auto shared_node
        = std::make_shared<LOpChainNode>(*this, stats_node);

    return DIA<ValueType>(shared_node, { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_CACHE_HEADER

/******************************************************************************/
