/*******************************************************************************
 * thrill/api/rebalance.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_REBALANCE_HEADER
#define THRILL_API_REBALANCE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType>
class RebalanceNode final : public DOpNode<ValueType>
{
    static constexpr bool debug = false;

public:
    using Super = DOpNode<ValueType>;
    using Super::context_;

    template <typename ParentDIA>
    explicit RebalanceNode(const ParentDIA& parent)
        : Super(parent.ctx(), "Rebalance", { parent.id() }, { parent.node() }),
          parent_stack_empty_(ParentDIA::stack_empty) {

        auto save_fn = [this](const ValueType& input) {
                           writer_.Put(input);
                       };
        auto lop_chain = parent.stack().push(save_fn).fold();
        parent.node()->AddChild(this, lop_chain);
    }

    bool OnPreOpFile(const data::File& file, size_t /* parent_index */) final {
        if (!parent_stack_empty_) {
            LOGC(common::g_debug_push_file)
                << "Rebalance rejected File from parent "
                << "due to non-empty function stack.";
            return false;
        }
        assert(file_.num_items() == 0);
        file_ = file.Copy();
        return true;
    }

    void StopPreOp(size_t /* id */) final {
        // Push local elements to children
        writer_.Close();
    }

    //! Executes the rebalance operation.
    void Execute() final {
        LOG << "RebalanceNode::Execute() processing";

        size_t local_size;
        local_size = file_.num_items();
        sLOG << "local_size" << local_size;

        size_t local_rank = local_size;
        size_t global_size = context_.net.ExPrefixSumTotal(local_rank);
        sLOG << "local_rank" << local_rank;
        sLOG << "global_size" << global_size;

        const size_t num_workers = context_.num_workers();
        const double pre_pe =
            static_cast<double>(global_size) / static_cast<double>(num_workers);

        // calculate offset vector
        std::vector<size_t> offsets(num_workers + 1, 0);
        for (size_t p = 0; p < num_workers; ++p) {
            size_t limit = static_cast<size_t>(static_cast<double>(p) * pre_pe);
            if (limit < local_rank) continue;

            offsets[p] = std::min(limit - local_rank, file_.num_items());
        }
        offsets[num_workers] = file_.num_items();
        LOG << "offsets = " << common::VecToStr(offsets);

        stream_->template Scatter<ValueType>(
            file_, offsets, /* consume */ true);
    }

    void PushData(bool consume) final {
        auto reader = stream_->GetCatReader(consume);
        while (reader.HasNext()) {
            this->PushItem(reader.template Next<ValueType>());
        }
    }

    void Dispose() final {
        file_.Clear();
    }

private:
    //! Local data file
    data::File file_ { context_.GetFile(this) };
    //! Data writer to local file (only active in PreOp).
    data::File::Writer writer_ { file_.GetWriter() };
    //! Whether the parent stack is empty
    const bool parent_stack_empty_;

    //! CatStream for exchange
    data::CatStreamPtr stream_ { context_.GetNewCatStream(this) };
};

template <typename ValueType, typename Stack>
auto DIA<ValueType, Stack>::Rebalance() const {
    assert(IsValid());
    using RebalanceNode = api::RebalanceNode<ValueType>;
    return DIA<ValueType>(tlx::make_counting<RebalanceNode>(*this));
}

} // namespace api
} // namespace thrill

#endif // !THRILL_API_REBALANCE_HEADER

/******************************************************************************/
