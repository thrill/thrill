/*******************************************************************************
 * thrill/api/concat.hpp
 *
 * Part of Project Thrill - http://project-thrill.org
 *
 * Copyright (C) 2016 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the BSD-2 license in the LICENSE file.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_CONCAT_HEADER
#define THRILL_API_CONCAT_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/functional.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/common/string.hpp>
#include <thrill/data/file.hpp>

#include <algorithm>
#include <initializer_list>
#include <vector>

namespace thrill {
namespace api {

/*!
 * \ingroup api_layer
 */
template <typename ValueType, typename ParentDIA0, typename ... ParentDIAs>
class ConcatNode final : public DOpNode<ValueType>
{
public:
    static constexpr bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    //! Constructor for variant with variadic parent parameter pack, which each
    //! parent may have a different FunctionStack.
    explicit ConcatNode(const ParentDIA0& parent0,
                        const ParentDIAs& ... parents)
        : Super(parent0.ctx(), "Concat",
                { parent0.id(), parents.id() ... },
                { parent0.node(), parents.node() ... }),
          num_inputs_(1 + sizeof ... (ParentDIAs))
    {
        PrintWarning();

        files_.reserve(num_inputs_);
        writers_.reserve(num_inputs_);

        // allocate files.
        for (size_t i = 0; i < num_inputs_; ++i)
            files_.emplace_back(context_.GetFile(this));

        for (size_t i = 0; i < num_inputs_; ++i)
            writers_.emplace_back(files_[i].GetWriter());

        common::VariadicCallForeachIndex(
            RegisterParent(this), parent0, parents ...);
    }

    //! Constructor for variant with a std::vector of parents all with the same
    //! (usually empty) FunctionStack.
    explicit ConcatNode(const std::vector<ParentDIA0>& parents)
        : Super(parents.front().ctx(), "Concat",
                common::MapVector(
                    parents, [](const ParentDIA0& d) { return d.id(); }),
                common::MapVector(
                    parents, [](const ParentDIA0& d) {
                        return DIABasePtr(d.node().get());
                    })),
          num_inputs_(parents.size())
    {
        PrintWarning();

        files_.reserve(num_inputs_);
        writers_.reserve(num_inputs_);

        // allocate files.
        for (size_t i = 0; i < num_inputs_; ++i)
            files_.emplace_back(context_.GetFile(this));

        for (size_t i = 0; i < num_inputs_; ++i)
            writers_.emplace_back(files_[i].GetWriter());

        for (size_t i = 0; i < num_inputs_; ++i)
        {
            // construct lambda with only the writer in the closure
            data::File::Writer* writer = &writers_[i];
            auto pre_op_fn = [writer](const ValueType& input) -> void {
                                 writer->Put(input);
                             };

            // close the function stacks with our pre ops and register it at
            // parent nodes for output
            auto lop_chain = parents[i].stack().push(pre_op_fn).fold();
            parents[i].node()->AddChild(this, lop_chain, i);
        }
    }

    void PrintWarning() {
        static bool warned_once = false;
        if (warned_once) return;
        warned_once = true;

        LOG1 << "Warning: Concat() is a _very_ expensive data shuffle operation"
             << " which can usually be avoided.";
    }

    //! Register Parent PreOp Hooks, instantiated and called for each Concat
    //! parent
    class RegisterParent
    {
    public:
        explicit RegisterParent(ConcatNode* concat_node)
            : concat_node_(concat_node) { }

        template <typename Index, typename Parent>
        void operator () (const Index&, Parent& parent) {

            // construct lambda with only the writer in the closure
            data::File::Writer* writer = &concat_node_->writers_[Index::index];
            auto pre_op_fn = [writer](const ValueType& input) -> void {
                                 writer->Put(input);
                             };

            // close the function stacks with our pre ops and register it at
            // parent nodes for output
            auto lop_chain = parent.stack().push(pre_op_fn).fold();

            parent.node()->AddChild(concat_node_, lop_chain, Index::index);
        }

    private:
        ConcatNode* concat_node_;
    };

    //! Receive a whole data::File of ValueType, but only if our stack is empty.
    bool OnPreOpFile(const data::File& file, size_t parent_index) final {
        assert(parent_index < num_inputs_);

        // construct indication whether the parent stack is empty
        static constexpr bool parent_stack_empty[1 + sizeof ... (ParentDIAs)] {
            // parenthesis are due to a MSVC2015 parser bug
            ParentDIA0::stack_empty, (ParentDIAs::stack_empty)...
        };
        if (num_inputs_ == 1 + sizeof ... (ParentDIAs)) {
            // ConcatNode was constructed from different parents
            if (!parent_stack_empty[parent_index]) return false;
        }
        else {
            // ConcatNode was constructor with a vector of equal parents
            assert(sizeof ... (ParentDIAs) == 0);
            if (!parent_stack_empty[0]) return false;
        }

        // accept file
        assert(files_[parent_index].num_items() == 0);
        files_[parent_index] = file.Copy();
        return true;
    }

    void StopPreOp(size_t id) final {
        writers_[id].Close();
    }

    //! Executes the concat operation.
    void Execute() final {
        LOG << "ConcatNode::Execute() processing";

        using VectorSizeT = std::vector<size_t>;

        VectorSizeT local_sizes(num_inputs_);
        for (size_t i = 0; i < num_inputs_; ++i) {
            local_sizes[i] = files_[i].num_items();
        }
        sLOG << "local_sizes" << common::VecToStr(local_sizes);

        VectorSizeT global_sizes = context_.net.AllReduce(
            local_sizes, common::ComponentSum<VectorSizeT>());

        sLOG << "global_sizes" << common::VecToStr(global_sizes);

        // exclusive prefixsum of whole dia sizes
        size_t total_items = 0;
        for (size_t i = 0; i < num_inputs_; ++i) {

            size_t next_total_items = total_items + global_sizes[i];

            // on rank 0: add sum to local_sizes
            if (context_.my_rank() == 0)
                local_sizes[i] = total_items;

            total_items = next_total_items;
        }

        sLOG << "local_sizes" << common::VecToStr(local_sizes);
        sLOG << "total_items" << total_items;

        VectorSizeT local_ranks = context_.net.PrefixSum(
            local_sizes, VectorSizeT(num_inputs_),
            common::ComponentSum<VectorSizeT>());

        sLOG << "local_ranks" << common::VecToStr(local_ranks);

        streams_.reserve(num_inputs_);
        for (size_t i = 0; i < num_inputs_; ++i)
            streams_.emplace_back(context_.GetNewCatStream(this));

        /*
         * Data Exchange in Concat (Example):
         *
         * /  worker0  \ /  worker1  \ /  worker2  \ /  worker3  \
         * |--256--|     |--256--|     |--256--|     |--256--|      DIA0
         * |----512----| |----512----| |----512----| |----512----|  DIA1
         *
         * In the steps above, we calculate the global rank of each DIA piece.
         *
         * Result:
         *                (global per PE split points)
         *               v             v             v            v
         *               |             |             |            |
         * |256||256||256||256||--512--||--512--||--512--||--512--|
         * |------------------||----------------------------------|
         *       (stream0)                 (stream1)
         *
         * With the global ranks of each DIA piece, one can calculate where it
         * should go. We have to use k CatStreams for the data exchange, since
         * the last PE must be able to transmit its piece to the first PE before
         * all those in DIA1. The offset calculation below determines the global
         * per_pe split point relative to the current DIA piece's global rank:
         * if the pre_pe split is below the global rank, nothing needs to be
         * send. Otherwise, one can send only that PE part to the PE. -tb
         */
        const size_t num_workers = context_.num_workers();
        const double pre_pe =
            static_cast<double>(total_items) / static_cast<double>(num_workers);

        for (size_t in = 0; in < num_inputs_; ++in) {

            // calculate offset vector
            std::vector<size_t> offsets(num_workers + 1, 0);
            for (size_t p = 0; p < num_workers; ++p) {
                size_t limit =
                    static_cast<size_t>(static_cast<double>(p) * pre_pe);
                if (limit < local_ranks[in]) continue;

                offsets[p] = std::min(limit - local_ranks[in],
                                      files_[in].num_items());
            }
            offsets[num_workers] = files_[in].num_items();

            LOG << "offsets[" << in << "] = " << common::VecToStr(offsets);

            streams_[in]->template Scatter<ValueType>(
                files_[in], offsets, /* consume */ true);
        }
    }

    void PushData(bool consume) final {

        size_t total = 0;
        // concatenate all CatStreams
        for (size_t in = 0; in < num_inputs_; ++in) {
            data::CatStream::CatReader reader =
                streams_[in]->GetCatReader(consume);

            while (reader.HasNext()) {
                this->PushItem(reader.Next<ValueType>());
                ++total;
            }
        }
        LOG << "total = " << total;
    }

    void Dispose() final { }

private:
    size_t num_inputs_;

    //! Files for intermediate storage
    std::vector<data::File> files_;
    //! Writers to intermediate files
    std::vector<data::File::Writer> writers_;

    //! Array of CatStreams for exchange
    std::vector<data::CatStreamPtr> streams_;
};

/*!
 * Concat is a DOp, which concatenates any number of DIAs to a single DIA.  All
 * input DIAs must contain the same type, which is also the output DIA's type.
 *
 * The concat operation balances all input data, so that each worker will have
 * an equal number of elements when the concat completes.
 *
 * \param first_dia first DIA
 * \param dias DIAs, which are concatd with the first DIA.
 *
 * \ingroup dia_dops
 */
template <typename FirstDIA, typename ... DIAs>
auto Concat(const FirstDIA &first_dia, const DIAs &... dias) {

    using VarForeachExpander = int[];

    first_dia.AssertValid();
    (void)VarForeachExpander {
        (dias.AssertValid(), 0) ...
    };

    using ValueType = typename FirstDIA::ValueType;

    using ConcatNode = api::ConcatNode<ValueType, FirstDIA, DIAs ...>;

    return DIA<ValueType>(common::MakeCounting<ConcatNode>(first_dia, dias ...));
}

/*!
 * Concat is a DOp, which concatenates any number of DIAs to a single DIA.  All
 * input DIAs must contain the same type, which is also the output DIA's type.
 *
 * The concat operation balances all input data, so that each worker will have
 * an equal number of elements when the concat completes.
 *
 * \param dias DIAs, which is concatenated.
 *
 * \ingroup dia_dops
 */
template <typename ValueType>
auto Concat(const std::initializer_list<DIA<ValueType> >&dias) {

    for (const DIA<ValueType>& d : dias)
        d.AssertValid();

    using ConcatNode = api::ConcatNode<ValueType, DIA<ValueType> >;

    return DIA<ValueType>(common::MakeCounting<ConcatNode>(dias));
}

/*!
 * Concat is a DOp, which concatenates any number of DIAs to a single DIA.  All
 * input DIAs must contain the same type, which is also the output DIA's type.
 *
 * The concat operation balances all input data, so that each worker will have
 * an equal number of elements when the concat completes.
 *
 * \param dias DIAs, which is concatenated.
 *
 * \ingroup dia_dops
 */
template <typename ValueType>
auto Concat(const std::vector<DIA<ValueType> >&dias) {

    for (const DIA<ValueType>& d : dias)
        d.AssertValid();

    using ConcatNode = api::ConcatNode<ValueType, DIA<ValueType> >;

    return DIA<ValueType>(common::MakeCounting<ConcatNode>(dias));
}

template <typename ValueType, typename Stack>
template <typename SecondDIA>
auto DIA<ValueType, Stack>::Concat(
    const SecondDIA &second_dia) const {
    return api::Concat(*this, second_dia);
}

} // namespace api

//! imported from api namespace
using api::Concat;

} // namespace thrill

#endif // !THRILL_API_CONCAT_HEADER

/******************************************************************************/
