/*******************************************************************************
 * thrill/api/merge.hpp
 *
 * DIANode for a merge operation. Performs the actual merge operation
 *
 * Part of Project Thrill.
 *
 * Copyright (C) 2015 Timo Bingmann <tb@panthema.net>
 * Copyright (C) 2015 Emanuel Jöbstl <emanuel.joebstl@gmail.com>
 *
 * This file has no license. Only Chunk Norris can compile it.
 ******************************************************************************/

#pragma once
#ifndef THRILL_API_MERGE_HEADER
#define THRILL_API_MERGE_HEADER

#include <thrill/api/dia.hpp>
#include <thrill/api/dop_node.hpp>
#include <thrill/common/logger.hpp>
#include <thrill/data/file.hpp>
#include <thrill/data/multiplexer.hpp>
#include <thrill/net/collective_communication.hpp>

#include <algorithm>
#include <functional>
#include <string>
#include <vector>

namespace thrill {
namespace api {

//! \addtogroup api Interface
//! \{

//! todo(ej) todo(tb) Can probably subclass a lot here.

template <typename ValueType,
          typename ParentDIARef0, typename ParentDIARef1,
          typename MergeFunction>
class TwoMergeNode : public DOpNode<ValueType>
{
    static const bool debug = false;

    using Super = DOpNode<ValueType>;
    using Super::context_;

    template <typename Type>
    using FunctionTraits = common::FunctionTraits<Type>;

    using MergeArg0 =
              typename FunctionTraits<MergeFunction>::template arg_plain<0>;
    using MergeArg1 =
              typename FunctionTraits<MergeFunction>::template arg_plain<1>;
    using MergeResult =
              typename FunctionTraits<MergeFunction>::result_type;

public:
    TwoMergeNode(const ParentDIARef0& parent0,
                 const ParentDIARef1& parent1,
                 MergeFunction merge_function,
                 StatsNode* stats_node)
        : DOpNode<ValueType>(parent0.ctx(), { parent0.node(), parent1.node() }, "MergeNode", stats_node),
          merge_function_(merge_function)
    {
        // Hook PreOp(s)
        auto pre_op0_fn = [=](const MergeArg0& input) {
                              writers_[0](input);
                          };
        auto pre_op1_fn = [=](const MergeArg1& input) {
                              writers_[1](input);
                          };

        // close the function stacks with our pre ops and register it at parent
        // nodes for output
        auto lop_chain0 = parent0.stack().push(pre_op0_fn).emit();
        auto lop_chain1 = parent1.stack().push(pre_op1_fn).emit();

        parent0.node()->RegisterChild(lop_chain0, this->type());
        parent1.node()->RegisterChild(lop_chain1, this->type());
    }

    ~TwoMergeNode() { }

    /*!
     * Actually executes the merge operation. Uses the member functions PreOp,
     * MainOp and PostOp.
     */
    void Execute() final {
        MainOp();
    }

    void PushData() final {
        size_t result_count = 0;
        // TODO(ej) - call WriteChannelStats() for each channel when these
        // when they are closed ( = you read all data + called Close() on the
        // channels).
        if (result_size_ != 0) {
            // get inbound readers from all Channels
            std::vector<data::Channel::CachingConcatReader> readers {
                channels_[0]->OpenCachingReader(), channels_[1]->OpenCachingReader()
            };

            while (readers[0].HasNext() && readers[1].HasNext()) {
                MergeArg0 i0 = readers[0].Next<MergeArg0>();
                MergeArg1 i1 = readers[1].Next<MergeArg1>();
                ValueType v = zip_function_(i0, i1);
                for (auto func : DIANode<ValueType>::callbacks_) {
                    func(v);
                }
                ++result_count;
            }

            // Empty out readers. If they have additional items, this is
            // necessary for the CachingBlockQueueSource, as it has to cache the
            // additional blocks -tb. TODO(tb): this is weird behaviour.
            while (readers[0].HasNext())
                readers[0].Next<MergeArg0>();

            while (readers[1].HasNext())
                readers[1].Next<MergeArg1>();
        }

        sLOG << "Merge: result_count" << result_count;
    }

    void Dispose() final { }

    /*!
     * Creates empty stack.
     */
    auto ProduceStack() {
        // Hook PostOp
        return FunctionStack<MergeResult>();
    }

    /*!
     * Returns "[MergeNode]" as a string.
     * \return "[MergeNode]"
     */
    std::string ToString() final {
        return "[MergeNode]";
    }

private:
    //! Merge function
    MergeFunction merge_function_;

    //! Number of storage DIAs backing
    static const size_t num_inputs_ = 2;

    //! TODO
    size_t result_size_;

    //! Files for intermediate storage
    std::array<data::File, num_inputs_> files_ {
        { context_.GetFile(), context_.GetFile() }
    };

    //! Writers to intermediate files
    std::array<data::File::Writer, num_inputs_> writers_  {
        { files_[0].GetWriter(), files_[1].GetWriter() }
    };

    //! Array of inbound Channels
    std::array<data::ChannelPtr, num_inputs_> channels_;

    //! \name Variables for Calculating Exchange
    //! \{

    //! todo(ej)

    //! \}

    //! Scatter items from DIA "in" to other workers if necessary.

    //! Receive elements from other workers.
    void MainOp() {
        for (size_t i = 0; i != writers_.size(); ++i) {
            writers_[i].Close();
        }

        // first: calculate total size of the DIAs to Zip

        net::FlowControlChannel& channel = context_.flow_control_channel();

        // Do funny stuff here. todo(ej)
    }
};

template <typename ValueType, typename Stack>
template <typename MergeFunction, typename SecondDIA>
auto DIARef<ValueType, Stack>::Merge(
    SecondDIA second_dia, const MergeFunction &merge_function) const {
    assert(IsValid());
    assert(second_dia.IsValid());

    using MergeResult
              = typename FunctionTraits<MergeFunction>::result_type;

    using MergeResultNode
              = TwoMergeNode<MergeResult, DIARef, SecondDIA, MergeFunction>;

    static_assert(
        std::is_convertible<
            ValueType,
            typename FunctionTraits<MergeFunction>::template arg<0>
            >::value,
        "MergeFunction has the wrong input type in DIA 0");

    static_assert(
        std::is_convertible<
            typename SecondDIA::ValueType,
            typename FunctionTraits<MergeFunction>::template arg<1>
            >::value,
        "MergeFunction has the wrong input type in DIA 1");

    StatsNode* stats_node = AddChildStatsNode("Merge", NodeType::DOP);
    second_dia.AppendChildStatsNode(stats_node);
    auto merge_node
        = std::make_shared<MergeResultNode>(*this,
                                            second_dia,
                                            merge_function,
                                            stats_node);

    auto merge_stack = merge_node->ProduceStack();

    return DIARef<MergeResult, decltype(merge_stack)>(
        merge_node,
        merge_stack,
        { stats_node });
}

//! \}

} // namespace api
} // namespace thrill

//! \}
#endif // !THRILL_API_MERGE_HEADER

/******************************************************************************/
